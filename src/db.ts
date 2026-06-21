/**
 * Central SQL layer (Bun.sql / Postgres) — the durable source of truth.
 *
 * Postgres holds authoritative per-query totals (`query_counts`). The Redis
 * shards are a *derived* per-prefix top-K cache rebuilt from this table by the
 * seeder (cold start) and the cache-updater (live). App nodes write counts here
 * and mark affected prefixes dirty; they never write the suggestion cache.
 *
 * Verified Bun.sql behaviors this module relies on (probed against PG 16):
 *   - the `${sql(rows, ...cols)}` bulk-insert helper composes with
 *     `ON CONFLICT ... DO UPDATE`;
 *   - a row touched twice in one INSERT aborts it ("cannot affect row a second
 *     time") → callers MUST dedup (Map for counts, Set for prefixes);
 *   - `BIGINT` columns come back as JS **strings** (handy: ZADD wants a string);
 *   - DDL with multiple statements must go through `.simple()` (no params).
 */

import { SQL } from "bun";
import { MAX_PREFIX_LEN } from "./config";

export const DATABASE_URL =
  process.env.DATABASE_URL ?? "postgres://typeahead:typeahead@localhost:5432/typeahead";

/** Pooled client (one per process). */
export const db = new SQL(DATABASE_URL, { max: Number(process.env.PG_POOL_MAX ?? 10) });

/** A row of the top-K cache. `count` is a string because BIGINT → string. */
export interface CountRow {
  query: string;
  count: string;
}

/**
 * Create the schema if absent. The seeder is the single DDL writer (so app
 * nodes / the updater never race on CREATE INDEX), but it is idempotent.
 *
 * `query_counts_query_pattern_idx` uses `text_pattern_ops` so that
 * `query LIKE 'prefix%'` is a range scan regardless of the DB collation.
 */
export async function bootstrapSchema(client: SQL = db): Promise<void> {
  await client`
    CREATE TABLE IF NOT EXISTS query_counts (
      query TEXT PRIMARY KEY,
      count BIGINT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS query_counts_query_pattern_idx
      ON query_counts (query text_pattern_ops);
    CREATE TABLE IF NOT EXISTS dirty_prefixes (
      prefix   TEXT PRIMARY KEY,
      dirty_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `.simple();
}

/**
 * Escape LIKE metacharacters so a user prefix is matched literally:
 * `_` and `%` are wildcards, `\` is the escape char. Pair with `ESCAPE '\'`.
 */
export function escapeLike(prefix: string): string {
  return prefix.replace(/[\\%_]/g, (c) => "\\" + c);
}

/**
 * Write path (app nodes), in ONE transaction so a dirty mark is never visible
 * to the updater without its count already committed:
 *   1. accumulate per-query deltas (UPSERT add),
 *   2. bump dirty_at=now() for every affected prefix.
 * `counts` and `prefixes` MUST already be deduped by the caller.
 */
export async function recordCountsAndDirty(
  counts: Map<string, number>,
  prefixes: Set<string>,
): Promise<void> {
  if (counts.size === 0) return;
  const countRows = [...counts].map(([query, count]) => ({ query, count }));
  const prefixRows = [...prefixes].map((prefix) => ({ prefix }));

  await db.begin(async (tx) => {
    await tx`
      INSERT INTO query_counts ${tx(countRows, "query", "count")}
      ON CONFLICT (query) DO UPDATE
        SET count = query_counts.count + EXCLUDED.count
    `;
    if (prefixRows.length > 0) {
      await tx`
        INSERT INTO dirty_prefixes ${tx(prefixRows, "prefix")}
        ON CONFLICT (prefix) DO UPDATE SET dirty_at = now()
      `;
    }
  });
}

/**
 * Re-mark prefixes dirty (no count change). Used by the cache-updater to requeue
 * a batch whose shard write failed, so it is retried on a later cycle.
 */
export async function markDirty(prefixes: string[], client: SQL = db): Promise<void> {
  if (prefixes.length === 0) return;
  const rows = prefixes.map((prefix) => ({ prefix }));
  await client`
    INSERT INTO dirty_prefixes ${client(rows, "prefix")}
    ON CONFLICT (prefix) DO UPDATE SET dirty_at = now()
  `;
}

/**
 * The top-K queries for a single prefix, by count desc (deterministic tiebreak
 * `query ASC` — MUST match the seeder's window function so re-derivation is
 * stable). Uses the constant-prefix LIKE range scan.
 */
export async function topKForPrefix(
  prefix: string,
  k: number,
  client: SQL = db,
): Promise<CountRow[]> {
  return (await client`
    SELECT query, count FROM query_counts
    WHERE query LIKE ${escapeLike(prefix) + "%"} ESCAPE ${"\\"}
    ORDER BY count DESC, query ASC
    LIMIT ${k}
  `) as CountRow[];
}

/** A derived cache row: the top-K for `prefix`. */
export interface DerivedRow extends CountRow {
  prefix: string;
}

/**
 * Derive the top-K for EVERY distinct prefix from `query_counts`, used for the
 * cold-start cache build. Chunked by prefix length (1..MAX_PREFIX_LEN): each
 * pass ranks all prefixes of one length with a single window-function scan, so
 * peak memory is one length-class instead of all ~1.6M rows at once. Yields
 * batches; the caller pipelines them to the owning shards.
 */
export async function* deriveAllTopK(
  k: number,
  client: SQL = db,
): AsyncGenerator<DerivedRow[]> {
  for (let len = 1; len <= MAX_PREFIX_LEN; len++) {
    const batch = (await client`
      WITH expanded AS (
        SELECT query, count, left(query, ${len}) AS prefix
        FROM query_counts
        WHERE length(query) >= ${len}
      ), ranked AS (
        SELECT prefix, query, count,
               row_number() OVER (PARTITION BY prefix ORDER BY count DESC, query ASC) AS rn
        FROM expanded
      )
      SELECT prefix, query, count FROM ranked WHERE rn <= ${k}
    `) as DerivedRow[];
    if (batch.length > 0) yield batch;
  }
}

/** Bulk-insert raw dataset counts (seeder). Caller chunks + dedups. */
export async function bulkLoadCounts(
  rows: { query: string; count: number }[],
  client: SQL = db,
): Promise<void> {
  if (rows.length === 0) return;
  await client`
    INSERT INTO query_counts ${client(rows, "query", "count")}
    ON CONFLICT (query) DO UPDATE
      SET count = query_counts.count + EXCLUDED.count
  `;
}
