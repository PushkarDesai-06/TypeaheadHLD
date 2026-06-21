/**
 * Application Node (Phase 2 & 3)
 *
 * A stateless Bun HTTP server paired 1:1 with a single local Redis shard. It is
 * deliberately "dumb" about routing — the Load Balancer (src/lb.ts) decides
 * which node a request reaches via the shared consistent-hash ring, so by the
 * time a request lands here it already belongs to this shard.
 *
 * Endpoints:
 *   GET  /suggest?q=<prefix>  read top-N suggestions from the local shard.
 *   POST /search  {query}     buffer the query in memory, return 202 immediately.
 *   GET  /trending            top-N trending queries on THIS shard (LB merges).
 *   GET  /health              shard id + buffer depth (used by compose + LB).
 *   POST /internal/cache      apply refreshed top-K caches to the local shard
 *                             (called by the cache-updater; not proxied by LB).
 *
 * Write path: searches are buffered and flushed in batches — primarily the
 * instant the buffer hits BATCH_SIZE, with a timed safety-net flush for low
 * traffic. A flush UPSERTs counts into central Postgres (the source of truth)
 * and marks each affected prefix dirty.
 *
 * The cache-updater (src/cache-updater.ts) recomputes each dirty prefix's top-K
 * from Postgres and POSTs it to /internal/cache on the shard that route(prefix)
 * owns — so THIS node is the only writer of its own Redis (textbook Step 8),
 * yet a live /search becomes globally visible in /suggest across shards.
 * Trending stays per-shard in Redis (decay is a ZSET feature) and is merged by
 * the LB.
 */

import { RedisClient } from "bun";
import { route } from "./hash-ring";
import {
  db,
  recordCountsAndDirty,
  topKForPrefix,
  topKForPrefixRecency,
  markDirty,
} from "./db";
import {
  BATCH_SIZE,
  FLUSH_INTERVAL_MS,
  SUGGEST_LIMIT,
  TRENDING_LIMIT,
  DECAY_FACTOR,
  DECAY_INTERVAL_MS,
  TRENDING_KEY,
  normalize,
  prefixesOf,
  suggestKey,
  recencyKey,
  rankModeOf,
  redisUrlFor,
  type ShardId,
} from "./config";

const SHARD_ID = (process.env.SHARD_ID ?? "1") as ShardId;
const PORT = Number(process.env.PORT ?? 3000 + Number(SHARD_ID));
const REDIS_URL = process.env.REDIS_URL ?? redisUrlFor(SHARD_ID);

const redis = new RedisClient(REDIS_URL);

// ---------------------------------------------------------------------------
// In-memory write buffer (Step 3) + batch writer (Step 4)
// ---------------------------------------------------------------------------

/** Pending search queries awaiting a batch flush. */
const buffer: string[] = [];
/** The in-progress drain, if any. Ensures only one drain runs at a time. */
let draining: Promise<void> | null = null;

/**
 * In-memory observability counters, exposed at `GET /metrics` and aggregated by
 * the LB. They make the assignment's Non-Functional asks measurable: the cache
 * hit rate (`cacheHits` vs `cacheMisses`) and the write-reduction from batching
 * (`searchesReceived` per `batchesFlushed`). All approximate, reset on restart.
 */
const metrics = {
  searchesReceived: 0, // POST /search calls accepted into the buffer
  batchesFlushed: 0, // batch transactions written to Postgres
  rowsUpserted: 0, // unique (query,count) rows upserted across all batches
  cacheHits: 0, // /suggest served from the Redis shard
  cacheMisses: 0, // /suggest that fell back to Postgres (§6)
};

/**
 * Write one BATCH_SIZE chunk:
 *   1. Aggregate + dedup the chunk's counts (dedup is required: Postgres
 *      ON CONFLICT cannot touch the same row twice in one statement).
 *   2. UPSERT counts into central Postgres (source of truth) AND mark every
 *      affected prefix dirty — the cache-updater turns those into refreshed
 *      `q:<prefix>` caches on the owning shards. The app no longer writes the
 *      suggestion cache itself (that was the cross-shard-visibility bug).
 *   3. Still ZINCRBY each completed query into THIS shard's trending ZSET
 *      (trending stays in Redis, strict-networking-safe).
 * On failure the chunk is returned to the front of the buffer so it is retried
 * rather than silently dropped (the caller stops the drain to avoid a hot loop).
 */
async function writeChunk(batch: string[]): Promise<void> {
  // Aggregate + deduplicate counts (Step 4.3).
  const counts = new Map<string, number>();
  for (const q of batch) counts.set(q, (counts.get(q) ?? 0) + 1);

  // Dedup every affected prefix for the dirty-mark upsert.
  const prefixes = new Set<string>();
  for (const query of counts.keys()) {
    for (const prefix of prefixesOf(query)) prefixes.add(prefix);
  }

  // Order matters for retry safety: a failed chunk is re-queued and replayed
  // whole, and neither write is idempotent. Do the APPROXIMATE trending write
  // first and the EXACT Postgres source-of-truth last — so if trending succeeds
  // but Postgres throws, the retry re-applies only the (tolerable) trending
  // double-count, never a double-count of the authoritative counts.

  // 1) Trending -> this shard's local Redis (one auto-pipelined round-trip).
  const pipeline: Promise<unknown>[] = [];
  for (const [query, count] of counts) {
    pipeline.push(redis.send("ZINCRBY", [TRENDING_KEY, String(count), query]));
  }
  await Promise.all(pipeline);

  // 2) Durable counts + dirty marks -> Postgres (source of truth), one tx.
  await recordCountsAndDirty(counts, prefixes);

  metrics.batchesFlushed++;
  metrics.rowsUpserted += counts.size;
  console.log(
    `[app${SHARD_ID}] flushed ${batch.length} searches ` +
      `(${counts.size} unique, ${prefixes.size} prefixes dirtied) ` +
      `[batch #${metrics.batchesFlushed}]`,
  );
}

/** Drain the whole buffer in BATCH_SIZE chunks, newest-arriving items included. */
async function drain(): Promise<void> {
  while (buffer.length > 0) {
    const batch = buffer.splice(0, BATCH_SIZE);
    try {
      await writeChunk(batch);
    } catch (err) {
      console.error(`[app${SHARD_ID}] batch flush failed, re-queuing:`, err);
      buffer.unshift(...batch); // retry on the next trigger; don't hot-loop
      return;
    }
  }
}

/** Start a drain if one isn't already running; returns the active drain. */
function scheduleFlush(): Promise<void> {
  if (!draining) draining = drain().finally(() => (draining = null));
  return draining;
}

// Safety-net flush so a partial buffer is never stranded on low traffic.
setInterval(() => {
  if (buffer.length > 0) void scheduleFlush();
}, FLUSH_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Time decay (Phase 3, Step 6)
// ---------------------------------------------------------------------------

/**
 * Atomically multiply every trending score by DECAY_FACTOR (default 0.9 → a
 * 10% daily decay) so historically huge queries slowly cede the top spots to
 * recently active ones. A Lua script keeps the read-modify-write atomic.
 */
const DECAY_LUA = `
local key = KEYS[1]
local factor = tonumber(ARGV[1])
local items = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')
for i = 1, #items, 2 do
  redis.call('ZADD', key, tonumber(items[i + 1]) * factor, items[i])
end
return #items / 2
`;

async function runDecay(): Promise<void> {
  try {
    const n = await redis.send("EVAL", [
      DECAY_LUA,
      "1",
      TRENDING_KEY,
      String(DECAY_FACTOR),
    ]);
    console.log(`[app${SHARD_ID}] applied ${DECAY_FACTOR}x decay to ${n} trending entries`);
  } catch (err) {
    console.error(`[app${SHARD_ID}] decay failed:`, err);
  }
}

setInterval(() => void runDecay(), DECAY_INTERVAL_MS);

// ---------------------------------------------------------------------------
// Derived suggestion cache (written ONLY via POST /internal/cache)
// ---------------------------------------------------------------------------

/**
 * Atomically replace a prefix's cache: DEL then ZADD the top-K in one Lua call,
 * so /suggest never observes an empty key mid-rebuild.
 * ARGV = [score1, member1, score2, member2, ...].
 */
const REPLACE_LUA = `
redis.call('DEL', KEYS[1])
for i = 1, #ARGV, 2 do
  redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
end
return 1
`;

interface ScoredEntry {
  query: string;
  count: string | number;
}
interface CacheUpdate {
  prefix: string;
  /** Top-K ordered by all-time count -> the `q:<prefix>` ZSET. */
  topK: ScoredEntry[];
  /** Top-K ordered by blended recency score -> the `qr:<prefix>` ZSET (§7). */
  topKRecency?: ScoredEntry[];
}

/** Flatten a scored top-K into the `[score, member, ...]` ARGV REPLACE_LUA wants. */
function toArgv(entries: ScoredEntry[]): string[] {
  const argv: string[] = [];
  for (const { query, count } of entries) argv.push(String(count), query);
  return argv;
}

/**
 * Apply a batch of cache replacements to THIS shard's local Redis. Each prefix
 * carries BOTH rankings: the all-time `q:<prefix>` cache and the recency
 * `qr:<prefix>` cache, replaced atomically. Prefixes this shard does not own
 * (route(prefix) !== SHARD_ID) are rejected as a guard, so a misrouted update
 * can never plant a key on the wrong shard.
 */
async function applyCacheUpdates(
  updates: CacheUpdate[],
): Promise<{ applied: number; rejected: number }> {
  const pipeline: Promise<unknown>[] = [];
  let applied = 0;
  let rejected = 0;
  for (const { prefix, topK, topKRecency } of updates) {
    if (route(prefix) !== SHARD_ID) {
      rejected++;
      continue;
    }
    pipeline.push(
      redis.send("EVAL", [REPLACE_LUA, "1", suggestKey(prefix), ...toArgv(topK)]),
    );
    if (topKRecency) {
      pipeline.push(
        redis.send("EVAL", [REPLACE_LUA, "1", recencyKey(prefix), ...toArgv(topKRecency)]),
      );
    }
    applied++;
  }
  await Promise.all(pipeline);
  return { applied, rejected };
}

// ---------------------------------------------------------------------------
// HTTP server
// ---------------------------------------------------------------------------

const json = (data: unknown, status = 200) =>
  Response.json(data, { status, headers: { "access-control-allow-origin": "*" } });

/** Parse a `?limit=` param to a positive integer, falling back to `def`. */
function parseLimit(raw: string | null, def: number): number {
  const n = Number(raw);
  return Number.isInteger(n) && n > 0 ? n : def;
}

/**
 * Parse a `... WITHSCORES` reply into {query, score} objects. Bun speaks RESP3,
 * so it returns an array of [member, score] tuples; we also tolerate the flat
 * RESP2 layout ([member, score, member, score, ...]) for safety.
 */
function parseScored(raw: unknown): { query: string; score: number }[] {
  if (!Array.isArray(raw)) return [];
  if (raw.length > 0 && Array.isArray(raw[0])) {
    return (raw as [string, number][]).map(([query, score]) => ({
      query,
      score: Number(score),
    }));
  }
  const out: { query: string; score: number }[] = [];
  for (let i = 0; i < raw.length; i += 2) {
    out.push({ query: String(raw[i]), score: Number(raw[i + 1]) });
  }
  return out;
}

const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);

    // GET /suggest?q=<prefix>[&rank=basic|recency] — read top-N from the local
    // shard, falling back to Postgres on a cache miss (§6). `rank=recency`
    // reads the blended-score cache (§7); default reads all-time count.
    if (req.method === "GET" && url.pathname === "/suggest") {
      const prefix = normalize(url.searchParams.get("q") ?? "");
      if (!prefix) return json({ prefix: "", suggestions: [], source: "cache" });

      const limit = parseLimit(url.searchParams.get("limit"), SUGGEST_LIMIT);
      const rank = rankModeOf(url.searchParams.get("rank"));
      const key = rank === "recency" ? recencyKey(prefix) : suggestKey(prefix);

      let suggestions = (await redis.send("ZREVRANGE", [
        key,
        "0",
        String(limit - 1),
      ])) as string[];
      let source = "cache";

      // Cache miss -> fall back to the primary data store, then warm the cache
      // by marking the prefix dirty (fire-and-forget; the updater rebuilds it).
      if (suggestions.length === 0) {
        metrics.cacheMisses++;
        const rows =
          rank === "recency"
            ? await topKForPrefixRecency(prefix, limit)
            : await topKForPrefix(prefix, limit);
        suggestions = rows.map((r) => r.query);
        source = "db";
        if (suggestions.length > 0) markDirty([prefix]).catch(() => {});
      } else {
        metrics.cacheHits++;
      }

      return json({ shard: SHARD_ID, prefix, rank, source, suggestions });
    }

    // POST /search {query} — buffer + 202 Accepted (no synchronous write).
    if (req.method === "POST" && url.pathname === "/search") {
      let query = "";
      try {
        query = normalize(((await req.json()) as { query?: string }).query ?? "");
      } catch {
        return json({ error: "invalid json body" }, 400);
      }
      if (!query) return json({ error: "missing query" }, 400);

      metrics.searchesReceived++;
      const buffered = buffer.push(query); // push returns the new length
      if (buffered >= BATCH_SIZE) void scheduleFlush(); // primary trigger (Step 4)

      return json({ message: "Searched", query, buffered }, 202);
    }

    // GET /trending — local shard trending (the LB fans out + merges).
    if (req.method === "GET" && url.pathname === "/trending") {
      const limit = parseLimit(url.searchParams.get("limit"), TRENDING_LIMIT);
      const raw = await redis.send("ZREVRANGE", [
        TRENDING_KEY,
        "0",
        String(limit - 1),
        "WITHSCORES",
      ]);
      return json({ shard: SHARD_ID, trending: parseScored(raw) });
    }

    // GET /cache/debug?prefix=<p> — which node owns this prefix's cache, and
    // whether it is a cache HIT (entry present on this shard) or MISS.
    if (req.method === "GET" && url.pathname === "/cache/debug") {
      const prefix = normalize(url.searchParams.get("prefix") ?? "");
      if (!prefix) return json({ error: "missing prefix" }, 400);

      const [cachedRaw, cachedRecencyRaw] = await Promise.all([
        redis.send("ZCARD", [suggestKey(prefix)]),
        redis.send("ZCARD", [recencyKey(prefix)]),
      ]);
      const cached = Number(cachedRaw);
      const cachedRecency = Number(cachedRecencyRaw);
      return json({
        prefix,
        node: `app${SHARD_ID}`,
        shard: SHARD_ID,
        status: cached > 0 ? "hit" : "miss",
        cached, // entries in the all-time `q:<prefix>` cache
        recencyCached: cachedRecency, // entries in the recency `qr:<prefix>` cache
      });
    }

    // GET /metrics — per-node observability counters (LB aggregates these).
    if (req.method === "GET" && url.pathname === "/metrics") {
      const suggestTotal = metrics.cacheHits + metrics.cacheMisses;
      return json({
        shard: SHARD_ID,
        buffered: buffer.length,
        searchesReceived: metrics.searchesReceived,
        batchesFlushed: metrics.batchesFlushed,
        rowsUpserted: metrics.rowsUpserted,
        cacheHits: metrics.cacheHits,
        cacheMisses: metrics.cacheMisses,
        cacheHitRate: suggestTotal > 0 ? metrics.cacheHits / suggestTotal : null,
        // Searches absorbed per database transaction — the batching win (§8).
        writeReduction:
          metrics.batchesFlushed > 0
            ? metrics.searchesReceived / metrics.batchesFlushed
            : null,
      });
    }

    // POST /internal/cache — the cache-updater pushes refreshed top-K caches
    // here; this node writes them to ITS OWN Redis (so each shard has exactly
    // one writer). Internal only: the LB never proxies this path.
    if (req.method === "POST" && url.pathname === "/internal/cache") {
      let updates: CacheUpdate[];
      try {
        updates = ((await req.json()) as { updates?: CacheUpdate[] }).updates ?? [];
      } catch {
        return json({ error: "invalid json body" }, 400);
      }
      const result = await applyCacheUpdates(updates);
      return json({ shard: SHARD_ID, ...result });
    }

    // GET /health — pings Redis (read path) + Postgres (write path). 503 only
    // if Redis is down, since /suggest still serves from cache without Postgres;
    // Postgres status is reported for visibility.
    if (req.method === "GET" && url.pathname === "/health") {
      let redisUp = true;
      let pgUp = true;
      try {
        await redis.send("PING", []);
      } catch {
        redisUp = false;
      }
      try {
        await db`SELECT 1`;
      } catch {
        pgUp = false;
      }
      return json(
        {
          shard: SHARD_ID,
          buffered: buffer.length,
          redis: redisUp ? "up" : "down",
          postgres: pgUp ? "up" : "down",
        },
        redisUp ? 200 : 503,
      );
    }

    return json({ error: "not found" }, 404);
  },
});

// Flush whatever is buffered before exiting so no searches are lost.
const shutdown = async () => {
  console.log(`[app${SHARD_ID}] shutting down, flushing buffer ...`);
  server.stop(); // stop accepting new requests first
  if (draining) await draining; // wait for any in-flight drain to finish
  await scheduleFlush(); // then drain whatever remains
  redis.close();
  await db.close({ timeout: 5 });
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

console.log(
  `🦊 App node ${SHARD_ID} on :${server.port} -> redis ${REDIS_URL} ` +
    `(routing self-check: route("a")=${route("a")})`,
);
