/**
 * Shared configuration + small pure helpers used across the seeder, the app
 * nodes and the load balancer. Centralised here so prefix generation and shard
 * addressing are guaranteed identical everywhere (Bun auto-loads `.env`).
 */

import { SHARDS, type ShardId } from "./hash-ring";

/** Trigger a batch write the moment the in-memory buffer reaches this size. */
export const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 100);

/**
 * Safety-net flush: if a partial buffer sits idle this long it is flushed even
 * before reaching BATCH_SIZE, so low-traffic searches are never stranded. The
 * 100-hit trigger remains the primary path (Step 4).
 */
export const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS ?? 5000);

/** Default number of autocomplete suggestions returned by GET /suggest. */
export const SUGGEST_LIMIT = Number(process.env.SUGGEST_LIMIT ?? 5);

/** Default number of trending queries returned by GET /trending. */
export const TRENDING_LIMIT = Number(process.env.TRENDING_LIMIT ?? 10);

/**
 * Depth of each derived per-prefix top-K suggestion cache (`q:<prefix>` ZSET).
 * Must be >= SUGGEST_LIMIT; the extra headroom absorbs churn between rebuilds.
 */
export const CACHE_K = Number(process.env.CACHE_K ?? 50);

/**
 * Cap prefix generation length. Real users never type 200-char autocomplete
 * prefixes, and the dataset contains junk rows up to 500 chars; capping keeps
 * seeding and batch writes bounded.
 */
export const MAX_PREFIX_LEN = Number(process.env.MAX_PREFIX_LEN ?? 32);

/** Time-decay knobs (Phase 3, Step 6). Interval overridable for testing. */
export const DECAY_FACTOR = Number(process.env.DECAY_FACTOR ?? 0.9);
export const DECAY_INTERVAL_MS = Number(
  process.env.DECAY_INTERVAL_MS ?? 24 * 60 * 60 * 1000,
);

/** Redis key prefix for a suggestion ZSET. `q:<prefix>` -> ZSET(query => freq). */
export const SUGGEST_KEY_PREFIX = "q:";

/** Redis key for the trending ZSET (one per shard; merged at the LB). */
export const TRENDING_KEY = "trending";

/** Normalise a raw query/prefix: trim + lowercase. Returns "" if blank. */
export function normalize(raw: string): string {
  return raw.trim().toLowerCase();
}

/**
 * Generate every valid prefix of a query: "go" -> ["g", "go"]. Capped at
 * MAX_PREFIX_LEN. Assumes `query` is already normalised and non-empty.
 */
export function prefixesOf(query: string): string[] {
  const max = Math.min(query.length, MAX_PREFIX_LEN);
  const prefixes: string[] = new Array(max);
  for (let i = 1; i <= max; i++) prefixes[i - 1] = query.slice(0, i);
  return prefixes;
}

/** Build the `q:<prefix>` Redis key for a suggestion ZSET. */
export function suggestKey(prefix: string): string {
  return SUGGEST_KEY_PREFIX + prefix;
}

/**
 * Resolve the Redis connection URL for a shard. The seeder needs all three;
 * an app node only ever uses its own (enforced by Docker networking).
 */
export function redisUrlFor(shard: ShardId): string {
  const fromEnv = process.env[`REDIS_URL_${shard}`];
  if (fromEnv) return fromEnv;
  // Local dev default: three logical DBs on one redis instance.
  return `redis://localhost:6379/${Number(shard) - 1}`;
}

/** Resolve the HTTP base URL of the App Node paired with a shard. */
export function appUrlFor(shard: ShardId): string {
  return process.env[`APP_URL_${shard}`] ?? `http://localhost:${3000 + Number(shard)}`;
}

/** All shard ids, re-exported for convenience. */
export { SHARDS, type ShardId };
