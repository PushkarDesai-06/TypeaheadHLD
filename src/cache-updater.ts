/**
 * Cache Updater — central derived-cache builder.
 *
 * It owns the *decision* of what each per-prefix top-K cache should contain
 * (computed from Postgres, the source of truth), but it does NOT write Redis
 * itself. Instead it POSTs each refreshed cache to the App Node that owns the
 * prefix's shard (`POST /internal/cache`), and that node writes its OWN Redis.
 * So every Redis shard has exactly one writer — its app node (textbook Step 8) —
 * and the updater never needs shard network/Redis access, only Postgres + the
 * app-node mesh.
 *
 * Loop: poll `dirty_prefixes` → claim a batch (DELETE...RETURNING) → recompute
 * each prefix's top-K from Postgres → group by owning shard → POST each group to
 * that shard's app node. A group whose POST fails is re-marked dirty to retry.
 */

import { route } from "./hash-ring";
import { db, topKForPrefix, markDirty, type CountRow } from "./db";
import { CACHE_K, SHARDS, appUrlFor, type ShardId } from "./config";

const POLL_INTERVAL_MS = Number(process.env.CACHE_POLL_INTERVAL_MS ?? 1000);
const DIRTY_BATCH = Number(process.env.CACHE_DIRTY_BATCH ?? 200);

interface PrefixUpdate {
  prefix: string;
  topK: CountRow[];
}

/** POST one shard's batch of cache updates to its app node. */
async function pushToShard(shard: ShardId, updates: PrefixUpdate[]): Promise<void> {
  const res = await fetch(`${appUrlFor(shard)}/internal/cache`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ updates }),
  });
  if (!res.ok) throw new Error(`app${shard} /internal/cache -> ${res.status}`);
}

/**
 * Process up to DIRTY_BATCH dirty prefixes. Returns how many were claimed so
 * the caller can keep draining while a full batch comes back (backlog).
 *
 * Claiming = atomically DELETE...RETURNING the oldest marks. This dodges the
 * SELECT-then-DELETE race without timestamp precision games: any `/search` that
 * bumps a prefix AFTER we claim it re-INSERTs a fresh dirty row (the old one is
 * already gone), so it is reprocessed next cycle. Rebuilds read the source of
 * truth, so a redundant rebuild is harmless (idempotent). If an app node POST
 * fails, that shard's prefixes are re-marked dirty so they retry later.
 */
async function runCycle(): Promise<number> {
  const claimed = (await db`
    DELETE FROM dirty_prefixes
    WHERE prefix IN (
      SELECT prefix FROM dirty_prefixes ORDER BY dirty_at ASC LIMIT ${DIRTY_BATCH}
    )
    RETURNING prefix
  `) as { prefix: string }[];

  if (claimed.length === 0) return 0;
  const prefixes = claimed.map((c) => c.prefix);

  // Recompute top-K for each prefix (bounded by the PG pool size).
  const updates = await Promise.all(
    prefixes.map(async (prefix) => ({ prefix, topK: await topKForPrefix(prefix, CACHE_K) })),
  );

  // Group by the shard that owns each prefix, then push one batch per shard.
  const byShard = new Map<ShardId, PrefixUpdate[]>();
  for (const u of updates) {
    const shard = route(u.prefix);
    let group = byShard.get(shard);
    if (!group) byShard.set(shard, (group = []));
    group.push(u);
  }

  await Promise.all(
    [...byShard].map(async ([shard, group]) => {
      try {
        await pushToShard(shard, group);
      } catch (err) {
        console.error(
          `[updater] shard ${shard} push failed, re-marking ${group.length} dirty:`,
          (err as Error).message,
        );
        await markDirty(group.map((g) => g.prefix));
      }
    }),
  );

  return claimed.length;
}

let running = false;
async function tick(): Promise<void> {
  if (running) return; // never overlap cycles
  running = true;
  try {
    // Drain the backlog this tick: keep going while batches come back full.
    let n: number;
    do {
      n = await runCycle();
      if (n > 0) console.log(`[updater] rebuilt ${n} prefix cache(s)`);
    } while (n === DIRTY_BATCH);
  } catch (err) {
    console.error("[updater] cycle failed:", err);
  } finally {
    running = false;
  }
}

const timer = setInterval(() => void tick(), POLL_INTERVAL_MS);

const shutdown = async () => {
  console.log("[updater] shutting down ...");
  clearInterval(timer);
  while (running) await new Promise((r) => setTimeout(r, 20)); // let the cycle finish
  await db.close({ timeout: 5 });
  process.exit(0);
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

console.log(
  `🛠️  Cache updater: poll=${POLL_INTERVAL_MS}ms batch=${DIRTY_BATCH} K=${CACHE_K} ` +
    `-> app nodes [${SHARDS.map((s) => appUrlFor(s)).join(", ")}]`,
);
