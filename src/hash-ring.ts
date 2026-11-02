/**
 * Consistent Hashing Ring (Phase 1, Step 1)
 *
 * A deterministic router that maps an arbitrary string (a search prefix or a
 * full query) onto one of a fixed set of shards. The SAME ring construction is
 * used by three independent processes:
 *
 *   - `scripts/seed.ts` : decides which Redis shard a prefix is bulk-loaded into.
 *   - `src/lb.ts`       : decides which App Node a request is proxied to.
 *   - `src/server.ts`   : decides which shard owns a prefix during batch writes.
 *
 * Because every process imports this module and builds the ring from the same
 * `SHARDS` list + `VIRTUAL_NODES` count, `route(key)` is globally consistent:
 * a given prefix always resolves to the same shard everywhere.
 *
 * Virtual nodes (a.k.a. replicas) are used so the keyspace is spread evenly
 * across the physical shards instead of clumping — see `VIRTUAL_NODES`.
 */

/** The logical shard ids. App node `i` is paired 1:1 with `redis<i>`. */
export const SHARDS = ["1", "2", "3"] as const;
export type ShardId = (typeof SHARDS)[number];

/**
 * Replicas per physical shard placed around the ring. Higher = smoother
 * distribution at the cost of a larger ring. 150 is a common production value.
 */
export const VIRTUAL_NODES = 150;

/**
 * Changing the seed (or the hash) remaps almost every key, so the shards must
 * be re-seeded afterwards.
 */
const RING_HASH_SEED = 0;

/**
 * MurmurHash3 (32-bit) via Bun's native implementation. Returns an unsigned
 * 32-bit integer so ring positions are stable and comparable across processes.
 *
 * Replaced FNV-1a, which avalanches poorly on short near-identical inputs like
 * the vnode labels `1#0`, `1#1`, … and gave shard 3 ~42% of the ring.
 */
export function ringHash(str: string): number {
  return Bun.hash.murmur32v3(str, RING_HASH_SEED) >>> 0;
}

interface RingEntry {
  hash: number;
  shard: ShardId;
}

export class HashRing {
  /** Ring entries sorted ascending by hash for binary search. */
  private readonly ring: RingEntry[] = [];

  constructor(
    shards: readonly string[] = SHARDS,
    private readonly virtualNodes: number = VIRTUAL_NODES,
  ) {
    for (const shard of shards) {
      for (let v = 0; v < this.virtualNodes; v++) {
        this.ring.push({ hash: ringHash(`${shard}#${v}`), shard: shard as ShardId });
      }
    }
    this.ring.sort((a, b) => a.hash - b.hash);
  }

  /**
   * Map a key to the shard that owns it: hash the key, then walk clockwise to
   * the first ring entry whose position is >= the key's hash (wrapping around
   * to the start of the ring). Implemented as a binary search → O(log n).
   */
  route(key: string): ShardId {
    const h = ringHash(key);
    const ring = this.ring;

    let lo = 0;
    let hi = ring.length - 1;
    // Find the first entry with hash >= h.
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (ring[mid]!.hash < h) lo = mid + 1;
      else hi = mid;
    }
    // If h is greater than every entry, wrap to the first entry on the ring.
    const entry = ring[lo]!;
    return entry.hash >= h ? entry.shard : ring[0]!.shard;
  }
}

/** Shared singleton ring built from the canonical `SHARDS` list. */
export const ring = new HashRing();

/** Convenience: route a key using the shared ring. */
export function route(key: string): ShardId {
  return ring.route(key);
}
