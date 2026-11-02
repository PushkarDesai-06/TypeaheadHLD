/**
 * Hash function comparison for the consistent-hashing ring.
 *
 * Compares the deployed ring hash (MurmurHash3 32-bit, `ringHash`) against the
 * original FNV-1a and the other candidates we evaluated, on every real prefix
 * derived from the dataset. Offline: needs no running cluster.
 *
 * Checks:
 *   1. speed (ns/hash)
 *   2. balance with the ring as deployed (labels `${shard}#${v}`)
 *   3. salt sweep: 30 independently-labelled rings per hash, so one lucky or
 *      unlucky label set is not mistaken for hash quality
 *   4. adding a 4th shard: share of keys moved (ideal 25%) and none moved
 *      between existing shards
 *   5. avalanche on consecutive vnode labels (ideal 50% of bits flip)
 *   6. search-weighted load per shard (each prefix weighted by search events)
 *
 * Run:  bun run bench:hash
 * Results recorded in docs/HASH_BENCHMARK.md.
 */

import { ringHash, SHARDS, VIRTUAL_NODES } from "../src/hash-ring";
import { normalize, MAX_PREFIX_LEN } from "../src/config";

const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";
const SALT_COUNT = 30;

function fnv1a32(str: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
}

function murmurFinalizer(h: number): number {
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;
  return h >>> 0;
}

const upper32 = (b: bigint) => Number(b >> 32n);

const HASHES: Record<string, (s: string) => number> = {
  "fnv1a32 (original)": fnv1a32,
  "fnv1a32 + fmix32": (s) => murmurFinalizer(fnv1a32(s)),
  "murmur32v3 (deployed)": ringHash,
  "murmur64v2 (hi32)": (s) => upper32(Bun.hash.murmur64v2(s)),
  "xxHash64 (hi32)": (s) => upper32(Bun.hash.xxHash64(s)),
};

type Ring = { pos: Uint32Array; owner: Uint8Array };

function buildRing(hash: (s: string) => number, shards: number, vnodes: number, salt = ""): Ring {
  const entries: { p: number; s: number }[] = [];
  for (let s = 1; s <= shards; s++) {
    for (let v = 0; v < vnodes; v++) entries.push({ p: hash(`${salt}${s}#${v}`), s });
  }
  entries.sort((a, b) => a.p - b.p);
  return { pos: Uint32Array.from(entries, (e) => e.p), owner: Uint8Array.from(entries, (e) => e.s) };
}

function ownerOf(ring: Ring, keyHash: number): number {
  let lo = 0;
  let hi = ring.pos.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (ring.pos[mid]! < keyHash) lo = mid + 1;
    else hi = mid;
  }
  return ring.pos[lo]! >= keyHash ? ring.owner[lo]! : ring.owner[0]!;
}

function shares(ring: Ring, keyHashes: Uint32Array, weights: Float64Array | null, shards: number): number[] {
  const totals = new Array(shards + 1).fill(0);
  keyHashes.forEach((kh, i) => (totals[ownerOf(ring, kh)] += weights ? weights[i]! : 1));
  const sum = totals.reduce((a, b) => a + b, 0);
  return totals.slice(1).map((t) => t / sum);
}

const worstDeviationPct = (sh: number[]) => Math.max(...sh.map((x) => Math.abs(x * sh.length - 1))) * 100;
const fmtShares = (sh: number[]) => sh.map((x) => (x * 100).toFixed(1).padStart(5)).join(" /");
const pad = (s: string) => s.padEnd(23);

function popcount32(x: number): number {
  x -= (x >>> 1) & 0x55555555;
  x = (x & 0x33333333) + ((x >>> 2) & 0x33333333);
  return (((x + (x >>> 4)) & 0x0f0f0f0f) * 0x01010101) >>> 24;
}

async function loadPrefixes(): Promise<{ prefixes: string[]; weights: Float64Array }> {
  const entries = JSON.parse(await Bun.file(DATA_PATH).text()) as { query: string; count: number }[];
  const searchWeight = new Map<string, number>();
  for (const { query, count } of entries) {
    const q = normalize(query).slice(0, MAX_PREFIX_LEN);
    for (let i = 1; i <= q.length; i++) {
      const p = q.slice(0, i);
      searchWeight.set(p, (searchWeight.get(p) ?? 0) + count);
    }
  }
  return { prefixes: [...searchWeight.keys()], weights: Float64Array.from(searchWeight.values()) };
}

let speedSink = 0;

function benchSpeed(prefixes: string[]) {
  console.log("\n== 1. speed (ns/hash, best of 3 over all prefixes) ==");
  for (const [name, hash] of Object.entries(HASHES)) {
    let best = Infinity;
    for (let round = 0; round < 3; round++) {
      const t = performance.now();
      for (const p of prefixes) speedSink ^= hash(p);
      best = Math.min(best, performance.now() - t);
    }
    console.log(`${pad(name)} ${((best / prefixes.length) * 1e6).toFixed(0).padStart(4)} ns`);
  }
}

function benchDeployedBalance(keyHashes: Record<string, Uint32Array>) {
  console.log("\n== 2. balance as deployed (3 shards, labels `${shard}#${v}`), share per shard ==");
  for (const vnodes of [50, 150, 500]) {
    for (const [name, hash] of Object.entries(HASHES)) {
      const sh = shares(buildRing(hash, 3, vnodes), keyHashes[name]!, null, 3);
      console.log(`vn=${String(vnodes).padEnd(3)} ${pad(name)} ${fmtShares(sh)}   worst dev ${worstDeviationPct(sh).toFixed(1).padStart(5)}%`);
    }
    console.log();
  }
}

function benchSaltSweep(keyHashes: Record<string, Uint32Array>) {
  console.log(`== 3. salt sweep (${SALT_COUNT} rings per hash, worst-shard deviation from 33.3%) ==`);
  const salts = Array.from({ length: SALT_COUNT }, (_, i) => `ring-${i}:`);
  for (const vnodes of [150, 500]) {
    console.log(`vn=${vnodes}  (ideal random hash: per-shard relative std ~ ${(100 / Math.sqrt(vnodes)).toFixed(1)}%)`);
    for (const [name, hash] of Object.entries(HASHES)) {
      const devs = salts
        .map((salt) => worstDeviationPct(shares(buildRing(hash, 3, vnodes, salt), keyHashes[name]!, null, 3)))
        .sort((a, b) => a - b);
      const mean = devs.reduce((a, b) => a + b, 0) / devs.length;
      const median = devs[Math.floor(devs.length / 2)]!;
      console.log(`  ${pad(name)} mean ${mean.toFixed(1).padStart(5)}%   median ${median.toFixed(1).padStart(5)}%   worst ${devs.at(-1)!.toFixed(1).padStart(5)}%`);
    }
  }
}

function benchAddShard(keyHashes: Record<string, Uint32Array>) {
  console.log(`\n== 4. add shard 4 (vn=${VIRTUAL_NODES}): keys moved (ideal 25.0%), all must go to shard 4 ==`);
  for (const [name, hash] of Object.entries(HASHES)) {
    const before = buildRing(hash, 3, VIRTUAL_NODES);
    const after = buildRing(hash, 4, VIRTUAL_NODES);
    let moved = 0;
    let movedBetweenOldShards = 0;
    for (const kh of keyHashes[name]!) {
      const a = ownerOf(before, kh);
      const b = ownerOf(after, kh);
      if (a === b) continue;
      moved++;
      if (b !== 4) movedBetweenOldShards++;
    }
    console.log(`${pad(name)} moved ${((moved / keyHashes[name]!.length) * 100).toFixed(1).padStart(5)}%   moved between old shards: ${movedBetweenOldShards}`);
  }
}

function benchAvalanche() {
  console.log("\n== 5. avalanche on consecutive vnode labels `s#v` vs `s#v+1` (ideal 50% bits flipped) ==");
  for (const [name, hash] of Object.entries(HASHES)) {
    let flipped = 0;
    let topByteFlipped = 0;
    let pairs = 0;
    for (let s = 1; s <= 3; s++) {
      for (let v = 0; v < VIRTUAL_NODES - 1; v++) {
        const diff = (hash(`${s}#${v}`) ^ hash(`${s}#${v + 1}`)) >>> 0;
        flipped += popcount32(diff);
        topByteFlipped += popcount32(diff >>> 24);
        pairs++;
      }
    }
    console.log(`${pad(name)} all bits ${((flipped / (pairs * 32)) * 100).toFixed(1).padStart(5)}%   top 8 bits ${((topByteFlipped / (pairs * 8)) * 100).toFixed(1).padStart(5)}%`);
  }
}

function benchWeightedLoad(keyHashes: Record<string, Uint32Array>, weights: Float64Array) {
  console.log(`\n== 6. as deployed (vn=${VIRTUAL_NODES}): prefix keys vs search-weighted load per shard ==`);
  for (const [name, hash] of Object.entries(HASHES)) {
    const ring = buildRing(hash, 3, VIRTUAL_NODES);
    console.log(`${pad(name)} keys ${fmtShares(shares(ring, keyHashes[name]!, null, 3))}   load ${fmtShares(shares(ring, keyHashes[name]!, weights, 3))}`);
  }
}

const { prefixes, weights } = await loadPrefixes();
console.log(`# Ring hash comparison`);
console.log(`bun ${Bun.version}, ${navigator.hardwareConcurrency} threads, shards=${SHARDS.length}, deployed vnodes=${VIRTUAL_NODES}`);
console.log(`real prefixes: ${prefixes.length.toLocaleString()} (from ${DATA_PATH}, max len ${MAX_PREFIX_LEN})`);

const keyHashes: Record<string, Uint32Array> = {};
for (const [name, hash] of Object.entries(HASHES)) keyHashes[name] = Uint32Array.from(prefixes, hash);

benchSpeed(prefixes);
benchDeployedBalance(keyHashes);
benchSaltSweep(keyHashes);
benchAddShard(keyHashes);
benchAvalanche();
benchWeightedLoad(keyHashes, weights);
