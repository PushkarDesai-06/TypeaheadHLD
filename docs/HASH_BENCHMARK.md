# Ring Hash Benchmark

Why the consistent-hashing ring (`src/hash-ring.ts`) uses **MurmurHash3 (32-bit)**
instead of the original FNV-1a, and the evidence behind that choice.

- **Run on:** 2026-11-02, Bun 1.4.2, Intel i7-13620H (16 threads), Linux
- **Input:** all 932,701 distinct prefixes derived from `data/search_frequencies.json`
  (the exact keys the seeder routes, capped at `MAX_PREFIX_LEN = 32`)
- **Script:** `scripts/hash-compare.ts` (offline, no cluster needed)
- **Raw output:** [`benchmarks/hash-compare.txt`](./benchmarks/hash-compare.txt)

```bash
bun run bench:hash
```

All balance, movement and avalanche numbers are deterministic and reproduce exactly.
Only the speed numbers vary from run to run.

## The problem

With FNV-1a and 150 virtual nodes, shard 3 owned 42.5% of the ring and shard 2 only
25.2%. The real prefixes followed that split almost exactly, and measured by search
traffic shard 3 took **48.5%** of the load. This was not skew in the
keyspace. FNV-1a avalanches poorly on short, near-identical inputs, and every vnode
label is one (`1#0`, `1#1`, `1#2`, …), so the vnode positions clustered on the ring.

## Candidates

| Name | What it is |
| --- | --- |
| `fnv1a32 (original)` | the hash the ring shipped with (control) |
| `fnv1a32 + fmix32` | FNV-1a followed by MurmurHash3's finalizer (a hand-rolled fix we tried first) |
| `murmur32v3 (deployed)` | `Bun.hash.murmur32v3`, seed 0; this is the code's actual `ringHash` |
| `murmur64v2 (hi32)` | `Bun.hash.murmur64v2`, upper 32 bits |
| `xxHash64 (hi32)` | `Bun.hash.xxHash64`, upper 32 bits |

The 64-bit hashes are cut to 32 bits because ring positions are `u32`. With 3 shards and
under a million keys, the extra bits would make no difference to balance.

## 1. Speed (ns per hash, best of 3)

| Hash | ns |
| --- | ---: |
| fnv1a32 (original) | 54 |
| fnv1a32 + fmix32 | 68 |
| **murmur32v3 (deployed)** | **63** |
| murmur64v2 (hi32) | 92 |
| xxHash64 (hi32) | 76 |

The 64-bit variants pay for converting Bun's BigInt result into a regular number.
Every option is under 0.1 µs, against a `/suggest` request of roughly 520 µs, so speed
did not decide anything.

## 2. Balance with the ring as deployed

Share of prefixes per shard, plus the worst shard's deviation from an even 33.3%.

| Hash | 50 vnodes | 150 vnodes (deployed) | 500 vnodes |
| --- | --- | --- | --- |
| fnv1a32 (original) | 45.9% | **27.6%** (32.3 / 25.2 / 42.5) | 17.7% |
| fnv1a32 + fmix32 | 20.7% | 3.4% (33.8 / 34.0 / 32.2) | 5.7% |
| **murmur32v3 (deployed)** | 8.5% | **6.9%** (35.6 / 31.5 / 32.9) | 2.4% |
| murmur64v2 (hi32) | 21.5% | 5.2% (35.1 / 31.8 / 33.1) | 8.3% |
| xxHash64 (hi32) | 5.6% | 1.7% (33.1 / 33.9 / 33.0) | 7.1% |

The ranking between the good hashes changes from column to column. xxHash64 is best at
150 vnodes and second worst at 500. A single ring is one random draw, which is why
section 3 exists.

## 3. Salt sweep (hash quality vs luck)

Each hash builds 30 rings with different vnode label prefixes. The table shows the
spread of worst-shard deviation across those rings.

| Hash | 150 vnodes: mean / median / worst | 500 vnodes: mean / median / worst |
| --- | --- | --- |
| fnv1a32 (original) | 34.6% / 32.9% / 87.9% | 20.6% / 23.1% / 35.6% |
| fnv1a32 + fmix32 | 7.7% / 6.9% / 18.3% | 5.0% / 5.1% / 9.0% |
| **murmur32v3 (deployed)** | **7.1% / 7.1% / 13.7%** | **4.6% / 4.6% / 10.2%** |
| murmur64v2 (hi32) | 5.9% / 4.8% / 16.3% | 4.3% / 4.7% / 8.8% |
| xxHash64 (hi32) | 6.8% / 6.9% / 18.5% | 4.2% / 4.2% / 9.9% |

For an ideal random hash, a shard's share varies with a relative standard deviation of
about 1/√vnodes: 8.2% at 150 and 4.5% at 500. All four good hashes sit at that
limit and within about 2 points of each other. Only FNV-1a is an outlier. murmur32v3 has
the smallest worst case at 150 vnodes.

## 4. Adding a 4th shard

Ideal: about 25% of keys move, and every moved key goes to the new shard.

| Hash | Keys moved | Moved between existing shards |
| --- | ---: | ---: |
| fnv1a32 (original) | 28.4% | 0 |
| fnv1a32 + fmix32 | 26.4% | 0 |
| **murmur32v3 (deployed)** | **22.6%** | **0** |
| murmur64v2 (hi32) | 27.0% | 0 |
| xxHash64 (hi32) | 25.7% | 0 |

Every hash moves keys only to the new shard, because that is guaranteed by how a
consistent-hash ring works. The share moved is just the new shard's slice of the ring.

## 5. Avalanche on vnode labels

Share of output bits that flip between neighbouring labels `s#v` and `s#v+1`
(ideal 50%).

| Hash | All 32 bits | Top 8 bits |
| --- | ---: | ---: |
| fnv1a32 (original) | **31.0%** | **30.3%** |
| fnv1a32 + fmix32 | 49.3% | 48.8% |
| **murmur32v3 (deployed)** | **50.6%** | **51.0%** |
| murmur64v2 (hi32) | 48.9% | 49.4% |
| xxHash64 (hi32) | 50.4% | 50.7% |

This is the root cause. FNV-1a flips only about 31% of bits between neighbouring labels,
so they land near each other on the ring.

## 6. Key balance vs traffic balance (150 vnodes, deployed)

`keys` counts distinct prefixes per shard. `load` weights each prefix by the
number of search events whose query starts with it, which approximates `/suggest` traffic.

| Hash | Keys | Search-weighted load |
| --- | --- | --- |
| fnv1a32 (original) | 32.3 / 25.2 / 42.5 | 30.1 / 21.4 / **48.5** |
| fnv1a32 + fmix32 | 33.8 / 34.0 / 32.2 | 32.9 / 35.2 / 31.9 |
| **murmur32v3 (deployed)** | **35.6 / 31.5 / 32.9** | **37.3 / 30.7 / 32.0** |
| murmur64v2 (hi32) | 35.1 / 31.8 / 33.1 | 36.0 / 33.0 / 31.0 |
| xxHash64 (hi32) | 33.1 / 33.9 / 33.0 | 32.1 / 37.4 / 30.6 |

## Decision

**MurmurHash3 32-bit (`Bun.hash.murmur32v3`, seed 0).**

- It is a standard, well-studied hash with a native implementation, so we are not
  maintaining a hand-rolled mix like `fnv1a32 + fmix32`.
- Its balance is as good as any candidate (section 3), and it has the best
  worst case at the deployed 150 vnodes.
- It returns a plain 32-bit number, so there is no BigInt conversion like the 64-bit hashes need.
- xxHash64's 1.7% in section 2 was not used as a reason. Section 3 shows it is luck
  of the label set, not a better hash.

## Caveats and possible follow-ups

- **Remaining imbalance is variance, not a defect.** At 150 vnodes shard 1 holds 35.6% of
  keys and 37.3% of search-weighted load. Raising `VIRTUAL_NODES` to 500 lowers the
  typical deviation from about 7% to about 4.5%, at the cost of a 1,500-entry ring.
- **Changing the hash, seed or vnode count remaps almost every key.** Re-seed after any
  change: `docker compose down -v && docker compose up --build`.
- **Exact balance** would need a ring-free scheme such as jump consistent hash
  (Lamping & Veach). That trade-off is out of scope here.
- **Not adversarially safe.** Murmur is not keyed with a secret, so someone could craft
  prefixes that all land on one shard. A keyed hash (SipHash with a secret seed) would be
  needed if that became a threat.
