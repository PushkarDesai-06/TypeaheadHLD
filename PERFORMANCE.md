# Performance Report

Covers the assignment's Non-Functional asks (§10) and the batch-write evidence
(§8): **suggestion latency (incl. p95), cache hit rate, and the write reduction
from batching.** Every number below is reproducible against a running cluster
with the included harness — no synthetic figures.

```bash
docker compose up --build          # bring the cluster up + seed
bun run bench                      # latency + hit rate + write reduction
bun run demo:recency               # §7 basic-vs-recency ranking demonstration
```

## Measurement setup (honest disclosure)

- **Topology:** the full `docker-compose` cluster — 1 LB → 3 app nodes → 3 Redis
  shards + Postgres + cache-updater. All requests go through the public LB
  (`http://localhost:8080`), so figures include the consistent-hash proxy hop,
  not just a raw Redis read.
- **Host:** single developer machine, all containers local. **Latency is
  therefore optimistic** — there is no real network between the LB and the app
  nodes. Treat the absolute numbers as a *lower bound*; the **shape** (cache vs
  DB-fallback, batched vs per-request writes) is what generalises.
- **Dataset:** 93,387 unique queries / 1.72M search events (see
  [Dataset](./README.md#dataset)).
- **Tooling:** `scripts/bench.ts` uses only the public HTTP API + `/metrics`;
  4,000 `/suggest` requests at concurrency 32 after a 200-request warm-up.

## 1. Suggestion latency (`GET /suggest`)

4,000 requests over ~12.5k distinct dataset prefixes, concurrency 32:

| Ranking            | mean   | p50    | p90    | p95        | p99    | max    | throughput   |
| ------------------ | ------ | ------ | ------ | ---------- | ------ | ------ | ------------ |
| `basic` (count)    | 0.63ms | 0.52ms | 1.05ms | **1.21ms** | 2.26ms | 2.77ms | ~50,400 req/s |
| `recency` (blended)| 0.63ms | 0.52ms | 1.12ms | **1.35ms** | 2.49ms | 3.31ms | ~50,800 req/s |

**Why recency is just as fast:** both rankings are a single `ZREVRANGE` over a
pre-computed per-prefix ZSET (`q:<prefix>` vs `qr:<prefix>`). The blend is
computed at **write/rebuild** time by the cache-updater, never on the read path —
so the enhanced §7 ranking adds **no read latency**.

## 2. Cache hit rate (`/suggest` cache vs Postgres fallback, §6)

`/suggest` reads the Redis shard first and **falls back to Postgres on a miss**,
then warms the cache (marks the prefix dirty) if Postgres found anything. The
`source` field on every response (`cache` or `db`) makes hit vs miss directly
observable, and `/metrics` counts both.

### Why a seeded-only hit rate would be circular

The seeder pre-caches the top-K of **every prefix of every dataset query**
(932,700 prefixes). Benchmarking with dataset prefixes alone therefore gives
100% by construction and says nothing about real traffic. So the benchmark sends
a mix that includes prefixes the cache has never seen:

| Category | Share | What it is |
| -------- | ----: | ---------- |
| `seeded` | 70% | 2 to 4 char prefixes of dataset queries |
| `typo`   | 20% | a 3 to 8 char prefix of a real query with one edit after the first letter (substitute, delete, insert, or swap adjacent letters) |
| `novel`  | 10% | random 4 to 8 letter strings that are not a prefix of any dataset query |

The 70/20/10 split is an assumption, not measured traffic. Change it with
`BENCH_MIX=seeded,typo,novel`.

### Results

20,000 requests, concurrency 32, `rank=basic`, on the cluster reseeded with
murmur32 routing (2026-11-02). Raw output:
[`docs/benchmarks/bench-cache-mix.txt`](./docs/benchmarks/bench-cache-mix.txt).

| Category | Requests | Hit rate | Empty result | p50 | p95 | p99 |
| -------- | -------: | -------: | -----------: | --: | --: | --: |
| seeded   | 14,057 | 100.0% | 0.0%   | 0.73ms | 1.95ms | 4.21ms |
| typo     | 3,998  | 21.0%  | 79.0%  | 0.96ms | 2.61ms | 5.57ms |
| novel    | 1,945  | 0.0%   | 100.0% | 1.02ms | 2.59ms | 6.16ms |
| **overall** | **20,000** | **74.5%** | 25.5% | 0.80ms | 2.17ms | 4.55ms |

**What this shows:**

- **A prefix hits if and only if it exists in the data.** 21.0% of the typos
  happened to still be real dataset prefixes (839 / 3,998), and exactly 21.0% of
  typos hit. Every miss in the run came back with an empty result, so there was
  no case where Postgres had suggestions the cache was missing. The cache covers
  every prefix that has an answer.
- **The remaining misses are prefixes with no answer at all.** No cache can serve
  them, but each one still costs a Postgres query. They are cheap (p50 1.02ms vs
  0.73ms for a hit) because the `text_pattern_ops` index finds nothing quickly,
  but at the measured ~31k req/s a 25.5% miss share is roughly 7,900 Postgres
  queries per second.
- **There is no negative caching.** Requesting the same unseen prefix twice went
  to Postgres both times (5 / 5 samples), because an empty result is never
  cached. So a user typing a typo hits Postgres on every keystroke.
- **The hit rate depends on the mix**, roughly `seeded% + 0.21 × typo%`. A
  traffic mix with more typos or new terms will score lower.

**Possible improvement:** cache empty results as a short-TTL negative entry.
When someone later searches a query with that prefix, the prefix is marked dirty
and the cache-updater overwrites the negative entry with real suggestions.

### Fallback and rewarm

The miss path was verified by evicting a live key (re-checked after the reseed):

```text
/suggest?q=goo            -> source: cache         # served from redis1
redis1> DEL q:goo                                  # evict the hot key
/suggest?q=goo            -> source: db            # Postgres fallback (§6)
…cache-updater rebuilds (~2s)…
/suggest?q=goo            -> source: cache         # rewarmed automatically
```

## 3. Write reduction from batching (§8)

App nodes buffer `POST /search` in memory and flush to Postgres in batches
(`BATCH_SIZE=100`, or after `FLUSH_INTERVAL_MS`). Measured via `/metrics`:

| Metric                          | Value     |
| ------------------------------- | --------- |
| Searches submitted              | 2,000     |
| Postgres batch **transactions** | 22        |
| **Write reduction**             | **90.9×** |
| Unique rows upserted            | 410       |

So **~91 search requests collapsed into a single database transaction**. Without
batching, those 2,000 searches would be 2,000 synchronous writes (plus per-row
index maintenance); batching turns them into 22 multi-row `UPSERT`s. The
cumulative cluster figure across the whole session was **83.3×**
(`searches=2,417 / batches=29`).

**Knobs:** larger `BATCH_SIZE` → higher reduction but more data at risk on crash;
shorter `FLUSH_INTERVAL_MS` → fresher data but smaller batches. See the
[failure trade-offs](./README.md#batch-writes--failure-trade-offs-8).

## 4. Distribution across shards (consistent hashing)

The seeder routed 1,352,775 derived top-K rows across the three shards by
`hash(prefix)`:

| Shard | Cache rows (top-K members) | Redis keys (`q:` + `qr:` + trending) |
| ----- | -------------------------- | ------------------------------------ |
| 1     | 431,490                    | 602,437                              |
| 2     | 334,549                    | 470,035                              |
| 3     | 586,736                    | 793,371                              |

The spread is uneven because the **prefix keyspace itself is skewed** (English
queries clump on common leading characters), not because the ring is unbalanced —
the ring distributes *random* keys to within ±25% (see `bun test`). Example
ownership (`GET /cache/debug?prefix=…`): `google → app1`, `yahoo → app2`,
`facebook → app3`.

## Reproducing

```bash
bun run bench                                   # table 1–3
BENCH_SUGGEST=20000 bun run bench               # §2 mixed-workload hit rate as reported
BENCH_MIX=50,30,20 bun run bench                # try a different seeded/typo/novel mix
BENCH_RANK=recency bun run bench                # recency-rank latency
bun run demo:recency                            # §7 reorder, before/after
curl -s localhost:8080/metrics | jq             # live cluster counters
docker compose exec redis1 redis-cli DEL q:goo  # force a cache miss to see fallback
```
