# Performance Report

Covers the assignment's Non-Functional asks (§10) and the batch-write evidence
(§8): **suggestion latency (incl. p95), cache hit rate, and the write reduction
from batching.** Every number below is reproducible against a running cluster
with the included harness, and the raw output of every run is committed in
[`docs/benchmarks/`](./docs/benchmarks/).

```bash
docker compose up --build          # bring the cluster up + seed
bun run bench:latency              # §1 fixed-rate latency sweep
bun run bench                      # §2 hit rate
bun run bench:writes               # §3 write reduction at several traffic rates
bun run bench:hash                 # §4 ring hash comparison (offline)
bun run demo:recency               # §7 basic-vs-recency ranking demonstration
```

## Measurement setup (honest disclosure)

- **Topology:** the full `docker-compose` cluster: 1 LB, 3 app nodes, 3 Redis
  shards, Postgres and the cache-updater. Every request goes through the LB, so
  the figures include the consistent-hash proxy hop, not just a raw Redis read.
- **Host:** one machine (Intel i7-13620H, 6 performance + 4 efficiency cores,
  Linux, Bun 1.4.2, Docker). There is no real network between the services, which
  flatters latency, but the services also share CPU caches and memory bandwidth,
  which works against it. So the absolute numbers are **neither a strict lower nor
  upper bound** for a real deployment. The shape (where the knee is, what
  saturates first) is what generalises.
- **CPU isolation for §1:** the cluster's containers were pinned to CPUs 0 to 9
  (`docker update --cpuset-cpus 0-9`), the load generator to CPUs 10 to 13
  (`taskset`), and the `docker stats` sampler to CPUs 14 to 15, so the generator
  and the cluster never competed for the same cores.
- **Traffic path for §1:** requests were sent straight to the LB container's IP
  instead of `localhost:8080`. Docker's port-forwarding proxy opens a new backend
  connection per client connection; in a first attempt it left 13,000+ sockets
  in `TIME_WAIT` and failed under load. That proxy does not exist in a real
  deployment, so it was taken out of the measurement.
- **Dataset:** 93,387 unique queries / 1.72M search events (see
  [Dataset](./README.md#dataset)).

## 1. Suggestion latency (`GET /suggest`)

### Method

Measured with `scripts/bench-latency.ts` (`bun run bench:latency`), a fixed-rate
(open-loop) load generator:

- **Requests go out on a fixed schedule and never wait for responses.** A
  generator that waits for each response before sending the next (as an earlier
  version of this report did) sends less traffic whenever the server stalls, so
  the stall barely shows up in the percentiles. Real users keep typing whether
  the server is slow or not. With a fixed schedule, traffic keeps arriving during
  a stall and the queueing it causes is measured in full.
- **The schedule is verified.** Every request records how late it was sent
  relative to its scheduled time. A step only counts if p99 send lag stayed under
  5 ms; otherwise the generator, not the server, was the bottleneck. Observed
  send lag stayed at or below 2.6 ms in every reported step.
- **Failures count as infinite latency**, so they can never hide inside a
  percentile. A step with more than 0.1% errors or timeouts (5 s) is marked
  saturated, and the sweep stops there.
- **Each rate runs for 30 s after a 3 s warm-up, repeated 3 times.** Tables show
  the median of the three runs, with the range across them in brackets.
- **Load comes from 2 generator threads**, each with its own heap and garbage
  collector, so one thread's pause cannot hold back the whole schedule.
- **Workloads:** the 70/20/10 mix from §2 (dataset prefixes, typos, novel
  strings) is the main result. A dataset-prefixes-only run isolates the pure
  cache path, and a `rank=recency` run checks the §7 ranking.

All 54 measured steps below (3 workloads × 6 rates × 3 runs) were valid: zero
failed requests and a worst p99 send lag of 2.42 ms. Run on 2026-11-02, raw
output in `docs/benchmarks/latency-*.{log,json}`.

### Headline

| Workload | Sustained rate | p50 | **p95** | p99 | p99.9 |
| -------- | -------------: | --: | ------: | --: | ----: |
| 70/20/10 mix, `basic` | **10,000 req/s** | 0.28ms | **0.74ms** | 1.85ms | 10.3ms |
| Dataset prefixes only (pure cache path), `basic` | **15,000 req/s** (highest tested) | 0.27ms | **0.60ms** | 1.17ms | 14.6ms |
| 70/20/10 mix, `recency` | **12,500 req/s** | 0.30ms | **0.82ms** | 2.40ms | 16.4ms |

"Sustained rate" is the highest tested rate at which **all three runs** kept p99
at or under 10 ms with no failures. Latencies are the medians at that rate.

### 70/20/10 mix, `rank=basic` (main result)

[`latency-basic-mix-70-20-10.log`](./docs/benchmarks/latency-basic-mix-70-20-10.log)

| Rate (req/s) | p50 | p95 | p99 | p99.9 | Max |
| -----------: | --- | --- | --- | ----- | --- |
| 2,500  | 0.25 [0.25 to 0.26] | 0.52 [0.50 to 0.55] | 0.87 [0.82 to 0.91] | 1.45 [1.43 to 2.12] | 6 to 10 |
| 5,000  | 0.25 [0.25 to 0.26] | 0.57 [0.56 to 0.61] | 0.93 [0.93 to 1.05] | 1.85 [1.69 to 4.17] | 5 to 14 |
| 7,500  | 0.27 [0.27 to 0.30] | 0.68 [0.61 to 1.05] | 1.54 [1.30 to 2.88] | 8.11 [6.05 to 9.44] | 17 to 19 |
| **10,000** | **0.28 [0.28 to 0.29]** | **0.74 [0.69 to 0.74]** | **1.85 [1.20 to 2.19]** | 10.28 [6.21 to 11.78] | 23 to 26 |
| 12,500 | 0.31 [0.29 to 0.32] | 1.02 [0.84 to 1.59] | 10.75 [7.81 to 12.41] | 35.61 [30.66 to 44.13] | 45 to 66 |
| 15,000 | 0.33 [0.33 to 0.36] | 4.86 [3.32 to 22.88] | 26.78 [24.54 to 121.68] | 49.91 [48.81 to 175.20] | 60 to 186 |

All values in ms, median of 3 runs with the range in brackets. Cache hit rate
held at 74.2%, matching §2.

### Dataset prefixes only, `rank=basic` (pure cache path)

[`latency-basic-mix-100-0-0.log`](./docs/benchmarks/latency-basic-mix-100-0-0.log)

| Rate (req/s) | p50 | p95 | p99 | p99.9 | Max |
| -----------: | --- | --- | --- | ----- | --- |
| 2,500  | 0.22 [0.22 to 0.22] | 0.55 [0.41 to 0.71] | 1.19 [0.74 to 1.41] | 2.12 [2.01 to 2.56] | 6 to 9 |
| 5,000  | 0.23 [0.22 to 0.23] | 0.44 [0.40 to 0.48] | 0.72 [0.69 to 1.06] | 2.18 [1.64 to 4.64] | 4 to 11 |
| 7,500  | 0.23 [0.23 to 0.24] | 0.51 [0.48 to 0.57] | 0.88 [0.82 to 1.48] | 2.15 [2.05 to 5.89] | 6 to 13 |
| 10,000 | 0.24 [0.23 to 0.24] | 0.54 [0.51 to 0.63] | 0.92 [0.86 to 1.30] | 3.11 [2.45 to 5.73] | 9 to 14 |
| 12,500 | 0.25 [0.25 to 0.26] | 0.57 [0.56 to 0.63] | 0.95 [0.95 to 1.89] | 3.71 [1.95 to 16.11] | 12 to 26 |
| **15,000** | **0.27 [0.26 to 0.27]** | **0.60 [0.54 to 0.60]** | **1.17 [1.02 to 1.52]** | 14.61 [4.87 to 20.83] | 13 to 34 |

### 70/20/10 mix, `rank=recency`

[`latency-recency-mix-70-20-10.log`](./docs/benchmarks/latency-recency-mix-70-20-10.log)

| Rate (req/s) | p50 | p95 | p99 | p99.9 | Max |
| -----------: | --- | --- | --- | ----- | --- |
| 2,500  | 0.26 [0.26 to 0.27] | 0.56 [0.50 to 0.82] | 1.01 [0.77 to 1.95] | 2.88 [1.24 to 7.03] | 5 to 16 |
| 5,000  | 0.25 [0.24 to 0.26] | 0.62 [0.48 to 0.64] | 1.05 [0.77 to 1.30] | 3.00 [1.46 to 5.09] | 6 to 10 |
| 7,500  | 0.27 [0.27 to 0.28] | 0.63 [0.62 to 0.73] | 1.07 [1.00 to 1.21] | 3.79 [2.95 to 4.67] | 9 to 15 |
| 10,000 | 0.29 [0.29 to 0.29] | 0.83 [0.77 to 0.84] | 2.38 [1.88 to 2.61] | 14.12 [13.17 to 14.76] | 21 to 24 |
| **12,500** | **0.30 [0.30 to 0.31]** | **0.82 [0.81 to 0.83]** | **2.40 [1.63 to 2.95]** | 16.44 [13.26 to 18.22] | 24 to 26 |
| 15,000 | 0.34 [0.32 to 0.34] | 3.50 [1.23 to 4.68] | 20.94 [15.99 to 22.01] | 30.10 [28.90 to 37.49] | 36 to 51 |

### What the numbers show

- **The median barely moves; overload shows up in the tail first.** Across the
  mixed sweep p50 only rises from 0.25 to 0.33 ms, while p99 goes from 0.87 ms to
  27 ms. A report that only quoted averages or p50 would miss the knee entirely.
- **The Postgres miss path sets the knee for realistic traffic.** The pure cache
  path held p99 around 1 ms all the way to 15,000 req/s, while the 70/20/10 mix
  (25% of requests miss and query Postgres) crossed 10 ms at 12,500. Postgres CPU
  rose from 7% to 48% across the mixed sweep and stayed near zero in the
  cache-only one. Caching empty results (see §2) would remove most of that load.
- **Recency ranking costs nothing measurable.** On a hit both rankings are a
  single `ZREVRANGE` over a precomputed ZSET (`q:<prefix>` vs `qr:<prefix>`); the
  blend is computed by the cache-updater, never on the read path. The recency
  query on a miss is heavier in principle (it orders every candidate by a
  blended score), but almost every miss in this mix is a prefix with no
  candidates at all, so there is nothing to sort. The recency sweep landed within
  the run-to-run noise of the basic one (better at 12,500, worse at 10,000).
- **Results vary between sessions near the knee.** The first full sweep of the
  same workload (see below) measured p99 of 2.76 ms at 10,000 req/s and 3.68 ms
  at 12,500, against 1.85 ms and 10.75 ms here. Well below the knee the two
  sweeps agree; at the knee, small differences in background activity decide
  whether queues build. That is why the sustained rate is set conservatively at
  10,000 req/s for the mix, where both sweeps stayed under 3 ms at p99.

### Why the previous numbers were withdrawn

An earlier version of this section reported p95 of 1.21 ms and "~50,000 req/s".
Those came from 4,000 requests sent as one ~80 ms burst by a generator that waited
for each response before sending the next. The burst was too short to sustain
anything, p99 rested on 40 samples, and waiting on responses hid stalls from the
tail. The fixed-rate sweep shows the cluster cannot sustain anywhere near 50,000
req/s: the mixed workload reaches its knee at 10,000 to 12,500 and collapses by
17,500.

### What happens past capacity: the LB does not recover

In a first full sweep (raw output:
[`overload-collapse-basic-mix-70-20-10.log`](./docs/benchmarks/overload-collapse-basic-mix-70-20-10.log)),
the 70/20/10 mix ran clean up to 15,000 req/s and then collapsed at 17,500 req/s.
Every request timed out in all three runs, including the two that started after a
5 s cool-down. After the load stopped, the LB:

- stayed **unresponsive for about 5.5 minutes** (load stopped 12:32:20, first
  successful request 12:37:47),
- held up to **4.8 GiB of memory** at about 130% CPU while draining its backlog,
- logged `upstream app3 unreachable` errors, while the app nodes themselves stayed
  healthy throughout.

The cause is that the LB has no way to limit or shed load. Its upstream `fetch`
has no timeout and there is no cap on in-flight requests, so once arrivals
outpace it, it keeps accepting work and then spends minutes finishing requests
whose clients gave up long ago. That is a robustness gap separate from raw speed:
a production LB should time out upstream calls, cap concurrent work, and reject
excess load quickly (HTTP 503) so it recovers as soon as the spike ends.

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
  but at the sustainable rate measured in §1 (10,000 req/s) a 25.5% miss share
  is roughly 2,500 Postgres queries per second.
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

App nodes buffer `POST /search` in memory and write to Postgres in batches. A
node writes a batch the moment its buffer holds `BATCH_SIZE` (100) searches, and
a timer writes whatever is buffered every `FLUSH_INTERVAL_MS` (5 s). Write
reduction is **searches accepted per Postgres transaction**: without batching,
every search would be its own transaction.

### Method

`bun run bench:writes` ([`scripts/bench-writes.ts`](./scripts/bench-writes.ts))
sends searches at a fixed rate for 60 s, waits for every buffer to empty, and
reads the app nodes' own counters (`searchesReceived`, `batchesFlushed`,
`rowsUpserted`) through the LB's `/metrics`. Six rates, from 1 to 1,500 searches
per second across the cluster, 3 repeats each. Queries are drawn from the
dataset weighted by their real frequency, so popular queries repeat inside a
batch the way they would in production. Postgres's own `deadlocks` counter is
read before and after every step.

Which trigger fires depends on the rate, so the ratio is a function of traffic,
not a constant. Each step is therefore checked against a simulation of the flush
logic (same send schedule, each node's measured share of traffic, a random timer
phase, instant commits, 200 runs). The gap between the simulation and the
measurement is what the real database costs.

### Results

Raw output: [`write-reduction.log`](./docs/benchmarks/write-reduction.log) /
[`.json`](./docs/benchmarks/write-reduction.json). Median [range] of 3 runs.

| Searches/s (cluster) | Searches/s per node | **Searches per transaction** | Simulated | Deadlocks | Buffers empty after load stops |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1     | ~0.3  | 1.8× [1.8–2.0]    | 1.9×  | 0 | within one 5 s tick |
| 5     | ~1.7  | 8.1× [8.1–8.1]    | 7.8×  | 0 | within one 5 s tick |
| 20    | ~7    | 32.4× [32.4–33.3] | 30.9× | 0 | within one 5 s tick |
| 60    | ~20   | 63.2× [62.1–72.0] | 63.2× | 0 | within one 5 s tick |
| 300   | ~100  | **88.7×** [88.2–88.7] | 88.6× | 0 | within one 5 s tick |
| 1,500 | ~500  | **97.9×** [97.6–98.0] | 97.8× | 0 | within one 5 s tick |

No request failed in any step.

### What the numbers show

- **The ratio is set by traffic.** Below about 20 searches/s per node, the
  buffer never reaches 100 before the 5 s timer fires, so each transaction holds
  roughly 5 seconds of that node's searches. At 1 search/s for the whole cluster
  that is under 2 searches per transaction. Batching only approaches its 100×
  ceiling once each node gets well over 20 searches/s.
- **Around 20 searches/s per node, both triggers compete.** The timer runs on a
  fixed clock, so an interval that receives slightly more than 100 searches is
  written as one full batch and one small one. That is why 60/s gives 63×, not
  100×. The simulation predicts it exactly.
- **The measurement now matches the flush logic at every rate.** Measured and
  simulated agree within the run-to-run range everywhere, so the database adds no
  extra transactions, and every buffer empties within one timer tick of the load
  stopping, so the write path keeps up at 1,500 searches/s.
- **Batching cuts transactions, not rows.** A full batch of 100 searches upserts
  about 90 distinct query rows (dedup only merges repeats inside one batch, and
  the dataset has a long tail) plus about 1,170 `dirty_prefixes` rows, one per
  affected prefix, all in one transaction. What batching removes is the per-search
  round trip, transaction and commit (WAL flush), and it moves all of that off the
  request path: `POST /search` returns `202` before anything touches Postgres.

### What the first measurement found

The first sweep ran on the code as committed at `6eca483`
([`write-reduction-before-lock-order.log`](./docs/benchmarks/write-reduction-before-lock-order.log)),
and it exposed two bugs in the write path.

| Searches/s | Code at `6eca483` | Lock order fixed only | Both fixes (above) |
| ---: | ---: | ---: | ---: |
| 20    | 27.9×, 2 deadlocks | 31.6×, 0 | 32.4×, 0 |
| 60    | 48.6×, 6 deadlocks | 52.9×, 0 | 63.2×, 0 |
| 300   | 87.0×, 202 deadlocks, buffers took 10–15 s to empty | 42.4×, 0 | 88.7×, 0 |
| 1,500 | 99.8×, 239 deadlocks, buffers took 23–36 s to empty | 27.1×, 0 | 97.9×, 0 |

1. **Concurrent flushes deadlocked.** The three app nodes share popular prefixes
   (`g`, `go`, ...), and each flush upserted its ~1,100 `dirty_prefixes` rows in
   whatever order its `Set` produced. Two flushes could each lock a row the other
   needed; Postgres waited out `deadlock_timeout` (1 s), killed one, and the app
   re-queued that chunk. The 300 and 1,500/s ratios still looked good, but only
   because stalled writes let the buffer pile up into full chunks. The buffers
   took up to 36 s to empty once the load stopped, so the write path was falling
   behind. **Fix:** every writer now locks rows in one global order. Flushes and
   `markDirty` sort their rows by byte order (`inLockOrder` in
   [`src/db.ts`](./src/db.ts), matching Postgres `COLLATE "C"`), the recency decay
   locks its rows in the same order before updating, and the cache-updater claims
   dirty prefixes with `FOR UPDATE SKIP LOCKED` so it never waits on a flush.
   ([`write-reduction-lock-order-drain-to-empty.log`](./docs/benchmarks/write-reduction-lock-order-drain-to-empty.log))
2. **The drain wrote a trail of tiny batches.** Once commits were fast, the
   ratio dropped to 27× at 1,500/s. A threshold flush kept draining until the
   buffer was empty, so after each 100-row commit it immediately wrote the few
   searches that had arrived during that commit, then the even fewer that arrived
   during that one. One node's log showed 964 one-search transactions next to 953
   full ones. **Fix:** the threshold trigger now writes only full chunks; a
   partial chunk is written only by the timer or on shutdown
   ([`src/server.ts`](./src/server.ts)).

A third bug surfaced in the cache-updater logs during the same runs: after a
recency decay touched a large number of queries, re-marking their prefixes dirty
sent more than Postgres's 65,535 parameters in one statement and failed, so
those recency caches were never rebuilt. `markDirty` now writes in chunks of
10,000 rows.

**Data note:** the sweeps added about 1.08 million search events (frequency
weighted, to queries that already exist) on top of the dataset's 1.72 million,
so absolute counts in Postgres are inflated until the cluster is reseeded
(`docker compose down -v && docker compose up --build`).

**Knobs:** a larger `BATCH_SIZE` raises the ceiling at high traffic but puts more
searches at risk if a node crashes; a shorter `FLUSH_INTERVAL_MS` makes counts
fresher but lowers the ratio at low traffic, where the timer does all the
flushing. See the
[failure trade-offs](./README.md#batch-writes--failure-trade-offs-8).

## 4. Distribution across shards (consistent hashing)

The seeder routes 1,352,775 derived top-K rows across the three shards by
`ringHash(prefix)` (MurmurHash3, 150 virtual nodes per shard):

| Shard | Cache rows, before (FNV-1a) | Cache rows, now (MurmurHash3) | Redis keys, before | Redis keys, now |
| ----- | --------------------------: | ----------------------------: | -----------------: | --------------: |
| 1     | 431,490 (31.9%)             | 482,139 (35.6%)               | 602,437 (32.3%)    | 664,778 (35.6%) |
| 2     | 334,549 (24.7%)             | 424,660 (31.4%)               | 470,035 (25.2%)    | 587,102 (31.5%) |
| 3     | 586,736 (**43.4%**)         | 445,976 (33.0%)               | 793,371 (**42.5%**) | 613,520 (32.9%) |

**The original imbalance was a hash defect, not skew in the data.** The ring
first used FNV-1a, which scrambles short, near-identical inputs poorly, and every
virtual node label is one (`1#0`, `1#1`, …). The vnode positions clustered and
shard 3 owned 42.5% of the ring. Real prefixes landed on each shard in exactly the
proportion of ring it owned, and weighted by search traffic shard 3 would have
taken 48.5% of the load. Switching to MurmurHash3 brought the worst shard from
about 30% above an even split to about 7%. The live Redis key split after
reseeding (35.6 / 31.5 / 32.9) matches the offline prediction exactly.

The remaining 7% is chance, not a defect: with 150 virtual nodes any good hash
varies by about 1/√150 ≈ 8% per shard. The full comparison of FNV-1a, xxHash64,
murmur32 and murmur64 (balance, a 30-ring salt sweep, shard addition, avalanche
and speed) is in [`docs/HASH_BENCHMARK.md`](./docs/HASH_BENCHMARK.md).

Example ownership (`GET /cache/debug?prefix=…`): `google → app1`,
`gmail → app2`, `ebay → app3`.

## Reproducing

§1 as reported (cluster up; pinning optional but recommended):

```bash
LB_IP=$(docker inspect typeahed-lb-1 --format '{{(index .NetworkSettings.Networks "typeahed_mesh").IPAddress}}')
docker update --cpuset-cpus 0-9 $(docker compose ps -q)          # isolate the cluster
for spec in "basic 70,20,10" "basic 100,0,0" "recency 70,20,10"; do
  set -- $spec
  BASE_URL=http://$LB_IP:8080 BUN_CONFIG_MAX_HTTP_REQUESTS=4096 \
  BENCH_RATES=2500,5000,7500,10000,12500,15000 BENCH_RANK=$1 BENCH_MIX=$2 \
  BENCH_DOCKER_STATS=1 BENCH_SAMPLER_CPUS=14,15 \
  taskset -c 10-13 bun run bench:latency
done
docker update --cpuset-cpus 0-15 $(docker compose ps -q)         # undo pinning
```

Everything else:

```bash
bun run bench                                   # §2 hit rate
BASE_URL=http://$LB_IP:8080 BENCH_PG_STATS=1 bun run bench:writes   # §3 as reported
bun run bench:hash                              # §4 ring hash comparison
BENCH_SUGGEST=20000 bun run bench               # §2 mixed-workload hit rate as reported
BENCH_MIX=50,30,20 bun run bench                # try a different seeded/typo/novel mix
BENCH_RANK=recency bun run bench                # §2 hit rate under recency ranking
bun run demo:recency                            # §7 reorder, before/after
curl -s localhost:8080/metrics | jq             # live cluster counters
docker compose exec redis1 redis-cli DEL q:goo  # force a cache miss to see fallback
```
