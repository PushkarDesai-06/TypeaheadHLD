# API Documentation

All endpoints are served by the **Load Balancer** (the single public entrypoint,
`http://localhost:8080`). The LB hashes the prefix/query with the consistent-hash
ring and proxies to the owning app node; clients never talk to a shard directly.

| Method | Path                            | Purpose                                  |
| ------ | ------------------------------- | ---------------------------------------- |
| GET    | `/suggest?q=<prefix>`           | Autocomplete suggestions (§4.1, §7)      |
| POST   | `/search`                       | Submit a completed search (§4.2)         |
| GET    | `/trending`                     | Global trending queries (§7 board)       |
| GET    | `/cache/debug?prefix=<prefix>`  | Which shard owns a prefix; hit/miss (§5) |
| GET    | `/metrics`                      | Cluster counters (latency/hit-rate/writes)|

---

## `GET /suggest?q=<prefix>[&rank=basic|recency][&limit=N]`

Returns up to `limit` (default 10) suggestions whose text starts with `<prefix>`,
read from the owning shard's Redis cache. **On a cache miss it falls back to
Postgres** (`source: "db"`) and warms the cache for next time (§6).

| Param   | Default  | Notes                                                          |
| ------- | -------- | -------------------------------------------------------------- |
| `q`     | —        | The prefix. Trimmed + lowercased. Empty → `{ suggestions: [] }`.|
| `rank`  | `basic`  | `basic` = all-time count; `recency` = recency-blended (§7).    |
| `limit` | `10`     | Positive integer; non-numeric input falls back to the default. |

**Request**

```bash
curl "http://localhost:8080/suggest?q=go&rank=basic&limit=5"
```

**Response** `200`

```json
{
  "shard": "3",
  "prefix": "go",
  "rank": "basic",
  "source": "cache",
  "suggestions": ["google", "google.com", "goggle", "google earth", "google com"]
}
```

- `shard` — which shard served it (the consistent-hash owner of the prefix).
- `source` — `"cache"` (Redis hit) or `"db"` (Postgres fallback, §6).
- `rank=recency` returns the same shape, ordered by the blended recency score so
  currently-active queries rank higher. See [Recency ranking](../README.md#trending--recency-aware-ranking-7).

Edge cases (all return `200` with `suggestions: []`, never an error): empty input,
whitespace-only input, mixed case (normalised), prefixes with no matches.

---

## `POST /search`

Records a completed search. The query is **buffered in memory and `202` is
returned immediately** — no synchronous database write (§4.2, §8). The dummy
response body is `{ "message": "Searched" }` as required by §4.2 / §5.

**Request**

```bash
curl -X POST "http://localhost:8080/search" \
  -H "content-type: application/json" \
  -d '{"query":"google maps"}'
```

**Response** `202 Accepted`

```json
{ "message": "Searched", "query": "google maps", "buffered": 1 }
```

The count update becomes visible in `/suggest` and `/trending` after the buffer
flushes (at `BATCH_SIZE`, or within `FLUSH_INTERVAL_MS`) and the cache-updater
rebuilds the affected prefix caches. Errors: `400` for missing/invalid body.

---

## `GET /trending?[limit=N]`

The globally highest-scoring recent queries. Each shard keeps its own `trending`
ZSET (incremented on flush, decayed daily); the LB **fans out to all shards and
merges** (each query lives on exactly one shard).

```bash
curl "http://localhost:8080/trending?limit=3"
```

```json
{ "trending": [
  { "query": "go surge demo", "score": 416 },
  { "query": "myspace demo",  "score": 61 },
  { "query": "dictionary demo","score": 61 }
] }
```

---

## `GET /cache/debug?prefix=<prefix>`

Shows which cache node owns a prefix and whether it is currently a hit or miss
there — the §5 debug/routing endpoint. Routed with the **same** consistent hash
as `/suggest`, so it reports the real owner.

```bash
curl "http://localhost:8080/cache/debug?prefix=go"
```

```json
{
  "prefix": "go",
  "node": "app3",
  "shard": "3",
  "status": "hit",
  "cached": 50,
  "recencyCached": 50
}
```

- `node` / `shard` — the consistent-hash owner of the prefix.
- `status` — `hit` if the all-time `q:<prefix>` cache is populated, else `miss`.
- `cached` / `recencyCached` — entry counts in the `q:` and `qr:` ZSETs.

---

## `GET /metrics`

Cluster-wide observability counters (LB sums the per-node values). Powers the
[performance report](../PERFORMANCE.md).

```bash
curl "http://localhost:8080/metrics"
```

```json
{
  "total": {
    "searchesReceived": 2417,
    "batchesFlushed": 29,
    "rowsUpserted": 417,
    "cacheHits": 4211,
    "cacheMisses": 0,
    "buffered": 0,
    "cacheHitRate": 1.0,
    "writeReduction": 83.3
  },
  "nodes": [ { "shard": "1", "...": "per-node counters" } ]
}
```

| Field              | Meaning                                                |
| ------------------ | ------------------------------------------------------ |
| `searchesReceived` | `POST /search` calls accepted into buffers             |
| `batchesFlushed`   | Postgres batch transactions written                    |
| `rowsUpserted`     | unique `(query,count)` rows upserted across batches    |
| `cacheHits/Misses` | `/suggest` served from Redis vs Postgres fallback      |
| `cacheHitRate`     | `hits / (hits + misses)`                               |
| `writeReduction`   | `searchesReceived / batchesFlushed` (the batching win) |

---

## Internal (not proxied by the LB)

| Method | Path               | Notes                                                          |
| ------ | ------------------ | -------------------------------------------------------------- |
| POST   | `/internal/cache`  | cache-updater pushes refreshed top-K to an app node's own Redis|
| GET    | `/health`          | per-node Redis + Postgres reachability (used by compose)       |

These are reachable only on the app-node mesh, not through the public LB.
