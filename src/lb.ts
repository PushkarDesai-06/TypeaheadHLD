/**
 * Load Balancer / Reverse Proxy (Phase 4, Step 7)
 *
 * The single public entrypoint. It serves the frontend and proxies API calls
 * to the correct App Node using the EXACT same consistent-hash ring the seeder
 * used — so a request for prefix "goo" is always routed to the node whose shard
 * actually holds `q:goo`. Every routing decision is logged for visibility.
 *
 *   GET  /              -> static frontend
 *   GET  /suggest?q=p   -> hash(p)    -> proxy to that App Node
 *   POST /search {q}    -> hash(q)    -> proxy to that App Node
 *   GET  /trending      -> fan out to ALL App Nodes, merge, return global top-N
 *
 * `/trending` is a fan-out+merge because the trending ZSET is sharded: each
 * query lives on exactly one shard, so merging per-shard top-N and re-sorting
 * yields the correct global ranking.
 */

import { route } from "./hash-ring";
import {
  SHARDS,
  TRENDING_LIMIT,
  appUrlFor,
  normalize,
  type ShardId,
} from "./config";

const PORT = Number(process.env.LB_PORT ?? process.env.PORT ?? 8080);

function logRoute(method: string, path: string, key: string, shard: ShardId) {
  console.log(`[LB] ${method} ${path} key="${key}" -> app${shard}`);
}

/** Forward a request to the App Node that owns `shard`, preserving the path. */
async function proxy(shard: ShardId, path: string, init?: RequestInit): Promise<Response> {
  try {
    return await fetch(appUrlFor(shard) + path, init);
  } catch (err) {
    console.error(`[LB] upstream app${shard} unreachable:`, (err as Error).message);
    return Response.json({ error: `app node ${shard} unavailable` }, { status: 502 });
  }
}

/** Fan out GET /trending to every App Node and merge into a global ranking. */
async function mergedTrending(limit: number): Promise<Response> {
  const results = await Promise.allSettled(
    SHARDS.map((s) =>
      fetch(`${appUrlFor(s)}/trending?limit=${limit}`).then(
        (r) => r.json() as Promise<{ trending: { query: string; score: number }[] }>,
      ),
    ),
  );

  // Each query lives on a single shard, so a flat sort is the correct merge.
  const merged: { query: string; score: number }[] = [];
  for (const r of results) {
    if (r.status === "fulfilled") merged.push(...(r.value.trending ?? []));
  }
  merged.sort((a, b) => b.score - a.score);

  return Response.json({ trending: merged.slice(0, limit) });
}

/** Fan out GET /metrics to every App Node and sum the counters into a cluster view. */
async function aggregatedMetrics(): Promise<Response> {
  const results = await Promise.allSettled(
    SHARDS.map((s) =>
      fetch(`${appUrlFor(s)}/metrics`).then((r) => r.json() as Promise<Record<string, number>>),
    ),
  );

  const nodes: Record<string, number>[] = [];
  const total = {
    searchesReceived: 0,
    batchesFlushed: 0,
    rowsUpserted: 0,
    cacheHits: 0,
    cacheMisses: 0,
    buffered: 0,
  };
  for (const r of results) {
    if (r.status !== "fulfilled") continue;
    const m = r.value;
    nodes.push(m);
    for (const k of Object.keys(total) as (keyof typeof total)[]) total[k] += m[k] ?? 0;
  }

  const suggestTotal = total.cacheHits + total.cacheMisses;
  return Response.json({
    total: {
      ...total,
      cacheHitRate: suggestTotal > 0 ? total.cacheHits / suggestTotal : null,
      writeReduction:
        total.batchesFlushed > 0 ? total.searchesReceived / total.batchesFlushed : null,
    },
    nodes,
  });
}

const server = Bun.serve({
  port: PORT,
  async fetch(req) {
    const url = new URL(req.url);
    const { pathname } = url;

    // ---- API: GET /suggest?q=<prefix> ------------------------------------
    if (req.method === "GET" && pathname === "/suggest") {
      const key = normalize(url.searchParams.get("q") ?? "");
      if (!key) return Response.json({ prefix: "", suggestions: [] });
      const shard = route(key);
      logRoute("GET", pathname, key, shard);
      return proxy(shard, `/suggest${url.search}`);
    }

    // ---- API: POST /search {query} ---------------------------------------
    if (req.method === "POST" && pathname === "/search") {
      const body = await req.text();
      let key = "";
      try {
        key = normalize((JSON.parse(body) as { query?: string }).query ?? "");
      } catch {
        return Response.json({ error: "invalid json body" }, { status: 400 });
      }
      if (!key) return Response.json({ error: "missing query" }, { status: 400 });
      const shard = route(key);
      logRoute("POST", pathname, key, shard);
      return proxy(shard, "/search", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
      });
    }

    // ---- API: GET /trending (fan-out + merge) ----------------------------
    if (req.method === "GET" && pathname === "/trending") {
      const raw = Number(url.searchParams.get("limit"));
      const limit = Number.isInteger(raw) && raw > 0 ? raw : TRENDING_LIMIT;
      return mergedTrending(limit);
    }

    // ---- API: GET /metrics (fan-out + sum) -------------------------------
    if (req.method === "GET" && pathname === "/metrics") {
      return aggregatedMetrics();
    }

    // ---- API: GET /cache/debug?prefix=<p> --------------------------------
    // Same consistent-hash routing as /suggest, so it reports the node that
    // actually owns the prefix's cache (and whether it's a hit/miss there).
    if (req.method === "GET" && pathname === "/cache/debug") {
      const key = normalize(url.searchParams.get("prefix") ?? "");
      if (!key) return Response.json({ error: "missing prefix" }, { status: 400 });
      const shard = route(key);
      logRoute("GET", pathname, key, shard);
      return proxy(shard, `/cache/debug${url.search}`);
    }

    // ---- Static frontend -------------------------------------------------
    if (req.method === "GET" && (pathname === "/" || pathname === "/index.html")) {
      return new Response(Bun.file("public/index.html"));
    }
    if (pathname === "/favicon.ico") return new Response(null, { status: 204 });
    if (req.method === "GET" && pathname === "/script.js") {
      return new Response(Bun.file("public/script.js"));
    }

    return new Response("Not Found", { status: 404 });
  },
});

console.log(`🌐 Load Balancer on :${server.port}`);
for (const s of SHARDS) console.log(`     app${s} -> ${appUrlFor(s)}`);
