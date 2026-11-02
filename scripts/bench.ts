/**
 * Benchmark harness (Assignment §10 Non-Functional).
 *
 * Drives a RUNNING cluster through the Load Balancer and reports:
 *   • /suggest latency      : p50 / p90 / p95 / p99 / max over many requests
 *   • cache hit rate        : per traffic category, on a mix of seeded prefixes,
 *                             typos and novel strings (not just seeded prefixes,
 *                             which are 100% hits by construction)
 *
 * Write reduction from batching has its own fixed-rate sweep in bench-writes.ts.
 *
 * It only uses the public HTTP API, so the numbers reflect the real proxy +
 * shard + Redis path. Pure measurement — no internal hooks.
 *
 * Run (cluster must be up, e.g. `docker compose up`):
 *   bun run scripts/bench.ts
 *   BASE_URL=http://localhost:8080 BENCH_SUGGEST=5000 BENCH_RANK=recency bun run scripts/bench.ts
 *   BENCH_MIX=70,20,10 bun run scripts/bench.ts   # % seeded, typo, novel
 */

import { normalize } from "../src/config";
import {
  CATEGORIES,
  type Category,
  type Workload,
  loadWorkload,
  mixLabel,
  novelPrefix,
  parseMix,
  percentile,
  pickCategory,
  prefixFor,
} from "./lib/workload";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const N_SUGGEST = Number(process.env.BENCH_SUGGEST ?? 3000);
const CONCURRENCY = Number(process.env.BENCH_CONCURRENCY ?? 32);
const RANK = process.env.BENCH_RANK ?? "basic"; // basic | recency
const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";

const MIX_WEIGHTS = parseMix(process.env.BENCH_MIX);

/** Run `total` async tasks with at most `concurrency` in flight. */
async function pooled<T>(total: number, concurrency: number, task: (i: number) => Promise<T>) {
  let next = 0;
  const workers = Array.from({ length: concurrency }, async () => {
    while (true) {
      const i = next++;
      if (i >= total) return;
      await task(i);
    }
  });
  await Promise.all(workers);
}

const ms = (n: number) => `${n.toFixed(2)}ms`;

async function getMetrics(): Promise<any> {
  const r = await fetch(`${BASE}/metrics`);
  return r.json();
}

interface CategoryStats {
  latencies: number[];
  hits: number;
  misses: number;
  empty: number;
  errors: number;
  realPrefix: number;
}

const newStats = (): CategoryStats => ({ latencies: [], hits: 0, misses: 0, empty: 0, errors: 0, realPrefix: 0 });
const pct = (part: number, whole: number) => (whole > 0 ? `${((part / whole) * 100).toFixed(1)}%` : "n/a");

function mergeStats(all: CategoryStats[]): CategoryStats {
  const merged = newStats();
  for (const s of all) {
    merged.latencies.push(...s.latencies);
    merged.hits += s.hits;
    merged.misses += s.misses;
    merged.empty += s.empty;
    merged.errors += s.errors;
    merged.realPrefix += s.realPrefix;
  }
  return merged;
}

function printCategoryRow(label: string, s: CategoryStats) {
  s.latencies.sort((a, b) => a - b);
  const answered = s.hits + s.misses;
  console.log(
    `  ${label.padEnd(8)} ${String(answered).padStart(6)}  ${pct(s.hits, answered).padStart(6)}  ` +
      `${pct(s.empty, answered).padStart(6)}  ${ms(percentile(s.latencies, 50)).padStart(8)}  ` +
      `${ms(percentile(s.latencies, 95)).padStart(8)}  ${ms(percentile(s.latencies, 99)).padStart(8)}  ${String(s.errors).padStart(6)}`,
  );
}

async function benchSuggest(w: Workload) {
  console.log(`\n▶ /suggest mixed workload  (rank=${RANK}, n=${N_SUGGEST}, concurrency=${CONCURRENCY}, mix ${mixLabel(MIX_WEIGHTS)})`);

  // Warm-up so JIT + connections are hot and we measure steady state.
  await pooled(Math.min(200, N_SUGGEST), CONCURRENCY, async (i) => {
    await fetch(`${BASE}/suggest?q=${encodeURIComponent(w.seeded[i % w.seeded.length]!)}&rank=${RANK}`);
  });

  const stats = Object.fromEntries(CATEGORIES.map((c) => [c, newStats()])) as Record<Category, CategoryStats>;

  const t0 = performance.now();
  await pooled(N_SUGGEST, CONCURRENCY, async () => {
    const category = pickCategory(MIX_WEIGHTS);
    const prefix = prefixFor(category, w);
    const s = stats[category];
    if (w.knownPrefixes.has(normalize(prefix))) s.realPrefix++;
    const start = performance.now();
    try {
      const res = await fetch(`${BASE}/suggest?q=${encodeURIComponent(prefix)}&rank=${RANK}`);
      const body = (await res.json()) as { source?: string; suggestions?: string[] };
      const elapsed = performance.now() - start;
      if (!res.ok || (body.source !== "cache" && body.source !== "db")) {
        s.errors++;
        return;
      }
      s.latencies.push(elapsed);
      if (body.source === "cache") s.hits++;
      else s.misses++;
      if ((body.suggestions?.length ?? 0) === 0) s.empty++;
    } catch {
      s.errors++;
    }
  });
  const wall = performance.now() - t0;

  const overall = mergeStats(CATEGORIES.map((c) => stats[c]));
  console.log(`  ${"category".padEnd(8)} ${"reqs".padStart(6)}  ${"hit".padStart(6)}  ${"empty".padStart(6)}  ${"p50".padStart(8)}  ${"p95".padStart(8)}  ${"p99".padStart(8)}  ${"errors".padStart(6)}`);
  for (const c of CATEGORIES) printCategoryRow(c, stats[c]);
  printCategoryRow("overall", overall);

  const typo = stats.typo;
  const typoTotal = typo.hits + typo.misses + typo.errors;
  console.log(`  typos that were still a real dataset prefix: ${typo.realPrefix} / ${typoTotal} (${pct(typo.realPrefix, typoTotal)})`);

  const sum = overall.latencies.reduce((a, b) => a + b, 0);
  console.log(`  throughput : ${(overall.latencies.length / (wall / 1000)).toFixed(0)} req/s (wall ${ms(wall)})`);
  console.log(`  mean       : ${ms(sum / overall.latencies.length)}`);
  console.log(`  p90        : ${ms(percentile(overall.latencies, 90))}`);
  console.log(`  max        : ${ms(overall.latencies.at(-1) ?? 0)}`);
}

/**
 * Asks for the same unseen prefix twice in a row. A miss with no results is
 * never marked dirty, so if the second request also says `db`, every keystroke
 * on an unknown prefix costs a Postgres query (no negative caching).
 */
async function benchRepeatMiss(w: Workload) {
  console.log(`\n▶ repeat-miss check  (same novel prefix requested twice)`);
  let stillMiss = 0;
  const samples = 5;
  for (let i = 0; i < samples; i++) {
    const prefix = novelPrefix(w);
    const sources: string[] = [];
    for (let attempt = 0; attempt < 2; attempt++) {
      const body = (await (await fetch(`${BASE}/suggest?q=${prefix}&rank=${RANK}`)).json()) as { source: string };
      sources.push(body.source);
    }
    if (sources[1] === "db") stillMiss++;
    console.log(`  ${prefix.padEnd(10)} ${sources.join(" -> ")}`);
  }
  console.log(`  second request still went to Postgres: ${stillMiss} / ${samples}`);
}

async function main() {
  console.log(`# Typeahead benchmark against ${BASE}`);
  try {
    await fetch(`${BASE}/metrics`);
  } catch {
    console.error(`Cannot reach ${BASE}. Is the cluster up? (docker compose up)`);
    process.exit(1);
  }

  const workload = await loadWorkload(DATA_PATH);
  console.log(`Seeded prefix pool: ${workload.seeded.length} distinct prefixes (${workload.knownPrefixes.size} cached prefixes in total)`);

  await benchSuggest(workload);
  await benchRepeatMiss(workload);

  const m = await getMetrics();
  console.log(`\n▶ cluster /metrics (cumulative)`);
  console.log(`  cache hit rate  : ${m.total.cacheHitRate != null ? (m.total.cacheHitRate * 100).toFixed(1) + "%" : "n/a"}`);
  console.log(`  hits=${m.total.cacheHits} misses=${m.total.cacheMisses}`);
}

main();
