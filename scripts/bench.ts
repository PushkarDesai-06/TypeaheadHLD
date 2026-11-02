/**
 * Benchmark harness (Assignment §10 Non-Functional + §8 batch-write evidence).
 *
 * Drives a RUNNING cluster through the Load Balancer and reports:
 *   • /suggest latency      : p50 / p90 / p95 / p99 / max over many requests
 *   • cache hit rate        : per traffic category, on a mix of seeded prefixes,
 *                             typos and novel strings (not just seeded prefixes,
 *                             which are 100% hits by construction)
 *   • write reduction       : searches accepted per Postgres batch (from /metrics)
 *
 * It only uses the public HTTP API, so the numbers reflect the real proxy +
 * shard + Redis path. Pure measurement — no internal hooks.
 *
 * Run (cluster must be up, e.g. `docker compose up`):
 *   bun run scripts/bench.ts
 *   BASE_URL=http://localhost:8080 BENCH_SUGGEST=5000 BENCH_RANK=recency bun run scripts/bench.ts
 *   BENCH_MIX=70,20,10 bun run scripts/bench.ts   # % seeded, typo, novel
 */

import { normalize, prefixesOf } from "../src/config";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const N_SUGGEST = Number(process.env.BENCH_SUGGEST ?? 3000);
const N_SEARCH = Number(process.env.BENCH_SEARCH ?? 1000);
const CONCURRENCY = Number(process.env.BENCH_CONCURRENCY ?? 32);
const RANK = process.env.BENCH_RANK ?? "basic"; // basic | recency
const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";

const CATEGORIES = ["seeded", "typo", "novel"] as const;
type Category = (typeof CATEGORIES)[number];

const MIX_WEIGHTS = (process.env.BENCH_MIX ?? "70,20,10").split(",").map(Number);
if (MIX_WEIGHTS.length !== 3 || MIX_WEIGHTS.some((w) => !(w >= 0))) {
  throw new Error(`BENCH_MIX must be three non-negative numbers (seeded,typo,novel), got "${process.env.BENCH_MIX}"`);
}

interface Entry {
  query: string;
  count: number;
}

interface Workload {
  /** 2–4 char prefixes of dataset queries: every one is pre-cached by the seeder. */
  seeded: string[];
  /** Normalised dataset queries, used as the source text for typos. */
  queries: string[];
  /** Every prefix the seeder caches, to tell a typo that is still a real prefix from one that is not. */
  knownPrefixes: Set<string>;
}

async function loadWorkload(): Promise<Workload> {
  const entries: Entry[] = JSON.parse(await Bun.file(DATA_PATH).text());
  const queries = entries.map((e) => normalize(e.query)).filter((q) => /[a-z0-9]/i.test(q));

  const knownPrefixes = new Set<string>();
  for (const q of queries) for (const p of prefixesOf(q)) knownPrefixes.add(p);

  const seeded = new Set<string>();
  for (const q of queries) {
    if (q.length < 2) continue;
    const len = 2 + (q.length % 3); // 2..4
    seeded.add(q.slice(0, Math.min(len, q.length)));
    if (seeded.size >= 20000) break;
  }
  return { seeded: [...seeded], queries: queries.filter((q) => /^[a-z]{4,}/.test(q)), knownPrefixes };
}

const LETTERS = "abcdefghijklmnopqrstuvwxyz";
const randInt = (n: number) => Math.floor(Math.random() * n);
const randLetter = () => LETTERS[randInt(LETTERS.length)]!;
const pick = <T>(xs: readonly T[]) => xs[randInt(xs.length)]!;

/**
 * A 3–8 char prefix of a real query with one edit (substitute, delete, insert,
 * or swap two adjacent letters) after the first character, the way people
 * mistype while autocomplete is open.
 */
function typoPrefix(w: Workload): string {
  while (true) {
    const q = pick(w.queries);
    const len = 3 + randInt(Math.min(6, q.length - 2));
    const chars = q.slice(0, len).split("");
    const pos = 1 + randInt(chars.length - 1);
    switch (randInt(4)) {
      case 0: chars[pos] = randLetter(); break;
      case 1: chars.splice(pos, 1); break;
      case 2: chars.splice(pos, 0, randLetter()); break;
      case 3:
        if (pos === chars.length - 1) continue;
        [chars[pos], chars[pos + 1]] = [chars[pos + 1]!, chars[pos]!];
        break;
    }
    const typo = chars.join("");
    if (typo !== q.slice(0, len) && normalize(typo)) return typo;
  }
}

/** A random 4–8 letter string that is not a prefix of any dataset query. */
function novelPrefix(w: Workload): string {
  while (true) {
    const s = Array.from({ length: 4 + randInt(5) }, randLetter).join("");
    if (!w.knownPrefixes.has(s)) return s;
  }
}

function pickCategory(): Category {
  const total = MIX_WEIGHTS.reduce((a, b) => a + b, 0);
  let r = Math.random() * total;
  for (let i = 0; i < CATEGORIES.length; i++) {
    r -= MIX_WEIGHTS[i]!;
    if (r < 0) return CATEGORIES[i]!;
  }
  return "seeded";
}

function prefixFor(category: Category, w: Workload): string {
  if (category === "typo") return typoPrefix(w);
  if (category === "novel") return novelPrefix(w);
  return pick(w.seeded);
}

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

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)]!;
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
  const mixLabel = CATEGORIES.map((c, i) => `${c} ${MIX_WEIGHTS[i]}`).join(" / ");
  console.log(`\n▶ /suggest mixed workload  (rank=${RANK}, n=${N_SUGGEST}, concurrency=${CONCURRENCY}, mix ${mixLabel})`);

  // Warm-up so JIT + connections are hot and we measure steady state.
  await pooled(Math.min(200, N_SUGGEST), CONCURRENCY, async (i) => {
    await fetch(`${BASE}/suggest?q=${encodeURIComponent(w.seeded[i % w.seeded.length]!)}&rank=${RANK}`);
  });

  const stats = Object.fromEntries(CATEGORIES.map((c) => [c, newStats()])) as Record<Category, CategoryStats>;

  const t0 = performance.now();
  await pooled(N_SUGGEST, CONCURRENCY, async () => {
    const category = pickCategory();
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

async function benchWriteReduction(pool: readonly string[]) {
  console.log(`\n▶ write reduction  (n=${N_SEARCH} searches)`);
  const before = await getMetrics();

  // Submit N searches; reuse a small set of queries so dedup also shows in logs.
  const queries = pool.slice(0, 50).map((p) => `${p} demo`);
  await pooled(N_SEARCH, CONCURRENCY, async (i) => {
    const q = queries[i % queries.length]!;
    await fetch(`${BASE}/search`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: q }),
    });
  });

  // Let the buffers flush (poll until drained, bounded wait).
  for (let i = 0; i < 40; i++) {
    const m = await getMetrics();
    if ((m.total?.buffered ?? 0) === 0) break;
    await Bun.sleep(250);
  }
  const after = await getMetrics();

  const dSearches = (after.total.searchesReceived ?? 0) - (before.total.searchesReceived ?? 0);
  const dBatches = (after.total.batchesFlushed ?? 0) - (before.total.batchesFlushed ?? 0);
  const dRows = (after.total.rowsUpserted ?? 0) - (before.total.rowsUpserted ?? 0);
  console.log(`  searches submitted     : ${dSearches}`);
  console.log(`  Postgres batch txns    : ${dBatches}`);
  console.log(`  unique rows upserted   : ${dRows}`);
  console.log(
    `  write reduction        : ${dBatches > 0 ? (dSearches / dBatches).toFixed(1) : "n/a"}× ` +
      `(searches per DB transaction)`,
  );
}

async function main() {
  console.log(`# Typeahead benchmark against ${BASE}`);
  try {
    await fetch(`${BASE}/metrics`);
  } catch {
    console.error(`Cannot reach ${BASE}. Is the cluster up? (docker compose up)`);
    process.exit(1);
  }

  const workload = await loadWorkload();
  console.log(`Seeded prefix pool: ${workload.seeded.length} distinct prefixes (${workload.knownPrefixes.size} cached prefixes in total)`);

  await benchSuggest(workload);
  await benchRepeatMiss(workload);
  await benchWriteReduction(workload.seeded);

  const m = await getMetrics();
  console.log(`\n▶ cluster /metrics (cumulative)`);
  console.log(`  cache hit rate  : ${m.total.cacheHitRate != null ? (m.total.cacheHitRate * 100).toFixed(1) + "%" : "n/a"}`);
  console.log(`  write reduction : ${m.total.writeReduction != null ? m.total.writeReduction.toFixed(1) + "×" : "n/a"}`);
  console.log(`  searches=${m.total.searchesReceived} batches=${m.total.batchesFlushed} rows=${m.total.rowsUpserted}`);
  console.log(`  hits=${m.total.cacheHits} misses=${m.total.cacheMisses}`);
}

main();
