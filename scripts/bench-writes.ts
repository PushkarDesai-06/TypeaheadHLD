/**
 * Write-reduction sweep (Assignment §8): how many POST /search calls share one
 * Postgres transaction, measured at several fixed traffic rates.
 *
 * Batching has two triggers per app node: a full buffer (BATCH_SIZE) and a
 * timer that fires every FLUSH_INTERVAL_MS. Which one fires depends on how fast
 * searches arrive, so a single number is meaningless without the rate it was
 * taken at. This sends searches on a fixed schedule (open loop, never waits for
 * replies) for each rate, then reads the app nodes' own counters through the
 * LB's /metrics:
 *
 *   write reduction = searches accepted / batch transactions committed
 *   row reduction   = searches accepted / unique rows upserted
 *
 * Each measurement is checked against a simulation of the flush logic in
 * src/server.ts: searches arrive on the same fixed schedule and land on a node
 * with that node's measured share, a full buffer flushes at BATCH_SIZE, and a
 * timer with a random phase flushes whatever is buffered every
 * FLUSH_INTERVAL_MS (a full-buffer flush does not reset it). The simulation
 * assumes every transaction commits instantly, so the gap between it and the
 * measurement is what the database's real behaviour costs.
 *
 * Queries are drawn from the dataset weighted by their real frequency, so
 * popular queries repeat inside a batch the way they would in production.
 * Every query already exists in the dataset, so no new prefixes are created;
 * the run only adds to existing counts.
 *
 * The cluster must have no other traffic while this runs, because the counters
 * are cluster-wide. With BENCH_PG_STATS=1 it also reads Postgres's own deadlock
 * counter (via `docker compose exec postgres psql`), since a flush that
 * deadlocks is rolled back and never shows up in batchesFlushed.
 *
 * If the buffers still hold searches BENCH_DRAIN_TIMEOUT_S after a step's load
 * stops, the write path is not keeping up: the step is recorded as BACKLOG and
 * the sweep stops there.
 *
 *   bun run bench:writes
 *   BENCH_WRITE_RATES=1,5,20,60,300,1500 BENCH_DURATION_S=60 BENCH_REPEATS=3 bun run bench:writes
 */

import { $ } from "bun";
import { BATCH_SIZE, FLUSH_INTERVAL_MS, normalize } from "../src/config";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const RATES = (process.env.BENCH_WRITE_RATES ?? "1,5,20,60,300,1500").split(",").map(Number);
const DURATION_S = Number(process.env.BENCH_DURATION_S ?? 60);
const REPEATS = Number(process.env.BENCH_REPEATS ?? 3);
const DRAIN_TIMEOUT_MS = Number(process.env.BENCH_DRAIN_TIMEOUT_S ?? 60) * 1000;
const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";
const OUT_DIR = process.env.BENCH_OUT ?? "docs/benchmarks";
const LABEL = process.env.BENCH_LABEL ?? "write-reduction";
const NOTE = process.env.BENCH_NOTE ?? "";
const PG_STATS = process.env.BENCH_PG_STATS === "1";
const REQUEST_TIMEOUT_MS = 5000;
const FLUSH_INTERVAL_S = FLUSH_INTERVAL_MS / 1000;

interface NodeMetrics {
  shard: number;
  buffered: number;
  searchesReceived: number;
  batchesFlushed: number;
  rowsUpserted: number;
}

interface ClusterMetrics {
  total: NodeMetrics;
  nodes: NodeMetrics[];
}

interface NodeDelta {
  shard: number;
  searches: number;
  batches: number;
  rows: number;
  ratePerS: number;
  modelBatches: number;
}

interface Drain {
  drained: boolean;
  ms: number;
  stillBuffered: number;
}

interface StepResult {
  rate: number;
  repeat: number;
  verdict: "ok" | "BACKLOG";
  sent: number;
  accepted: number;
  failed: number;
  failureKinds: Record<string, number>;
  searches: number;
  batches: number;
  rows: number;
  writeReduction: number;
  rowReduction: number;
  modelWriteReduction: number;
  drain: Drain;
  deadlocks: number | null;
  nodes: NodeDelta[];
}

async function getDeadlocks(): Promise<number | null> {
  if (!PG_STATS) return null;
  const sql = "select deadlocks from pg_stat_database where datname = current_database()";
  return Number((await $`docker compose exec -T postgres psql -U typeahead -d typeahead -Atc ${sql}`.text()).trim());
}

async function getMetrics(): Promise<ClusterMetrics> {
  return (await (await fetch(`${BASE}/metrics`)).json()) as ClusterMetrics;
}

async function waitForEmptyBuffers(): Promise<Drain> {
  const started = performance.now();
  let stillBuffered = 0;
  while (performance.now() - started < DRAIN_TIMEOUT_MS) {
    stillBuffered = (await getMetrics()).total.buffered;
    if (stillBuffered === 0) return { drained: true, ms: performance.now() - started, stillBuffered };
    await Bun.sleep(100);
  }
  return { drained: false, ms: performance.now() - started, stillBuffered };
}

/** Frequency-weighted sampler over the dataset's queries. */
async function loadQuerySampler(): Promise<() => string> {
  const entries: { query: string; count: number }[] = JSON.parse(await Bun.file(DATA_PATH).text());
  const queries: string[] = [];
  const cumulative: number[] = [];
  let total = 0;
  for (const { query, count } of entries) {
    const q = normalize(query);
    if (!q || !(count > 0)) continue;
    total += count;
    queries.push(q);
    cumulative.push(total);
  }
  return () => {
    const target = Math.random() * total;
    let lo = 0;
    let hi = cumulative.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (cumulative[mid]! > target) hi = mid;
      else lo = mid + 1;
    }
    return queries[lo]!;
  };
}

const SIMULATION_RUNS = 200;

/** Mean number of flushes one node makes in the simulated flush logic. */
function simulateFlushes(rate: number, durationS: number, share: number): number {
  const arrivals = Math.round(rate * durationS);
  let totalFlushes = 0;
  for (let run = 0; run < SIMULATION_RUNS; run++) {
    let nextTimerS = Math.random() * FLUSH_INTERVAL_S;
    let buffered = 0;
    let flushes = 0;
    for (let i = 0; i < arrivals; i++) {
      const arrivalS = i / rate;
      for (; nextTimerS <= arrivalS; nextTimerS += FLUSH_INTERVAL_S) {
        if (buffered > 0) flushes++;
        buffered = 0;
      }
      if (Math.random() >= share) continue;
      if (++buffered >= BATCH_SIZE) {
        flushes++;
        buffered = 0;
      }
    }
    if (buffered > 0) flushes++;
    totalFlushes += flushes;
  }
  return totalFlushes / SIMULATION_RUNS;
}

async function runStep(rate: number, repeat: number, sample: () => string): Promise<StepResult> {
  const before = await getMetrics();
  const deadlocksBefore = await getDeadlocks();

  const total = Math.round(rate * DURATION_S);
  const intervalMs = 1000 / rate;
  const failureKinds: Record<string, number> = {};
  let accepted = 0;
  let failed = 0;
  const inFlight = new Set<Promise<void>>();

  const send = () => {
    const request = fetch(`${BASE}/search`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query: sample() }),
      signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
    })
      .then(async (res) => {
        await res.arrayBuffer();
        if (res.status === 202) accepted++;
        else {
          failed++;
          failureKinds[`HTTP ${res.status}`] = (failureKinds[`HTTP ${res.status}`] ?? 0) + 1;
        }
      })
      .catch((err: Error) => {
        failed++;
        failureKinds[err.name] = (failureKinds[err.name] ?? 0) + 1;
      })
      .finally(() => inFlight.delete(request));
    inFlight.add(request);
  };

  const start = performance.now();
  let sent = 0;
  while (sent < total) {
    const due = Math.min(total, Math.floor((performance.now() - start) / intervalMs) + 1);
    while (sent < due) {
      send();
      sent++;
    }
    await Bun.sleep(Math.min(intervalMs, 5));
  }
  const elapsedS = (performance.now() - start) / 1000;
  await Promise.all(inFlight);

  const drain = await waitForEmptyBuffers();
  const after = await getMetrics();
  const deadlocksAfter = await getDeadlocks();

  const searches = after.total.searchesReceived - before.total.searchesReceived;

  const nodes: NodeDelta[] = after.nodes
    .map((a) => {
      const b = before.nodes.find((n) => n.shard === a.shard)!;
      const nodeSearches = a.searchesReceived - b.searchesReceived;
      return {
        shard: a.shard,
        searches: nodeSearches,
        batches: a.batchesFlushed - b.batchesFlushed,
        rows: a.rowsUpserted - b.rowsUpserted,
        ratePerS: nodeSearches / elapsedS,
        modelBatches: simulateFlushes(rate, DURATION_S, nodeSearches / searches),
      };
    })
    .sort((x, y) => x.shard - y.shard);

  const batches = after.total.batchesFlushed - before.total.batchesFlushed;
  const rows = after.total.rowsUpserted - before.total.rowsUpserted;
  const modelBatches = nodes.reduce((sum, n) => sum + n.modelBatches, 0);

  return {
    rate,
    repeat,
    verdict: drain.drained ? "ok" : "BACKLOG",
    sent,
    accepted,
    failed,
    failureKinds,
    searches,
    batches,
    rows,
    writeReduction: batches > 0 ? searches / batches : NaN,
    rowReduction: rows > 0 ? searches / rows : NaN,
    modelWriteReduction: modelBatches > 0 ? searches / modelBatches : NaN,
    drain,
    deadlocks: deadlocksBefore !== null && deadlocksAfter !== null ? deadlocksAfter - deadlocksBefore : null,
    nodes,
  };
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const mid = s.length >> 1;
  return s.length % 2 ? s[mid]! : (s[mid - 1]! + s[mid]!) / 2;
};
const fmt = (x: number, digits = 1) => (Number.isFinite(x) ? x.toFixed(digits) : "n/a");
const range = (xs: number[]) => `${fmt(Math.min(...xs))}–${fmt(Math.max(...xs))}`;

function describe(r: StepResult): string {
  const perNode = r.nodes.map((n) => `app${n.shard} ${fmt(n.ratePerS)}/s ${n.searches}/${n.batches}`).join(", ");
  const drain = r.drain.drained
    ? `drain ${Math.round(r.drain.ms)}ms`
    : `${r.drain.stillBuffered} searches still buffered after ${Math.round(r.drain.ms / 1000)}s`;
  return (
    `rate ${String(r.rate).padStart(5)}/s #${r.repeat}: ${r.searches} searches -> ${r.batches} txns ` +
    `(${fmt(r.writeReduction)}x, model ${fmt(r.modelWriteReduction)}x), ${r.rows} rows ` +
    `(${fmt(r.rowReduction)}x), failed ${r.failed}, ${drain}` +
    (r.deadlocks !== null ? `, pg deadlocks ${r.deadlocks}` : "") +
    ` | ${perNode}` +
    (r.verdict === "BACKLOG" ? "  BACKLOG" : "")
  );
}

async function main() {
  try {
    await getMetrics();
  } catch {
    console.error(`Cannot reach ${BASE}/metrics. Is the cluster up?`);
    process.exit(1);
  }
  const sample = await loadQuerySampler();

  console.log(`# Write-reduction sweep against ${BASE}`);
  console.log(`date: ${new Date().toISOString()}`);
  console.log(
    `rates (searches/s, whole cluster): ${RATES.join(", ")}; ${DURATION_S}s per step; ${REPEATS} repeats`,
  );
  console.log(`client-side model constants: BATCH_SIZE=${BATCH_SIZE}, FLUSH_INTERVAL_MS=${FLUSH_INTERVAL_MS}`);
  if (NOTE) console.log(`note: ${NOTE}`);

  const results: StepResult[] = [];
  sweep: for (const rate of RATES) {
    for (let repeat = 1; repeat <= REPEATS; repeat++) {
      const idle = await waitForEmptyBuffers();
      if (!idle.drained) {
        console.log(`buffers not empty before rate ${rate}/s (${idle.stillBuffered} searches); stopping`);
        break sweep;
      }
      const r = await runStep(rate, repeat, sample);
      results.push(r);
      console.log(describe(r));
      if (r.drain.drained && r.accepted !== r.searches) {
        console.log(`  WARNING: client saw ${r.accepted} accepted but nodes counted ${r.searches}; other traffic?`);
      }
      if (r.verdict === "BACKLOG") {
        console.log(`  write path did not keep up at ${rate}/s; stopping the sweep`);
        break sweep;
      }
    }
  }

  console.log(`\n## Summary (median [range] over the repeats that ran)`);
  console.log(
    `| Rate (searches/s) | Searches per txn | Model | Searches per row written | Failed requests |` +
      (PG_STATS ? ` Deadlocks |` : "") +
      ` Verdict |`,
  );
  console.log(`| ---: | ---: | ---: | ---: | ---: |` + (PG_STATS ? ` ---: |` : "") + ` --- |`);
  for (const rate of RATES) {
    const steps = results.filter((r) => r.rate === rate);
    if (steps.length === 0) continue;
    const wr = steps.map((s) => s.writeReduction);
    const rr = steps.map((s) => s.rowReduction);
    console.log(
      `| ${rate} | ${fmt(median(wr))}x [${range(wr)}] | ${fmt(median(steps.map((s) => s.modelWriteReduction)))}x ` +
        `| ${fmt(median(rr))}x [${range(rr)}] | ${steps.reduce((sum, s) => sum + s.failed, 0)} |` +
        (PG_STATS ? ` ${steps.reduce((sum, s) => sum + (s.deadlocks ?? 0), 0)} |` : "") +
        ` ${steps.some((s) => s.verdict === "BACKLOG") ? "BACKLOG" : "ok"} |`,
    );
  }

  const outPath = `${OUT_DIR}/${LABEL}.json`;
  await Bun.write(
    outPath,
    JSON.stringify(
      {
        date: new Date().toISOString(),
        baseUrl: BASE,
        durationS: DURATION_S,
        repeats: REPEATS,
        drainTimeoutMs: DRAIN_TIMEOUT_MS,
        batchSize: BATCH_SIZE,
        flushIntervalMs: FLUSH_INTERVAL_MS,
        note: NOTE,
        results,
      },
      null,
      2,
    ),
  );
  console.log(`\nraw results: ${outPath}`);
}

main();
