/**
 * Fixed-rate (open-loop) latency sweep for GET /suggest.
 *
 * Unlike bench.ts, which waits for a response before sending the next request
 * (so a stalled server is sent less traffic and its stall barely shows up), this
 * sends requests on a fixed schedule no matter how the server is doing, the way
 * real users keep typing. Because sends never wait on responses, traffic keeps
 * arriving during a stall and the queueing it causes is counted in full.
 * Load comes from BENCH_WORKERS threads (scripts/lib/load-worker.ts), each with
 * its own GC heap, so one thread's pause cannot hold back the whole schedule.
 *
 * For each target rate it runs a warm-up (discarded), then a measured step,
 * repeated BENCH_REPEATS times, and reports p50 / p90 / p99 / p99.9 / max.
 * Failed or timed-out requests count as infinite latency, so they can never
 * hide inside a percentile.
 *
 * Validity checks per step:
 *   send lag   : how late the generator actually sent vs schedule. If p99 lag
 *                exceeds BENCH_MAX_SEND_LAG_MS the arrival schedule was not kept,
 *                so the client, not the server, was the bottleneck and the step is
 *                marked CLIENT-LIMITED (not a valid result).
 *   failures   : more than 0.1% errors/timeouts marks the step SATURATED, and the
 *                sweep stops there rather than overloading the cluster further.
 *
 * Run (cluster up):
 *   bun run bench:latency
 *   BENCH_RATES=1000,5000,10000 BENCH_DURATION_S=30 BENCH_REPEATS=3 bun run bench:latency
 *   BENCH_MIX=100,0,0 BENCH_RANK=recency bun run bench:latency
 */

import type { InitMessage, StepMessage, StepReply } from "./lib/load-worker";
import { CATEGORIES, loadWorkload, mixLabel, parseMix, percentile, pickCategory, prefixFor } from "./lib/workload";

const BASE = process.env.BASE_URL ?? "http://localhost:8080";
const RATES = (process.env.BENCH_RATES ?? "1000,5000,10000,20000,30000").split(",").map(Number);
const DURATION_S = Number(process.env.BENCH_DURATION_S ?? 30);
const WARMUP_S = Number(process.env.BENCH_WARMUP_S ?? 3);
const COOLDOWN_S = Number(process.env.BENCH_COOLDOWN_S ?? 3);
const REPEATS = Number(process.env.BENCH_REPEATS ?? 3);
const RANK = process.env.BENCH_RANK ?? "basic";
const MIX = parseMix(process.env.BENCH_MIX);
const TIMEOUT_MS = Number(process.env.BENCH_TIMEOUT_MS ?? 5000);
const MAX_SEND_LAG_MS = Number(process.env.BENCH_MAX_SEND_LAG_MS ?? 5);
const MAX_FAILURE_RATE = 0.001;
const SLO_P99_MS = Number(process.env.BENCH_SLO_P99_MS ?? 10);
const DOCKER_STATS = process.env.BENCH_DOCKER_STATS === "1";
/** CPUs the docker stats sampler runs on, kept away from the load generator's own CPUs. */
const SAMPLER_CPUS = process.env.BENCH_SAMPLER_CPUS ?? "";
const DATA_PATH = process.env.DATA_PATH ?? "data/search_frequencies.json";
const OUT_DIR = process.env.BENCH_OUT ?? "docs/benchmarks";
const LABEL = process.env.BENCH_LABEL ?? `${RANK}-mix-${MIX.join("-")}`;
const NOTE = process.env.BENCH_NOTE ?? "";
const REQUEST_POOL_SIZE = 200_000;
const WORKERS = Number(process.env.BENCH_WORKERS ?? 2);

interface StepResult {
  rate: number;
  repeat: number;
  scheduled: number;
  succeeded: number;
  errors: number;
  timeouts: number;
  hitRate: number;
  achievedSendRate: number;
  p50: number;
  p90: number;
  p95: number;
  p99: number;
  p999: number;
  max: number;
  sendLagP99: number;
  sendLagMax: number;
  maxInFlight: number;
  errorKinds: Record<string, number>;
  verdict: "ok" | "CLIENT-LIMITED" | "SATURATED";
  containerCpu?: Record<string, string>;
}

const fmtMs = (n: number) => (Number.isFinite(n) ? n.toFixed(2) : "inf");

async function sampleContainerCpu(): Promise<Record<string, string>> {
  const format = "{{.Name}} {{.CPUPerc}}";
  const command = SAMPLER_CPUS
    ? Bun.$`taskset -c ${SAMPLER_CPUS} docker stats --no-stream --format ${format}`
    : Bun.$`docker stats --no-stream --format ${format}`;
  const out = await command.quiet().nothrow().text();
  const cpu: Record<string, string> = {};
  for (const line of out.trim().split("\n")) {
    const [name, perc] = line.split(" ");
    if (name && perc) cpu[name.replace(/^typeahed-|-1$/g, "")] = perc;
  }
  return cpu;
}

function buildRequestPool(workload: Awaited<ReturnType<typeof loadWorkload>>): string[] {
  return Array.from({ length: REQUEST_POOL_SIZE }, () => {
    const prefix = prefixFor(pickCategory(MIX), workload);
    return `${BASE}/suggest?q=${encodeURIComponent(prefix)}&rank=${RANK}`;
  });
}

function concat(arrays: Float64Array[]): Float64Array {
  const out = new Float64Array(arrays.reduce((n, a) => n + a.length, 0));
  let offset = 0;
  for (const a of arrays) {
    out.set(a, offset);
    offset += a.length;
  }
  return out;
}

async function startWorkers(urls: string[]): Promise<Worker[]> {
  const workers = Array.from({ length: WORKERS }, () => new Worker(new URL("./lib/load-worker.ts", import.meta.url).href));
  await Promise.all(
    workers.map((w, i) => {
      const ready = new Promise((resolve) => w.addEventListener("message", resolve, { once: true }));
      const share = urls.filter((_, j) => j % WORKERS === i);
      w.postMessage({ type: "init", urls: share, timeoutMs: TIMEOUT_MS } satisfies InitMessage);
      return ready;
    }),
  );
  return workers;
}

function runOnWorker(worker: Worker, msg: StepMessage): Promise<StepReply> {
  return new Promise((resolve) => {
    worker.addEventListener("message", (e: MessageEvent<StepReply>) => resolve(e.data), { once: true });
    worker.postMessage(msg);
  });
}

async function runStep(workers: Worker[], rate: number, durationS: number, repeat: number, sampleCpu: boolean): Promise<StepResult> {
  const startAt = Date.now() + 50;
  const cpuSample = sampleCpu ? Bun.sleep(50 + (durationS * 1000) / 2).then(sampleContainerCpu) : undefined;
  const replies = await Promise.all(
    workers.map((w) => runOnWorker(w, { type: "step", rate: rate / workers.length, durationS, startAt })),
  );

  const latencies = concat(replies.map((r) => r.latencies)).sort();
  const sendLag = concat(replies.map((r) => r.sendLag)).sort();
  const total = latencies.length;
  const sum = (pick: (r: StepReply) => number) => replies.reduce((n, r) => n + pick(r), 0);
  const succeeded = sum((r) => r.succeeded);
  const errors = sum((r) => r.errors);
  const timeouts = sum((r) => r.timeouts);
  const errorKinds: Record<string, number> = {};
  for (const r of replies) for (const [k, v] of Object.entries(r.errorKinds)) errorKinds[k] = (errorKinds[k] ?? 0) + v;

  const sendLagP99 = percentile(sendLag, 99);
  const verdict: StepResult["verdict"] =
    sendLagP99 > MAX_SEND_LAG_MS ? "CLIENT-LIMITED" : (errors + timeouts) / total > MAX_FAILURE_RATE ? "SATURATED" : "ok";

  return {
    rate,
    repeat,
    scheduled: total,
    succeeded,
    errors,
    timeouts,
    hitRate: succeeded > 0 ? sum((r) => r.hits) / succeeded : 0,
    achievedSendRate: total / (Math.max(...replies.map((r) => r.sendWindowMs)) / 1000),
    p50: percentile(latencies, 50),
    p90: percentile(latencies, 90),
    p95: percentile(latencies, 95),
    p99: percentile(latencies, 99),
    p999: percentile(latencies, 99.9),
    max: latencies[total - 1]!,
    sendLagP99,
    sendLagMax: sendLag[total - 1]!,
    maxInFlight: sum((r) => r.maxInFlight),
    errorKinds,
    verdict,
    containerCpu: cpuSample ? await cpuSample : undefined,
  };
}

function printStep(r: StepResult) {
  console.log(
    `  ${String(r.rate).padStart(6)}  #${r.repeat}  sent ${r.achievedSendRate.toFixed(0).padStart(6)}/s  ` +
      `p50 ${fmtMs(r.p50).padStart(6)}  p90 ${fmtMs(r.p90).padStart(6)}  p95 ${fmtMs(r.p95).padStart(6)}  p99 ${fmtMs(r.p99).padStart(7)}  ` +
      `p99.9 ${fmtMs(r.p999).padStart(7)}  max ${fmtMs(r.max).padStart(8)}  ` +
      `fail ${String(r.errors + r.timeouts).padStart(5)}  lag p99 ${fmtMs(r.sendLagP99).padStart(5)}  ` +
      `inflight ${String(r.maxInFlight).padStart(5)}  hit ${(r.hitRate * 100).toFixed(1)}%  ${r.verdict}`,
  );
  if (Object.keys(r.errorKinds).length) {
    console.log(`          errors: ${Object.entries(r.errorKinds).map(([k, v]) => `${k} x${v}`).join(", ")}`);
  }
  if (r.containerCpu) {
    console.log(`          cpu: ${Object.entries(r.containerCpu).map(([k, v]) => `${k} ${v}`).join(", ")}`);
  }
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const range = (xs: number[]) => `${fmtMs(Math.min(...xs))} to ${fmtMs(Math.max(...xs))}`;

function printSummary(results: StepResult[]) {
  console.log(`\n▶ summary (median of ${REPEATS} repeats, range in brackets)`);
  console.log(`  ${"rate".padStart(6)}  ${"p50".padStart(22)}  ${"p95".padStart(24)}  ${"p99".padStart(24)}  ${"p99.9".padStart(24)}  verdicts`);
  for (const rate of RATES) {
    const steps = results.filter((r) => r.rate === rate);
    if (steps.length === 0) continue;
    const col = (pick: (r: StepResult) => number) =>
      `${fmtMs(median(steps.map(pick)))} [${range(steps.map(pick))}]`;
    console.log(
      `  ${String(rate).padStart(6)}  ${col((r) => r.p50).padStart(22)}  ${col((r) => r.p95).padStart(24)}  ${col((r) => r.p99).padStart(24)}  ` +
        `${col((r) => r.p999).padStart(24)}  ${steps.map((r) => r.verdict).join(",")}`,
    );
  }
  const sustained = RATES.filter((rate) => {
    const steps = results.filter((r) => r.rate === rate);
    return steps.length > 0 && steps.every((r) => r.verdict === "ok" && r.p99 <= SLO_P99_MS);
  });
  console.log(
    `\n  highest rate where every repeat was valid with p99 <= ${SLO_P99_MS}ms: ` +
      (sustained.length ? `${Math.max(...sustained)} req/s` : "none"),
  );
}

async function main() {
  try {
    await fetch(`${BASE}/metrics`);
  } catch {
    console.error(`Cannot reach ${BASE}. Is the cluster up? (docker compose up)`);
    process.exit(1);
  }

  const workload = await loadWorkload(DATA_PATH);
  const urls = buildRequestPool(workload);
  const workers = await startWorkers(urls);
  const startedAt = new Date().toISOString();

  console.log(`# Fixed-rate /suggest latency sweep against ${BASE}`);
  console.log(`started ${startedAt}, bun ${Bun.version}, rank=${RANK}, mix ${mixLabel(MIX)}`);
  console.log(`rates ${RATES.join(", ")} req/s, ${WORKERS} generator threads, ${DURATION_S}s per step (+${WARMUP_S}s warm-up), ${REPEATS} repeats, timeout ${TIMEOUT_MS}ms`);
  if (NOTE) console.log(`note: ${NOTE}`);
  console.log(`open loop: sends follow a fixed schedule and never wait on responses; failures count as inf\n`);

  const results: StepResult[] = [];
  sweep: for (const rate of RATES) {
    for (let repeat = 1; repeat <= REPEATS; repeat++) {
      await runStep(workers, rate, WARMUP_S, 0, false);
      const result = await runStep(workers, rate, DURATION_S, repeat, DOCKER_STATS && repeat === 1);
      results.push(result);
      printStep(result);
      if (result.verdict === "SATURATED") {
        console.log(`  stopping sweep: ${rate} req/s saturated, higher rates would only overload the cluster further`);
        break sweep;
      }
      await Bun.sleep(COOLDOWN_S * 1000);
    }
  }
  printSummary(results);

  const outPath = `${OUT_DIR}/latency-${LABEL}.json`;
  await Bun.write(
    outPath,
    JSON.stringify(
      {
        startedAt,
        bun: Bun.version,
        note: NOTE,
        config: { base: BASE, workers: WORKERS, rates: RATES, durationS: DURATION_S, warmupS: WARMUP_S, repeats: REPEATS, rank: RANK, mix: Object.fromEntries(CATEGORIES.map((c, i) => [c, MIX[i]])), timeoutMs: TIMEOUT_MS, maxSendLagMs: MAX_SEND_LAG_MS, sloP99Ms: SLO_P99_MS },
        results,
      },
      null,
      2,
    ),
  );
  console.log(`\nraw results written to ${outPath}`);
  for (const w of workers) w.terminate();
}

main();
