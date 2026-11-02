/**
 * One open-loop load generator thread for bench-latency.ts. Each worker has its
 * own event loop and GC heap, so a pause in one worker does not delay the sends
 * of the others.
 *
 * Requests go out on a fixed schedule that never waits for responses, so a
 * server stall keeps receiving traffic and the queueing it causes is measured in
 * full. Latency is timed from the actual send. How far each send drifted from its
 * schedule is reported separately (send lag), to prove the schedule was kept.
 */

declare const self: Worker;

export interface InitMessage {
  type: "init";
  urls: string[];
  timeoutMs: number;
}

export interface StepMessage {
  type: "step";
  rate: number;
  durationS: number;
  startAt: number;
}

export interface StepReply {
  latencies: Float64Array;
  sendLag: Float64Array;
  succeeded: number;
  errors: number;
  timeouts: number;
  hits: number;
  maxInFlight: number;
  errorKinds: Record<string, number>;
  sendWindowMs: number;
}

let urls: string[] = [];
let timeoutMs = 5000;
let urlCursor = Math.floor(Math.random() * 1_000_000);

const TICK_MS = 1;

async function runStep({ rate, durationS, startAt }: StepMessage): Promise<StepReply> {
  const total = Math.round(rate * durationS);
  const intervalMs = 1000 / rate;
  const latencies = new Float64Array(total);
  const sendLag = new Float64Array(total);
  const errorKinds: Record<string, number> = {};
  const countError = (kind: string) => (errorKinds[kind] = (errorKinds[kind] ?? 0) + 1);
  let succeeded = 0;
  let errors = 0;
  let timeouts = 0;
  let hits = 0;
  let inFlight = 0;
  let maxInFlight = 0;

  const fire = (i: number) => {
    const sentAt = performance.now();
    inFlight++;
    if (inFlight > maxInFlight) maxInFlight = inFlight;
    fetch(urls[urlCursor++ % urls.length]!, { signal: AbortSignal.timeout(timeoutMs) })
      .then(async (res) => {
        const body = await res.text();
        const done = performance.now();
        const source = body.includes('"source":"cache"') ? "cache" : body.includes('"source":"db"') ? "db" : null;
        if (!res.ok || !source) {
          errors++;
          countError(`HTTP ${res.status}`);
          latencies[i] = Infinity;
          return;
        }
        latencies[i] = done - sentAt;
        succeeded++;
        if (source === "cache") hits++;
      })
      .catch((err: Error) => {
        if (err.name === "TimeoutError") timeouts++;
        else errors++;
        countError(err.name === "TimeoutError" ? "timeout" : `${err.name}: ${err.message}`.slice(0, 80));
        latencies[i] = Infinity;
      })
      .finally(() => inFlight--);
  };

  const epochNow = () => performance.timeOrigin + performance.now();
  const start = performance.now() + Math.max(0, startAt - epochNow());
  while (performance.now() < start) await Bun.sleep(Math.max(0, start - performance.now()));

  let sent = 0;
  while (sent < total) {
    const due = Math.min(total, Math.floor((performance.now() - start) / intervalMs) + 1);
    while (sent < due) {
      const intended = start + sent * intervalMs;
      sendLag[sent] = performance.now() - intended;
      fire(sent);
      sent++;
    }
    await Bun.sleep(TICK_MS);
  }
  const sendWindowMs = performance.now() - start;

  const drainDeadline = performance.now() + timeoutMs + 1000;
  while (inFlight > 0 && performance.now() < drainDeadline) await Bun.sleep(5);

  return { latencies, sendLag, succeeded, errors, timeouts, hits, maxInFlight, errorKinds, sendWindowMs };
}

self.onmessage = async (event: MessageEvent<InitMessage | StepMessage>) => {
  const msg = event.data;
  if (msg.type === "init") {
    urls = msg.urls;
    timeoutMs = msg.timeoutMs;
    self.postMessage({ ready: true });
    return;
  }
  const reply = await runStep(msg);
  self.postMessage(reply, [reply.latencies.buffer as ArrayBuffer, reply.sendLag.buffer as ArrayBuffer]);
};
