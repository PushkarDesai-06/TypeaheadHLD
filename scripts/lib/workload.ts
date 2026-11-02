/**
 * Shared `/suggest` traffic generator for the benchmarks, so the closed-loop
 * harness (bench.ts) and the fixed-rate latency sweep (bench-latency.ts) send
 * exactly the same kind of prefixes.
 *
 * Categories:
 *   seeded : 2–4 char prefixes of dataset queries (all pre-cached by the seeder)
 *   typo   : a 3–8 char prefix of a real query with one edit after the first letter
 *   novel  : random 4–8 letter strings that are not a prefix of any dataset query
 */

import { normalize, prefixesOf } from "../../src/config";

export const CATEGORIES = ["seeded", "typo", "novel"] as const;
export type Category = (typeof CATEGORIES)[number];

export function parseMix(raw: string | undefined, fallback = "70,20,10"): number[] {
  const weights = (raw ?? fallback).split(",").map(Number);
  if (weights.length !== 3 || weights.some((w) => !(w >= 0)) || weights.every((w) => w === 0)) {
    throw new Error(`mix must be three non-negative numbers (seeded,typo,novel), got "${raw}"`);
  }
  return weights;
}

export const mixLabel = (weights: number[]) => CATEGORIES.map((c, i) => `${c} ${weights[i]}`).join(" / ");

export interface Workload {
  seeded: string[];
  /** Dataset queries starting with 4+ letters, used as the source text for typos. */
  queries: string[];
  /** Every prefix the seeder caches. */
  knownPrefixes: Set<string>;
}

interface Entry {
  query: string;
  count: number;
}

export async function loadWorkload(dataPath: string): Promise<Workload> {
  const entries: Entry[] = JSON.parse(await Bun.file(dataPath).text());
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

/** One edit (substitute, delete, insert, or swap adjacent letters) after the first character. */
export function typoPrefix(w: Workload): string {
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

export function novelPrefix(w: Workload): string {
  while (true) {
    const s = Array.from({ length: 4 + randInt(5) }, randLetter).join("");
    if (!w.knownPrefixes.has(s)) return s;
  }
}

export function pickCategory(weights: number[]): Category {
  const total = weights.reduce((a, b) => a + b, 0);
  let r = Math.random() * total;
  for (let i = 0; i < CATEGORIES.length; i++) {
    r -= weights[i]!;
    if (r < 0) return CATEGORIES[i]!;
  }
  return "seeded";
}

export function prefixFor(category: Category, w: Workload): string {
  if (category === "typo") return typoPrefix(w);
  if (category === "novel") return novelPrefix(w);
  return pick(w.seeded);
}

/** Nearest-rank percentile over an ascending-sorted array. */
export function percentile(sorted: ArrayLike<number>, p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)]!;
}
