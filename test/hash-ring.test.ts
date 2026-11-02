import { test, expect } from "bun:test";
import { HashRing, ringHash, ring, route, SHARDS } from "../src/hash-ring";

test("ringHash is deterministic and unsigned 32-bit", () => {
  expect(ringHash("google")).toBe(ringHash("google"));
  expect(ringHash("google")).not.toBe(ringHash("yahoo"));
  const h = ringHash("anything");
  expect(h).toBeGreaterThanOrEqual(0);
  expect(h).toBeLessThanOrEqual(0xffffffff);
  expect(Number.isInteger(h)).toBe(true);
});

test("route() is deterministic across independent ring instances", () => {
  // The seeder, LB and app nodes each build their own ring; they must agree.
  const a = new HashRing();
  const b = new HashRing();
  for (const key of ["google", "g", "go", "yahoo.com", "facebook", "-", "amazon"]) {
    expect(a.route(key)).toBe(b.route(key));
    expect(route(key)).toBe(a.route(key)); // shared singleton agrees too
  }
});

test("route() only ever returns a known shard id", () => {
  for (let i = 0; i < 1000; i++) {
    expect(SHARDS).toContain(ring.route(`key-${i}`));
  }
});

test("virtual nodes spread keys reasonably evenly across shards", () => {
  const counts: Record<string, number> = { "1": 0, "2": 0, "3": 0 };
  const N = 30000;
  for (let i = 0; i < N; i++) counts[ring.route(`query-${i}`)]!++;

  const expected = N / SHARDS.length;
  for (const shard of SHARDS) {
    expect(counts[shard]).toBeGreaterThan(expected * 0.9);
    expect(counts[shard]).toBeLessThan(expected * 1.1);
  }
});

test("all prefixes of a key resolve to a valid shard (wrap-around safety)", () => {
  const q = "google.com";
  for (let i = 1; i <= q.length; i++) {
    expect(SHARDS).toContain(route(q.slice(0, i)));
  }
});
