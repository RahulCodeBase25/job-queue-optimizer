const { test } = require("node:test");
const assert = require("node:assert/strict");
const { MinHeap } = require("../src/heap");
const { TokenBucket } = require("../src/rateLimiter");
const { backoffDelay } = require("../src/queue");
const { readyScore, AGING_MS } = require("../src/scoring");

test("heap pops items in order, including after random pushes", () => {
  const heap = new MinHeap((a, b) => a - b);
  const values = Array.from({ length: 500 }, () => Math.floor(Math.random() * 1000));
  values.forEach((v) => heap.push(v));
  const out = [];
  while (heap.size) out.push(heap.pop());
  assert.deepEqual(out, [...values].sort((a, b) => a - b));
  assert.equal(heap.pop(), undefined);
});

test("heap removeWhere keeps heap order", () => {
  const heap = new MinHeap((a, b) => a - b);
  [5, 3, 9, 1, 7, 2].forEach((v) => heap.push(v));
  assert.equal(heap.removeWhere((v) => v % 2 === 1), 5);
  assert.deepEqual([heap.pop(), heap.pop()], [2, undefined]);
});

test("aging: a low job that waited 2 x AGING_MS longer ties with a fresh high job", () => {
  const now = 1_000_000_000;
  assert.equal(readyScore("low", now - 2 * AGING_MS), readyScore("high", now));
  assert.ok(readyScore("low", now - 2 * AGING_MS - 1) < readyScore("high", now));
  assert.throws(() => readyScore("urgent", now), /Unknown priority/);
});

test("backoff grows exponentially, stays within its cap, and is jittered", () => {
  const opts = { baseMs: 100, maxMs: 1000 };
  const max = (attempt) => backoffDelay(attempt, { ...opts, random: () => 1 });
  const min = (attempt) => backoffDelay(attempt, { ...opts, random: () => 0 });
  assert.deepEqual([1, 2, 3, 4, 5].map(max), [100, 200, 400, 800, 1000]);
  assert.deepEqual([1, 2, 3].map(min), [50, 100, 200]);
});

test("token bucket allows a burst, then the steady rate", () => {
  let t = 0;
  const bucket = new TokenBucket({ perSecond: 2, capacity: 2 }, () => t);
  assert.equal(bucket.take(), 0);
  assert.equal(bucket.take(), 0);
  assert.equal(bucket.take(), 500); // empty: next token in 500 ms
  t = 500;
  assert.equal(bucket.take(), 0);
});
