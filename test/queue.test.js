// The same behaviour tests run against both stores: in-memory, and real Redis
// (when a redis-server binary is available, it is started on a spare port).

const { test, describe, before, after, beforeEach } = require("node:test");
const assert = require("node:assert/strict");
const { spawn, execSync } = require("child_process");
const { JobQueue } = require("../src/queue");
const { MemoryStore } = require("../src/stores/memoryStore");
const { RedisStore } = require("../src/stores/redisStore");
const { AGING_MS } = require("../src/scoring");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const waitFor = async (check, timeoutMs = 5000) => {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await check()) return;
    await sleep(20);
  }
  throw new Error("Timed out waiting for condition");
};

const makeJob = (id, priority, createdAt, extra = {}) => ({
  id,
  type: "t",
  priority,
  payload: {},
  state: "waiting",
  attempts: 0,
  maxAttempts: 3,
  createdAt,
  runAt: createdAt,
  lastError: null,
  ...extra,
});

let redisProcess = null;
const REDIS_PORT = 6390;
const hasRedis = (() => {
  try {
    execSync("which redis-server", { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
})();

before(async () => {
  if (!hasRedis) return;
  redisProcess = spawn("redis-server", ["--port", String(REDIS_PORT), "--save", "", "--appendonly", "no"], { stdio: "ignore" });
  await sleep(300);
});

after(() => redisProcess?.kill());

const stores = [["memory", () => new MemoryStore()]];
if (hasRedis) stores.push(["redis", () => new RedisStore(`redis://127.0.0.1:${REDIS_PORT}`, { prefix: "test:" })]);

for (const [name, makeStore] of stores) {
  describe(`${name} store`, () => {
    let store;

    beforeEach(async () => {
      if (store) await store.close();
      store = makeStore();
      await store.connect();
      await store.clear();
    });

    after(async () => store && store.close());

    test("claims high before normal before low", async () => {
      const now = Date.now();
      await store.add(makeJob("low", "low", now));
      await store.add(makeJob("norm", "normal", now));
      await store.add(makeJob("high", "high", now));
      const order = [];
      for (let i = 0; i < 3; i++) order.push((await store.claim()).id);
      assert.deepEqual(order, ["high", "norm", "low"]);
      assert.equal(await store.claim(), null);
    });

    test("aging: an old low-priority job beats a new high-priority one", async () => {
      const now = Date.now();
      await store.add(makeJob("fresh-high", "high", now));
      await store.add(makeJob("old-low", "low", now - 2 * AGING_MS - 1000));
      assert.equal((await store.claim()).id, "old-low");
    });

    test("delayed jobs wait until their time", async () => {
      const now = Date.now();
      await store.add(makeJob("later", "high", now, { state: "delayed", runAt: now + 150 }));
      assert.equal(await store.claim(), null);
      await sleep(200);
      assert.equal((await store.claim()).id, "later");
    });

    test("a job is never handed to two workers at once", async () => {
      const now = Date.now();
      for (let i = 0; i < 50; i++) await store.add(makeJob(`j${i}`, "normal", now - i));
      const claims = await Promise.all(Array.from({ length: 80 }, () => store.claim()));
      const ids = claims.filter(Boolean).map((j) => j.id);
      assert.equal(ids.length, 50);
      assert.equal(new Set(ids).size, 50);
    });

    test("crash recovery puts unfinished active jobs back in the queue", async () => {
      await store.add(makeJob("crashy", "normal", Date.now()));
      const job = await store.claim();
      assert.equal(job.attempts, 1);
      assert.equal(await store.recoverActive(), 1);
      const again = await store.claim();
      assert.equal(again.id, "crashy");
      assert.equal(again.attempts, 1, "the interrupted try doesn't count");
    });

    test("worker: failing jobs retry with backoff, then go to the dead-letter queue", async () => {
      let calls = 0;
      const queue = new JobQueue({
        store,
        concurrency: 2,
        pollMs: 10,
        backoff: { baseMs: 20, maxMs: 50 },
        jobTypes: {
          flaky: { handler: async () => { calls += 1; throw new Error("boom"); }, maxAttempts: 3 },
        },
      });
      const events = [];
      queue.on("retrying", () => events.push("retrying"));
      queue.on("dead", () => events.push("dead"));
      const job = await queue.add("flaky");
      queue.start();
      await waitFor(async () => (await store.get(job.id))?.state === "dead");
      await queue.stop();

      assert.equal(calls, 3);
      assert.deepEqual(events, ["retrying", "retrying", "dead"]);
      const dead = await store.listDead();
      assert.equal(dead[0].id, job.id);
      assert.equal(dead[0].lastError, "boom");
      const counts = await store.counts();
      assert.equal(counts.retried, 2);
      assert.equal(counts.dead, 1);

      assert.equal(await store.retryDead(job.id), true);
      assert.equal((await store.get(job.id)).state, "waiting");
      assert.equal((await store.counts()).deadQueue, 0);
    });

    test("worker: completes jobs and records results", async () => {
      const queue = new JobQueue({
        store,
        concurrency: 3,
        pollMs: 10,
        jobTypes: { ok: { handler: async (p) => ({ doubled: p.n * 2 }) } },
      });
      const jobs = [];
      for (let n = 1; n <= 10; n++) jobs.push(await queue.add("ok", { n }));
      queue.start();
      await waitFor(async () => (await store.counts()).completed === 10);
      await queue.stop();
      const last = await store.get(jobs[9].id);
      assert.equal(last.state, "completed");
      assert.deepEqual(last.result, { doubled: 20 });
      assert.equal((await store.listRecent(5)).length, 5);
    });

    test("worker: rate limit spreads a type's jobs out without using up attempts", async () => {
      const ranAt = [];
      const queue = new JobQueue({
        store,
        concurrency: 4,
        pollMs: 5,
        jobTypes: {
          limited: { handler: async () => ranAt.push(Date.now()), rateLimit: { perSecond: 10, capacity: 1 }, maxAttempts: 1 },
        },
      });
      for (let i = 0; i < 4; i++) await queue.add("limited");
      queue.start();
      await waitFor(async () => (await store.counts()).completed === 4);
      await queue.stop();
      // 1 at once, then one per ~100 ms
      assert.ok(ranAt[3] - ranAt[0] >= 250, `took ${ranAt[3] - ranAt[0]} ms`);
      assert.equal((await store.counts()).dead, 0);
    });

    test("rejects unknown types and priorities", async () => {
      const queue = new JobQueue({ store, jobTypes: { ok: { handler: async () => 1 } } });
      await assert.rejects(queue.add("nope"), /Unknown job type/);
      await assert.rejects(queue.add("ok", {}, { priority: "urgent" }), /Unknown priority/);
    });
  });
}
