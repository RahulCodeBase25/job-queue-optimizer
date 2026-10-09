const crypto = require("crypto");
const { EventEmitter } = require("events");
const { PRIORITIES } = require("./scoring");
const { TokenBucket } = require("./rateLimiter");

const MAX_BACKLOG = Number(process.env.MAX_BACKLOG) || 5000;

// Retry delay: exponential (base, 2x, 4x, ...) capped at maxMs, with "full
// jitter" so many failing jobs don't all retry at the same moment.
function backoffDelay(attempt, { baseMs = 500, maxMs = 30_000, random = Math.random } = {}) {
  const exp = Math.min(maxMs, baseMs * 2 ** (attempt - 1));
  return Math.round(exp / 2 + random() * (exp / 2));
}

class JobQueue extends EventEmitter {
  constructor({ store, jobTypes, concurrency = 4, pollMs = 100, backoff = {}, jobTimeoutMs = 10_000 }) {
    super();
    this.store = store;
    this.jobTypes = jobTypes;
    this.concurrency = concurrency;
    this.pollMs = pollMs;
    this.backoff = backoff;
    this.jobTimeoutMs = jobTimeoutMs;
    this.running = false;
    this.workers = [];
    this.limiters = {};
    for (const [type, def] of Object.entries(jobTypes)) {
      if (def.rateLimit) this.limiters[type] = new TokenBucket(def.rateLimit);
    }
    this.completedAt = []; // timestamps of recent completions, for throughput
  }

  async add(type, payload = {}, { priority = "normal", delayMs = 0, maxAttempts } = {}) {
    const def = this.jobTypes[type];
    if (!def) throw new Error(`Unknown job type "${type}"`);
    if (!(priority in PRIORITIES)) throw new Error(`Unknown priority "${priority}". Use high, normal or low.`);
    const { waiting, delayed } = await this.store.counts();
    if (waiting + delayed >= MAX_BACKLOG) throw new Error(`Queue is full (${MAX_BACKLOG} jobs). Try again in a moment.`);

    const now = Date.now();
    const job = {
      id: crypto.randomUUID().slice(0, 8),
      type,
      priority,
      payload,
      state: delayMs > 0 ? "delayed" : "waiting",
      attempts: 0,
      maxAttempts: maxAttempts || def.maxAttempts || 3,
      createdAt: now,
      runAt: now + delayMs,
      lastError: null,
    };
    await this.store.add(job);
    this.emit("added", job);
    return job;
  }

  start() {
    if (this.running) return;
    this.running = true;
    for (let i = 0; i < this.concurrency; i++) this.workers.push(this.workerLoop(i));
  }

  async stop() {
    this.running = false;
    await Promise.all(this.workers);
    this.workers = [];
  }

  async workerLoop() {
    while (this.running) {
      let job;
      try {
        job = await this.store.claim();
      } catch (err) {
        this.emit("error", err);
      }
      if (!job) {
        await new Promise((r) => setTimeout(r, this.pollMs));
        continue;
      }
      await this.process(job);
    }
  }

  async process(job) {
    const def = this.jobTypes[job.type];

    // Respect the per-type rate limit: if no slot is free, put the job back
    // until one is, without counting it as an attempt.
    const limiter = this.limiters[job.type];
    const waitMs = limiter ? limiter.take() : 0;
    if (waitMs > 0) {
      await this.store.defer(job.id, Date.now() + waitMs);
      this.emit("throttled", { ...job, waitMs });
      return;
    }

    const started = Date.now();
    try {
      const result = await withTimeout(def.handler(job.payload, job), this.jobTimeoutMs);
      const durationMs = Date.now() - started;
      await this.store.complete(job.id, { result, durationMs });
      this.completedAt.push(Date.now());
      this.emit("completed", { ...job, durationMs });
    } catch (err) {
      const message = err?.message || String(err);
      if (job.attempts < job.maxAttempts) {
        const delay = backoffDelay(job.attempts, this.backoff);
        await this.store.fail(job.id, { error: message, retryAt: Date.now() + delay });
        this.emit("retrying", { ...job, error: message, retryInMs: delay });
      } else {
        await this.store.fail(job.id, { error: message, retryAt: null });
        this.emit("dead", { ...job, error: message });
      }
    }
  }

  // Completed jobs per second, averaged over the last `windowMs`
  throughput(windowMs = 10_000) {
    const cutoff = Date.now() - windowMs;
    while (this.completedAt.length && this.completedAt[0] < cutoff) this.completedAt.shift();
    return +(this.completedAt.length / (windowMs / 1000)).toFixed(1);
  }
}

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`Timed out after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

module.exports = { JobQueue, backoffDelay };
