const { MinHeap } = require("../heap");
const { readyScore } = require("../scoring");

const RECENT_LIMIT = 100;

// Keeps everything in this process. Two heaps do the scheduling:
//  - ready:   jobs that can run now, ordered by priority score (with aging)
//  - delayed: jobs waiting for a retry or a rate-limit slot, ordered by runAt
// Used for tests and when no Redis is configured.
class MemoryStore {
  constructor() {
    this.reset();
  }

  reset() {
    this.jobs = new Map();
    this.ready = new MinHeap((a, b) => a.score - b.score || a.seq - b.seq);
    this.delayed = new MinHeap((a, b) => a.runAt - b.runAt || a.seq - b.seq);
    this.active = new Set();
    this.recent = []; // ids of finished jobs, newest first
    this.dead = []; // ids in the dead-letter queue, newest first
    this.counters = { completed: 0, retried: 0, dead: 0, enqueued: 0 };
    this.seq = 0;
  }

  async connect() {}
  async close() {}

  async add(job) {
    this.jobs.set(job.id, { ...job });
    this.counters.enqueued += 1;
    if (job.runAt > Date.now()) this.delayed.push({ id: job.id, runAt: job.runAt, seq: this.seq++ });
    else this.ready.push({ id: job.id, score: readyScore(job.priority, job.createdAt), seq: this.seq++ });
  }

  // Move delayed jobs whose time has come into the ready heap
  promoteDue(now) {
    while (this.delayed.size && this.delayed.peek().runAt <= now) {
      const { id } = this.delayed.pop();
      const job = this.jobs.get(id);
      if (!job || job.state !== "delayed") continue;
      job.state = "waiting";
      this.ready.push({ id, score: readyScore(job.priority, job.runAt), seq: this.seq++ });
    }
  }

  async claim(now = Date.now()) {
    this.promoteDue(now);
    while (this.ready.size) {
      const { id } = this.ready.pop();
      const job = this.jobs.get(id);
      if (!job || job.state !== "waiting") continue;
      job.state = "active";
      job.startedAt = now;
      job.attempts += 1;
      this.active.add(id);
      return { ...job };
    }
    return null;
  }

  async complete(id, { result, durationMs }) {
    const job = this.jobs.get(id);
    if (!job) return;
    Object.assign(job, { state: "completed", result, durationMs, finishedAt: Date.now() });
    this.active.delete(id);
    this.counters.completed += 1;
    this.pushRecent(id);
  }

  // retryAt: when to try again, or null to send it to the dead-letter queue
  async fail(id, { error, retryAt }) {
    const job = this.jobs.get(id);
    if (!job) return;
    this.active.delete(id);
    job.lastError = error;
    if (retryAt) {
      Object.assign(job, { state: "delayed", runAt: retryAt });
      this.delayed.push({ id, runAt: retryAt, seq: this.seq++ });
      this.counters.retried += 1;
    } else {
      Object.assign(job, { state: "dead", finishedAt: Date.now() });
      this.counters.dead += 1;
      this.dead.unshift(id);
      this.pushRecent(id);
    }
  }

  // Put a claimed job back without counting the attempt (e.g. rate limited)
  async defer(id, runAt) {
    const job = this.jobs.get(id);
    if (!job) return;
    this.active.delete(id);
    Object.assign(job, { state: "delayed", runAt, attempts: job.attempts - 1 });
    this.delayed.push({ id, runAt, seq: this.seq++ });
  }

  // Send a dead job back to the queue for a fresh set of attempts
  async retryDead(id) {
    const job = this.jobs.get(id);
    if (!job || job.state !== "dead") return false;
    this.dead = this.dead.filter((x) => x !== id);
    Object.assign(job, { state: "waiting", attempts: 0, lastError: null, createdAt: Date.now() });
    this.ready.push({ id, score: readyScore(job.priority, job.createdAt), seq: this.seq++ });
    return true;
  }

  // After a crash, jobs left "active" were never finished: put them back
  async recoverActive() {
    let n = 0;
    for (const id of this.active) {
      const job = this.jobs.get(id);
      job.state = "waiting";
      job.attempts -= 1;
      this.ready.push({ id, score: readyScore(job.priority, job.createdAt), seq: this.seq++ });
      n += 1;
    }
    this.active.clear();
    return n;
  }

  pushRecent(id) {
    this.recent.unshift(id);
    if (this.recent.length > RECENT_LIMIT) {
      const dropped = this.recent.pop();
      const job = this.jobs.get(dropped);
      // Forget old finished jobs so memory stays bounded (dead jobs are kept)
      if (job && job.state === "completed") this.jobs.delete(dropped);
    }
  }

  async get(id) {
    const job = this.jobs.get(id);
    return job ? { ...job } : null;
  }

  async counts() {
    let waiting = 0;
    let delayed = 0;
    for (const job of this.jobs.values()) {
      if (job.state === "waiting") waiting += 1;
      else if (job.state === "delayed") delayed += 1;
    }
    return { waiting, delayed, active: this.active.size, deadQueue: this.dead.length, ...this.counters };
  }

  async listRecent(limit = 30) {
    return this.recent.slice(0, limit).map((id) => this.jobs.get(id)).filter(Boolean).map((j) => ({ ...j }));
  }

  async listDead(limit = 30) {
    return this.dead.slice(0, limit).map((id) => this.jobs.get(id)).filter(Boolean).map((j) => ({ ...j }));
  }

  async clear() {
    this.reset();
  }
}

module.exports = { MemoryStore };
