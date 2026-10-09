const path = require("path");
const express = require("express");
const { chaos } = require("./handlers");

const PRIORITY_NAMES = ["high", "normal", "low"];

// Small per-IP limit on write endpoints, so the public demo can't be flooded
function writeLimiter({ max = 40, windowMs = 60_000 } = {}) {
  const hits = new Map();
  return (req, res, next) => {
    const now = Date.now();
    const entry = hits.get(req.ip) || { count: 0, resetAt: now + windowMs };
    if (now > entry.resetAt) Object.assign(entry, { count: 0, resetAt: now + windowMs });
    entry.count += 1;
    hits.set(req.ip, entry);
    if (entry.count > max) return res.status(429).json({ message: "Too many requests. Wait a minute and try again." });
    next();
  };
}

function createApp({ queue, storeKind }) {
  const app = express();
  app.set("trust proxy", 1);
  app.use(express.json({ limit: "20kb" }));
  app.use(express.static(path.join(__dirname, "..", "public")));

  const writes = writeLimiter();

  app.get("/health", (req, res) => res.json({ status: "ok", store: storeKind }));

  app.get("/api/types", (req, res) => {
    res.json(
      Object.entries(queue.jobTypes).map(([type, d]) => ({
        type,
        label: d.label,
        maxAttempts: d.maxAttempts,
        rateLimitPerSecond: d.rateLimit?.perSecond || null,
      }))
    );
  });

  const stats = async () => ({
    ...(await queue.store.counts()),
    throughput: queue.throughput(),
    concurrency: queue.concurrency,
    store: storeKind,
    extraFailure: chaos.extraFailure,
  });

  app.get("/api/stats", async (req, res) => res.json(await stats()));

  // Add one job
  app.post("/api/jobs", writes, async (req, res) => {
    try {
      const { type, priority = "normal", payload = {}, delayMs = 0 } = req.body || {};
      const job = await queue.add(type, payload, { priority, delayMs: Math.min(Math.max(0, Number(delayMs) || 0), 60_000) });
      res.status(201).json(job);
    } catch (err) {
      res.status(400).json({ message: err.message });
    }
  });

  // Add many jobs at once (for the demo): a random mix, or one type/priority
  app.post("/api/jobs/bulk", writes, async (req, res) => {
    try {
      const count = Math.min(Math.max(1, Number(req.body?.count) || 1), 500);
      const types = Object.keys(queue.jobTypes);
      const { type, priority } = req.body || {};
      const created = [];
      for (let i = 0; i < count; i++) {
        const t = type || types[Math.floor(Math.random() * types.length)];
        const p = priority || PRIORITY_NAMES[Math.floor(Math.random() * 3)];
        created.push(await queue.add(t, { n: i + 1 }, { priority: p }));
      }
      res.status(201).json({ added: created.length });
    } catch (err) {
      res.status(400).json({ message: err.message });
    }
  });

  app.get("/api/jobs", async (req, res) => {
    const limit = Math.min(Number(req.query.limit) || 30, 100);
    const list = req.query.list === "dead" ? await queue.store.listDead(limit) : await queue.store.listRecent(limit);
    res.json(list);
  });

  app.get("/api/jobs/:id", async (req, res) => {
    const job = await queue.store.get(req.params.id);
    if (!job) return res.status(404).json({ message: "Job not found (finished jobs are kept for an hour)" });
    res.json(job);
  });

  // Give a dead job a fresh set of attempts
  app.post("/api/jobs/:id/retry", writes, async (req, res) => {
    const ok = await queue.store.retryDead(req.params.id);
    if (!ok) return res.status(404).json({ message: "No dead job with that id" });
    res.json({ message: "Job sent back to the queue" });
  });

  // Turn the failure rate up or down, to see retries and the dead-letter queue in action
  app.post("/api/chaos", writes, (req, res) => {
    const value = Number(req.body?.extraFailure);
    if (!(value >= 0 && value <= 0.8)) return res.status(400).json({ message: "extraFailure must be between 0 and 0.8" });
    chaos.extraFailure = value;
    res.json({ extraFailure: value });
  });

  // Live updates for the dashboard (Server-Sent Events):
  // every job event, plus a stats snapshot once a second
  const clients = new Set();
  app.get("/api/events", async (req, res) => {
    res.set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
    res.flushHeaders();
    res.write(`event: stats\ndata: ${JSON.stringify(await stats())}\n\n`);
    clients.add(res);
    req.on("close", () => clients.delete(res));
  });

  const send = (event, data) => {
    const msg = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    clients.forEach((c) => c.write(msg));
  };
  const slim = (j) => ({ id: j.id, type: j.type, priority: j.priority, attempts: j.attempts, maxAttempts: j.maxAttempts, error: j.error, durationMs: j.durationMs, retryInMs: j.retryInMs });
  for (const ev of ["completed", "retrying", "dead"]) queue.on(ev, (j) => send("job", { event: ev, job: slim(j) }));

  const ticker = setInterval(async () => {
    if (clients.size) send("stats", await stats().catch(() => ({})));
  }, 1000);
  ticker.unref();

  return app;
}

module.exports = { createApp };
