// Demo job types. Each one simulates real work: it takes a while and
// sometimes fails, like a flaky email provider or a slow report query.
// `chaos.extraFailure` lets the dashboard turn up the failure rate on purpose.

const chaos = { extraFailure: 0 };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const between = (min, max) => min + Math.random() * (max - min);

const simulate = ({ minMs, maxMs, failRate, error }) => async (payload) => {
  await sleep(between(minMs, maxMs));
  if (Math.random() < Math.min(0.95, failRate + chaos.extraFailure)) throw new Error(error);
  return { ok: true, ...(payload?.echo ? { echo: payload.echo } : {}) };
};

const jobTypes = {
  email: {
    label: "Send email",
    handler: simulate({ minMs: 60, maxMs: 250, failRate: 0.1, error: "Email provider timed out" }),
    rateLimit: { perSecond: 8, capacity: 8 }, // providers cap how fast you can send
    maxAttempts: 4,
  },
  report: {
    label: "Generate report",
    handler: simulate({ minMs: 400, maxMs: 1400, failRate: 0.05, error: "Report query failed" }),
    rateLimit: null,
    maxAttempts: 3,
  },
  "data-sync": {
    label: "Sync data",
    handler: simulate({ minMs: 120, maxMs: 600, failRate: 0.2, error: "Upstream API returned 503" }),
    rateLimit: { perSecond: 5, capacity: 5 },
    maxAttempts: 5,
  },
};

module.exports = { jobTypes, chaos, sleep };
