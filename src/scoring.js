// How jobs are ordered.
//
// Each priority level is worth AGING_MS of waiting time. A "low" job that has
// waited 2 x AGING_MS longer than a "high" job is treated as equal to it, so
// low-priority work can be delayed but never starved forever.

const PRIORITIES = { high: 0, normal: 1, low: 2 };
const AGING_MS = Number(process.env.AGING_MS) || 15_000;

const priorityLevel = (priority) => {
  if (!(priority in PRIORITIES)) throw new Error(`Unknown priority "${priority}". Use high, normal or low.`);
  return PRIORITIES[priority];
};

// Lower score runs first
const readyScore = (priority, readySince) => priorityLevel(priority) * AGING_MS + readySince;

module.exports = { PRIORITIES, AGING_MS, priorityLevel, readyScore };
