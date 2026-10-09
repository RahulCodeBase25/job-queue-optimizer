const Redis = require("ioredis");
const { AGING_MS, readyScore } = require("../scoring");

// Redis layout (all keys start with the prefix, default "jq:"):
//   job:<id>   JSON of the job
//   ready      sorted set, score = priority score with aging (lower runs first)
//   delayed    sorted set, score = runAt timestamp
//   active     set of job ids being worked on right now
//   recent     list of recently finished job ids (newest first)
//   dead       list of job ids in the dead-letter queue
//   counters   hash: enqueued, completed, retried, dead
//
// Claiming a job is one Lua script, so it's atomic: with any number of worker
// processes, a job is handed to exactly one of them.

const CLAIM_SCRIPT = `
local now = tonumber(ARGV[1])
local aging = tonumber(ARGV[2])
local prefix = ARGV[3]
local levels = { high = 0, normal = 1, low = 2 }

-- 1. Move delayed jobs whose time has come into the ready set
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 200)
for _, id in ipairs(due) do
  redis.call('ZREM', KEYS[2], id)
  local key = prefix .. 'job:' .. id
  local raw = redis.call('GET', key)
  if raw then
    local job = cjson.decode(raw)
    if job.state == 'delayed' then
      job.state = 'waiting'
      redis.call('SET', key, cjson.encode(job))
      redis.call('ZADD', KEYS[1], levels[job.priority] * aging + job.runAt, id)
    end
  end
end

-- 2. Take the best ready job
while true do
  local popped = redis.call('ZPOPMIN', KEYS[1])
  if #popped == 0 then return false end
  local id = popped[1]
  local key = prefix .. 'job:' .. id
  local raw = redis.call('GET', key)
  if raw then
    local job = cjson.decode(raw)
    if job.state == 'waiting' then
      job.state = 'active'
      job.startedAt = now
      job.attempts = job.attempts + 1
      local out = cjson.encode(job)
      redis.call('SET', key, out)
      redis.call('SADD', KEYS[3], id)
      return out
    end
  end
end
`;

const FINISHED_TTL_SECONDS = 3600; // finished jobs are forgotten after an hour
const RECENT_LIMIT = 100;
const DEAD_LIMIT = 500;

class RedisStore {
  constructor(url, { prefix = "jq:" } = {}) {
    this.url = url;
    this.prefix = prefix;
  }

  key(name) {
    return this.prefix + name;
  }

  jobKey(id) {
    return this.prefix + "job:" + id;
  }

  async connect() {
    this.redis = new Redis(this.url, { maxRetriesPerRequest: 3, lazyConnect: true });
    await this.redis.connect();
    this.redis.defineCommand("claimJob", { numberOfKeys: 3, lua: CLAIM_SCRIPT });
  }

  async close() {
    await this.redis.quit();
  }

  async add(job) {
    const multi = this.redis.multi().set(this.jobKey(job.id), JSON.stringify(job)).hincrby(this.key("counters"), "enqueued", 1);
    if (job.runAt > Date.now()) multi.zadd(this.key("delayed"), job.runAt, job.id);
    else multi.zadd(this.key("ready"), readyScore(job.priority, job.createdAt), job.id);
    await multi.exec();
  }

  async claim(now = Date.now()) {
    const raw = await this.redis.claimJob(this.key("ready"), this.key("delayed"), this.key("active"), now, AGING_MS, this.prefix);
    return raw ? JSON.parse(raw) : null;
  }

  async get(id) {
    const raw = await this.redis.get(this.jobKey(id));
    return raw ? JSON.parse(raw) : null;
  }

  async complete(id, { result, durationMs }) {
    const job = await this.get(id);
    if (!job) return;
    Object.assign(job, { state: "completed", result, durationMs, finishedAt: Date.now() });
    await this.redis
      .multi()
      .set(this.jobKey(id), JSON.stringify(job), "EX", FINISHED_TTL_SECONDS)
      .srem(this.key("active"), id)
      .hincrby(this.key("counters"), "completed", 1)
      .lpush(this.key("recent"), id)
      .ltrim(this.key("recent"), 0, RECENT_LIMIT - 1)
      .exec();
  }

  async fail(id, { error, retryAt }) {
    const job = await this.get(id);
    if (!job) return;
    job.lastError = error;
    const multi = this.redis.multi().srem(this.key("active"), id);
    if (retryAt) {
      Object.assign(job, { state: "delayed", runAt: retryAt });
      multi
        .set(this.jobKey(id), JSON.stringify(job))
        .zadd(this.key("delayed"), retryAt, id)
        .hincrby(this.key("counters"), "retried", 1);
    } else {
      Object.assign(job, { state: "dead", finishedAt: Date.now() });
      multi
        .set(this.jobKey(id), JSON.stringify(job))
        .hincrby(this.key("counters"), "dead", 1)
        .lpush(this.key("dead"), id)
        .ltrim(this.key("dead"), 0, DEAD_LIMIT - 1)
        .lpush(this.key("recent"), id)
        .ltrim(this.key("recent"), 0, RECENT_LIMIT - 1);
    }
    await multi.exec();
  }

  async defer(id, runAt) {
    const job = await this.get(id);
    if (!job) return;
    Object.assign(job, { state: "delayed", runAt, attempts: job.attempts - 1 });
    await this.redis
      .multi()
      .set(this.jobKey(id), JSON.stringify(job))
      .srem(this.key("active"), id)
      .zadd(this.key("delayed"), runAt, id)
      .exec();
  }

  async retryDead(id) {
    const job = await this.get(id);
    if (!job || job.state !== "dead") return false;
    Object.assign(job, { state: "waiting", attempts: 0, lastError: null, createdAt: Date.now() });
    await this.redis
      .multi()
      .lrem(this.key("dead"), 0, id)
      .set(this.jobKey(id), JSON.stringify(job))
      .zadd(this.key("ready"), readyScore(job.priority, job.createdAt), id)
      .exec();
    return true;
  }

  // Jobs left "active" by a crashed process go back to the queue.
  // Call this when the only worker process starts up.
  async recoverActive() {
    const ids = await this.redis.smembers(this.key("active"));
    for (const id of ids) {
      const job = await this.get(id);
      const multi = this.redis.multi().srem(this.key("active"), id);
      if (job) {
        Object.assign(job, { state: "waiting", attempts: Math.max(0, job.attempts - 1) });
        multi.set(this.jobKey(id), JSON.stringify(job)).zadd(this.key("ready"), readyScore(job.priority, job.createdAt), id);
      }
      await multi.exec();
    }
    return ids.length;
  }

  async counts() {
    const [[, waiting], [, delayed], [, active], [, deadQueue], [, counters]] = await this.redis
      .multi()
      .zcard(this.key("ready"))
      .zcard(this.key("delayed"))
      .scard(this.key("active"))
      .llen(this.key("dead"))
      .hgetall(this.key("counters"))
      .exec();
    const n = (k) => Number(counters?.[k] || 0);
    return {
      waiting,
      delayed,
      active,
      deadQueue,
      enqueued: n("enqueued"),
      completed: n("completed"),
      retried: n("retried"),
      dead: n("dead"),
    };
  }

  async listFrom(listName, limit) {
    const ids = await this.redis.lrange(this.key(listName), 0, limit - 1);
    if (!ids.length) return [];
    const raws = await this.redis.mget(ids.map((id) => this.jobKey(id)));
    return raws.filter(Boolean).map((r) => JSON.parse(r));
  }

  listRecent(limit = 30) {
    return this.listFrom("recent", limit);
  }

  listDead(limit = 30) {
    return this.listFrom("dead", limit);
  }

  async clear() {
    let cursor = "0";
    do {
      const [next, keys] = await this.redis.scan(cursor, "MATCH", this.prefix + "*", "COUNT", 500);
      if (keys.length) await this.redis.del(...keys);
      cursor = next;
    } while (cursor !== "0");
  }
}

module.exports = { RedisStore };
