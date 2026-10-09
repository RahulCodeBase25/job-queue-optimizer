const { JobQueue } = require("./queue");
const { jobTypes } = require("./handlers");
const { MemoryStore } = require("./stores/memoryStore");
const { RedisStore } = require("./stores/redisStore");
const { createApp } = require("./app");

const port = Number(process.env.PORT) || 3000;
const concurrency = Number(process.env.CONCURRENCY) || 4;

async function main() {
  // Redis when REDIS_URL is set, otherwise everything stays in memory
  const store = process.env.REDIS_URL ? new RedisStore(process.env.REDIS_URL) : new MemoryStore();
  const storeKind = process.env.REDIS_URL ? "redis" : "memory";
  await store.connect();

  const recovered = await store.recoverActive();
  if (recovered) console.log(`Recovered ${recovered} job(s) that were running when the last process stopped`);

  const queue = new JobQueue({ store, jobTypes, concurrency });
  queue.on("error", (err) => console.error("Worker error:", err.message));
  queue.start();

  const app = createApp({ queue, storeKind });
  const server = app.listen(port, () => {
    console.log(`Job Queue Optimizer running on port ${port} (${storeKind} store, ${concurrency} workers)`);
  });

  // Finish the jobs in hand before exiting, so nothing is left half-done
  const shutdown = async () => {
    console.log("Shutting down: finishing active jobs...");
    server.close();
    await queue.stop();
    await store.close();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  console.error("Failed to start:", err.message);
  process.exit(1);
});
