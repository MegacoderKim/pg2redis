import assert from "node:assert/strict";
import Redis from "ioredis";
import { Queue, Worker, QueueEvents } from "bullmq";

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = Number(process.env.REDIS_PORT || "6379");

const connection = {
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function testLowLevelLuaStreamsAndBzpopmin() {
  console.log("==> [1/5] Testing Lua 5.1 (EVAL/EVALSHA/SCRIPT LOAD/cjson/cmsgpack) + XADD + BZPOPMIN...");
  const r = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
  const blocker = new Redis({ host: REDIS_HOST, port: REDIS_PORT });

  try {
    await r.flushdb();

    // SCRIPT LOAD + EVALSHA + cjson + cmsgpack + XADD
    const script = `
      local id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', '100', '*', 'event', ARGV[1], 'orderId', ARGV[2])
      local packed = cmsgpack.pack({event = ARGV[1], id = id})
      local unpacked = cmsgpack.unpack(packed)
      return cjson.encode({streamId = unpacked['id'], len = redis.call('XLEN', KEYS[1])})
    `;
    const sha = await r.script("LOAD", script);
    assert.equal(sha.length, 40, "SCRIPT LOAD should return 40-char SHA1");
    assert.deepEqual(await r.script("EXISTS", sha, "0".repeat(40)), [1, 0]);

    const rawJson = await r.evalsha(sha, 1, "test:events", "order.placed", "ord_01");
    const parsed = JSON.parse(rawJson);
    assert.equal(parsed.len, 1);
    assert.match(parsed.streamId, /^\d+-\d+$/);

    // Concurrent BZPOPMIN wake-up test
    const bzPromise = blocker.bzpopmin("test:marker", 5);
    await sleep(150);
    await r.zadd("test:marker", 10, "wake_me_up");
    const popped = await bzPromise;
    assert.deepEqual(popped, ["test:marker", "wake_me_up", "10"]);

    console.log("    OK: Lua 5.1 + cjson + cmsgpack + XADD + BZPOPMIN verified");
  } finally {
    await r.quit();
    await blocker.quit();
  }
}

async function testBullMQMedusaWorkflows() {
  const queueName = "medusa-events";
  const admin = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
  await admin.flushdb();

  const queue = new Queue(queueName, { connection });
  const queueEvents = new QueueEvents(queueName, { connection });

  const processed = [];
  const observedEvents = [];
  let retryAttemptsSeen = 0;

  queueEvents.on("completed", ({ jobId, returnvalue }) => {
    observedEvents.push({ type: "completed", jobId, returnvalue });
  });

  // Start an IDLE Worker FIRST (verifies BZPOPMIN blocking wait when queue is empty!)
  const worker = new Worker(
    queueName,
    async (job) => {
      if (job.name === "email.flaky_webhook") {
        retryAttemptsSeen += 1;
        if (retryAttemptsSeen === 1) {
          throw new Error("Simulated transient SMTP error");
        }
      }
      processed.push({ id: job.id, name: job.name, data: job.data });
      return { delivered: true, jobName: job.name };
    },
    { connection, concurrency: 2 }
  );

  try {
    await Promise.all([queueEvents.waitUntilReady(), worker.waitUntilReady()]);
    // Let worker enter idle BZPOPMIN wait on bull:medusa-events:marker
    await sleep(300);

    console.log("==> [2/5] Enqueuing Medusa order confirmation & password reset jobs...");
    const orderJob = await queue.add("order.placed", {
      orderId: "order_01J9XYZ",
      email: "buyer@solarnest.example",
      total: 14900,
    });
    const resetJob = await queue.add(
      "auth.password_reset",
      {
        userId: "usr_42",
        email: "user@solarnest.example",
        token: "rst_secret_token",
      },
      { priority: 1 }
    );

    const orderResult = await orderJob.waitUntilFinished(queueEvents, 10000);
    const resetResult = await resetJob.waitUntilFinished(queueEvents, 10000);
    assert.deepEqual(orderResult, { delivered: true, jobName: "order.placed" });
    assert.deepEqual(resetResult, { delivered: true, jobName: "auth.password_reset" });
    console.log("    OK: Order confirmation and password reset processed via idle Worker + QueueEvents");

    console.log("==> [3/5] Testing BullMQ delayed job (addDelayedJob Lua + promotion)...");
    const t0 = Date.now();
    const delayedJob = await queue.add(
      "order.followup_email",
      { orderId: "order_01J9XYZ" },
      { delay: 800 }
    );
    const delayedResult = await delayedJob.waitUntilFinished(queueEvents, 10000);
    const elapsed = Date.now() - t0;
    assert.deepEqual(delayedResult, { delivered: true, jobName: "order.followup_email" });
    assert.ok(elapsed >= 700, `Delayed job ran too early (${elapsed}ms)`);
    console.log(`    OK: Delayed job executed after ${elapsed}ms`);

    console.log("==> [4/5] Testing BullMQ automatic retry on failure (moveToFinished/retryJob)...");
    const retryJob = await queue.add(
      "email.flaky_webhook",
      { orderId: "order_01J9XYZ" },
      { attempts: 2, backoff: { type: "fixed", delay: 300 } }
    );
    const retryResult = await retryJob.waitUntilFinished(queueEvents, 10000);
    assert.deepEqual(retryResult, { delivered: true, jobName: "email.flaky_webhook" });
    assert.equal(retryAttemptsSeen, 2, "Expected job to fail once and succeed on attempt 2");
    console.log("    OK: Failed job retried and completed on attempt 2");

    console.log("==> [5/5] Verifying BullMQ event stream entries & queue counts...");
    const counts = await queue.getJobCounts("completed", "failed", "waiting", "active");
    assert.equal(counts.completed, 4, `Expected 4 completed jobs, got ${JSON.stringify(counts)}`);
    assert.equal(counts.failed, 0);

    const streamLen = await admin.xlen(`bull:${queueName}:events`);
    assert.ok(streamLen >= 4, `Expected >=4 stream events, got ${streamLen}`);
    assert.ok(observedEvents.length >= 4, `Expected >=4 QueueEvents callbacks, got ${observedEvents.length}`);
    console.log(`    OK: ${counts.completed} completed jobs, ${streamLen} stream events persisted in PostgreSQL`);
  } finally {
    await worker.close();
    await queueEvents.close();
    await queue.close();
    await admin.quit();
  }
}

await testLowLevelLuaStreamsAndBzpopmin();
await testBullMQMedusaWorkflows();
console.log("PASS: All BullMQ 5.13.0 (Medusa) + Lua 5.1 + Streams + BZPOPMIN tests succeeded!");
