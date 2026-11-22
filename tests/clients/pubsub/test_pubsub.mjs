import assert from "node:assert/strict";
import { createRequire } from "node:module";
import net from "node:net";
import Redis from "ioredis";
import { Queue, Worker, QueueEvents } from "bullmq";

const require = createRequire(import.meta.url);
const ioredisPkg = require("ioredis/package.json");
const bullmqPkg = require("bullmq/package.json");

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT || "6379");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Minimal RESP2 framing helper over raw TCP so we can verify exact wire-level
 * subscription/unsubscription acknowledgements, counts, and subscribed-mode errors.
 */
function encodeRespCommand(...args) {
  const parts = [Buffer.from(`*${args.length}\r\n`)];
  for (const arg of args) {
    const buf = Buffer.isBuffer(arg) ? arg : Buffer.from(String(arg));
    parts.push(Buffer.from(`$${buf.length}\r\n`), buf, Buffer.from("\r\n"));
  }
  return Buffer.concat(parts);
}

class RawRespClient {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frames = [];
    this.waiters = [];
    socket.on("data", (chunk) => {
      this.buf = Buffer.concat([this.buf, chunk]);
      this.parseAll();
    });
  }

  static connect(host, port) {
    return new Promise((resolve, reject) => {
      const s = net.createConnection({ host, port }, () => resolve(new RawRespClient(s)));
      s.once("error", reject);
    });
  }

  parseOne(offset) {
    if (offset >= this.buf.length) return null;
    const prefix = this.buf[offset];
    const crlf = this.buf.indexOf("\r\n", offset);
    if (crlf === -1) return null;
    const line = this.buf.subarray(offset + 1, crlf).toString("utf8");
    const next = crlf + 2;

    if (prefix === 43) return { value: { type: "+", value: line }, next }; // +
    if (prefix === 45) return { value: { type: "-", value: line }, next }; // -
    if (prefix === 58) return { value: { type: ":", value: Number(line) }, next }; // :
    if (prefix === 36) {
      // $
      const len = Number(line);
      if (len === -1) return { value: null, next };
      if (next + len + 2 > this.buf.length) return null;
      const body = this.buf.subarray(next, next + len).toString("utf8");
      return { value: body, next: next + len + 2 };
    }
    if (prefix === 42) {
      // *
      const count = Number(line);
      if (count === -1) return { value: null, next };
      const arr = [];
      let cur = next;
      for (let i = 0; i < count; i++) {
        const item = this.parseOne(cur);
        if (!item) return null;
        arr.push(item.value);
        cur = item.next;
      }
      return { value: arr, next: cur };
    }
    throw new Error(`Unexpected RESP prefix: ${String.fromCharCode(prefix)}`);
  }

  parseAll() {
    while (true) {
      const parsed = this.parseOne(0);
      if (!parsed) break;
      this.buf = this.buf.subarray(parsed.next);
      if (this.waiters.length > 0) {
        const resolve = this.waiters.shift();
        resolve(parsed.value);
      } else {
        this.frames.push(parsed.value);
      }
    }
  }

  send(...args) {
    this.socket.write(encodeRespCommand(...args));
  }

  nextFrame(timeoutMs = 5000) {
    if (this.frames.length > 0) return Promise.resolve(this.frames.shift());
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("Timed out waiting for RESP frame")), timeoutMs);
      this.waiters.push((val) => {
        clearTimeout(timer);
        resolve(val);
      });
    });
  }

  close() {
    this.socket.destroy();
  }
}

function waitForMessage(client, expectedChannel, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      client.off("message", handler);
      reject(new Error(`Timed out waiting for message on ${expectedChannel}`));
    }, timeoutMs);
    const handler = (ch, msg) => {
      if (ch === expectedChannel) {
        clearTimeout(timer);
        client.off("message", handler);
        resolve({ channel: ch, message: msg });
      }
    };
    client.on("message", handler);
  });
}

function waitForMessageBuffer(client, expectedChannel, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      client.off("messageBuffer", handler);
      reject(new Error(`Timed out waiting for messageBuffer on ${expectedChannel}`));
    }, timeoutMs);
    const handler = (chBuf, msgBuf) => {
      if (chBuf.toString("utf8") === expectedChannel) {
        clearTimeout(timer);
        client.off("messageBuffer", handler);
        resolve({ channel: chBuf, payload: msgBuf });
      }
    };
    client.on("messageBuffer", handler);
  });
}

async function runTests() {
  console.log(
    `==> Pub/Sub E2E suite against ${REDIS_HOST}:${REDIS_PORT} (ioredis ${ioredisPkg.version}, bullmq ${bullmqPkg.version})`
  );
  assert.equal(ioredisPkg.version, "5.11.1", "Expected pinned ioredis 5.11.1");
  assert.equal(bullmqPkg.version, "5.13.0", "Expected pinned bullmq 5.13.0");

  // -------------------------------------------------------------------------
  // Test 1: Exact Redis wire-level acks, counts, duplicate & 0-arg UNSUBSCRIBE
  // -------------------------------------------------------------------------
  console.log("  [1/7] Wire-level SUBSCRIBE/UNSUBSCRIBE acks, counts & subscribed-mode enforcement...");
  {
    const raw = await RawRespClient.connect(REDIS_HOST, REDIS_PORT);
    try {
      // Unsubscribe with 0 active subscriptions -> ["unsubscribe", null, 0]
      raw.send("UNSUBSCRIBE");
      const emptyUnsub = await raw.nextFrame();
      assert.deepEqual(emptyUnsub, ["unsubscribe", null, { type: ":", value: 0 }]);

      // Multi-channel SUBSCRIBE -> one ack per channel with incrementing count
      raw.send("SUBSCRIBE", "raw:ch1", "raw:ch2");
      assert.deepEqual(await raw.nextFrame(), ["subscribe", "raw:ch1", { type: ":", value: 1 }]);
      assert.deepEqual(await raw.nextFrame(), ["subscribe", "raw:ch2", { type: ":", value: 2 }]);

      // Duplicate SUBSCRIBE -> ack emitted with UNCHANGED count
      raw.send("SUBSCRIBE", "raw:ch1");
      assert.deepEqual(await raw.nextFrame(), ["subscribe", "raw:ch1", { type: ":", value: 2 }]);

      // Subscribed-mode PING -> ["pong", ""] and ["pong", "hi"]
      raw.send("PING");
      assert.deepEqual(await raw.nextFrame(), ["pong", ""]);
      raw.send("PING", "hi");
      assert.deepEqual(await raw.nextFrame(), ["pong", "hi"]);

      // Subscribed-mode disallowed command -> Redis error
      raw.send("GET", "somekey");
      const errFrame = await raw.nextFrame();
      assert.equal(errFrame.type, "-");
      assert.match(errFrame.value, /Can't execute 'get'.*allowed in this context/i);

      // Unsubscribe single channel
      raw.send("UNSUBSCRIBE", "raw:ch1");
      assert.deepEqual(await raw.nextFrame(), ["unsubscribe", "raw:ch1", { type: ":", value: 1 }]);

      // Unsubscribe non-subscribed channel -> remaining stays 1
      raw.send("UNSUBSCRIBE", "raw:nonexistent");
      assert.deepEqual(await raw.nextFrame(), [
        "unsubscribe",
        "raw:nonexistent",
        { type: ":", value: 1 },
      ]);

      // Unsubscribe all (0 args) -> acks remaining channel down to 0
      raw.send("UNSUBSCRIBE");
      assert.deepEqual(await raw.nextFrame(), ["unsubscribe", "raw:ch2", { type: ":", value: 0 }]);

      // Back in normal mode -> PING returns +PONG
      raw.send("PING");
      assert.deepEqual(await raw.nextFrame(), { type: "+", value: "PONG" });
    } finally {
      raw.close();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 2: ioredis 5.11.1 Medusa workflow events, multi-sub counts, multi-ch & unsub
  // -------------------------------------------------------------------------
  console.log("  [2/7] ioredis 5.11.1 Medusa 2.22.7 workflow Pub/Sub flow & recipient counts...");
  {
    const sub1 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const sub2 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      const wfChannel = "medusa:workflow:create-order:tx_01JMEDUSA001";
      const ch2 = "medusa:workflow:fulfill-order:tx_01JMEDUSA002";
      const ch3 = "medusa:workflow:send-receipt:tx_01JMEDUSA003";

      const count1 = await sub1.subscribe(wfChannel);
      assert.equal(count1, 1);

      const medusaEvent = JSON.stringify({
        event: "workflow.step.succeeded",
        workflowId: "create-order-workflow",
        transactionId: "tx_01JMEDUSA001",
        stepId: "reserve-inventory-step",
        data: {
          orderId: "ord_01J999",
          customerEmail: "buyer@solarnest.example",
          items: [{ sku: "PANEL-450W", qty: 4 }],
          total: 129900,
          currency: "usd",
        },
        timestamp: new Date().toISOString(),
      });

      const recv1Promise = waitForMessage(sub1, wfChannel);
      const recipients1 = await pub.publish(wfChannel, medusaEvent);
      assert.equal(recipients1, 1);
      const recv1 = await recv1Promise;
      assert.equal(recv1.channel, wfChannel);
      assert.equal(recv1.message, medusaEvent);

      // Add second subscriber -> PUBLISH must return 2 and both receive exact payload
      assert.equal(await sub2.subscribe(wfChannel), 1);
      const r1 = waitForMessage(sub1, wfChannel);
      const r2 = waitForMessage(sub2, wfChannel);
      assert.equal(await pub.publish(wfChannel, medusaEvent), 2);
      assert.deepEqual(await r1, { channel: wfChannel, message: medusaEvent });
      assert.deepEqual(await r2, { channel: wfChannel, message: medusaEvent });

      // Duplicate subscribe on sub1 must not double-count or double-deliver
      assert.equal(await sub1.subscribe(wfChannel), 1);
      const dupReceived = [];
      const dupListener = (ch, msg) => dupReceived.push({ ch, msg });
      sub1.on("message", dupListener);
      assert.equal(await pub.publish(wfChannel, '{"dedup":true}'), 2);
      await sleep(100);
      sub1.off("message", dupListener);
      assert.equal(dupReceived.length, 1);
      assert.equal(dupReceived[0].msg, '{"dedup":true}');

      // Multi-channel subscribe + single-channel unsubscribe + PUBSUB inspection
      assert.equal(await sub1.subscribe(ch2, ch3), 3);
      const activeChannels = await pub.pubsub("CHANNELS", "medusa:workflow:*");
      assert.equal(activeChannels.length, 3);
      const numsub = await pub.pubsub("NUMSUB", wfChannel, ch2, "medusa:unused");
      assert.deepEqual(numsub, [wfChannel, 2, ch2, 1, "medusa:unused", 0]);

      // Unsubscribe one channel (ch2) -> count drops to 2
      assert.equal(await sub1.unsubscribe(ch2), 2);
      assert.equal(await pub.publish(ch2, "nobody-home"), 0);
      const r3 = waitForMessage(sub1, ch3);
      assert.equal(await pub.publish(ch3, '{"step":"receipt"}'), 1);
      assert.equal((await r3).message, '{"step":"receipt"}');

      // Unsubscribe all (0 args) on sub1 -> count drops to 0, normal commands resume
      assert.equal(await sub1.unsubscribe(), 0);
      assert.equal(await sub1.set("pubsub:post-unsub", "ok"), "OK");
      assert.equal(await sub1.get("pubsub:post-unsub"), "ok");

      // Only sub2 remains on wfChannel -> PUBLISH returns 1
      assert.equal(await pub.publish(wfChannel, '{"only":"sub2"}'), 1);

      // Unsubscribe sub2 -> PUBLISH returns 0
      assert.equal(await sub2.unsubscribe(), 0);
      assert.equal(await pub.publish(wfChannel, '{"nobody":true}'), 0);
    } finally {
      sub1.disconnect();
      sub2.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 3: Idle subscriber leaves BGWorker free for other clients & BullMQ 5.13.0
  // -------------------------------------------------------------------------
  console.log("  [3/7] Idle subscriber concurrency alongside normal clients & BullMQ 5.13.0...");
  {
    const idleSub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const otherClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const queueName = `pubsub-coexist-${Date.now()}`;
    const connection = { host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: null };
    const queue = new Queue(queueName, { connection });
    const queueEvents = new QueueEvents(queueName, { connection });
    let worker;

    try {
      await idleSub.subscribe("medusa:workflow:idle");
      // Verify normal Redis commands work immediately while idleSub is subscribed
      assert.equal(await otherClient.set("coexist:key", "alive"), "OK");
      assert.equal(await otherClient.get("coexist:key"), "alive");

      // Run a full BullMQ 5.13.0 job while idleSub remains subscribed & idle
      await queueEvents.waitUntilReady();
      const processed = [];
      worker = new Worker(
        queueName,
        async (job) => {
          processed.push(job.data);
          // Also publish a workflow notification from inside the worker!
          await otherClient.publish(
            "medusa:workflow:idle",
            JSON.stringify({ jobId: job.id, status: "done" })
          );
          return { completed: true };
        },
        { connection }
      );
      await worker.waitUntilReady();

      const idleMsgPromise = waitForMessage(idleSub, "medusa:workflow:idle", 8000);
      const job = await queue.add("send-order-confirmation", { orderId: "ord_coexist_1" });
      const result = await job.waitUntilFinished(queueEvents, 10000);
      assert.deepEqual(result, { completed: true });
      assert.equal(processed.length, 1);

      const idleMsg = await idleMsgPromise;
      const parsed = JSON.parse(idleMsg.message);
      assert.equal(parsed.jobId, job.id);
      assert.equal(parsed.status, "done");
    } finally {
      if (worker) await worker.close();
      await queueEvents.close();
      await queue.close();
      idleSub.disconnect();
      otherClient.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 4: Disconnect cleanup + reconnect & auto-resubscribe
  // -------------------------------------------------------------------------
  console.log("  [4/7] Abrupt disconnect cleanup & automatic reconnect/resubscribe...");
  {
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const subDrop = new Redis({ host: REDIS_HOST, port: REDIS_PORT, retryStrategy: null });
    const subReconn = new Redis({
      host: REDIS_HOST,
      port: REDIS_PORT,
      autoResubscribe: true,
      retryStrategy: () => 50,
    });
    try {
      // Abrupt socket destruction -> subscription must be cleaned up promptly
      await subDrop.subscribe("medusa:disconnect-cleanup");
      assert.equal(await pub.publish("medusa:disconnect-cleanup", "before-drop"), 1);
      subDrop.disconnect(false);
      await sleep(150);
      assert.equal(await pub.publish("medusa:disconnect-cleanup", "after-drop"), 0);

      // Reconnect & auto-resubscribe via ioredis
      await subReconn.subscribe("medusa:reconnect-ch");
      const beforePromise = waitForMessage(subReconn, "medusa:reconnect-ch");
      assert.equal(await pub.publish("medusa:reconnect-ch", "before-reconnect"), 1);
      assert.equal((await beforePromise).message, "before-reconnect");

      // Force-destroy the underlying TCP stream so ioredis reconnects and resubscribes
      const readyAgain = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Timed out waiting for reconnect ready")), 8000);
        subReconn.once("ready", () => {
          clearTimeout(timer);
          resolve();
        });
      });
      subReconn.stream.destroy();
      await readyAgain;
      // Give autoResubscribe command a moment to complete on the new socket
      await sleep(150);

      const afterPromise = waitForMessage(subReconn, "medusa:reconnect-ch");
      assert.equal(await pub.publish("medusa:reconnect-ch", "after-reconnect"), 1);
      assert.equal((await afterPromise).message, "after-reconnect");
    } finally {
      subDrop.disconnect();
      subReconn.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 5: Strict message ordering (100 sequential messages)
  // -------------------------------------------------------------------------
  console.log("  [5/7] Strict FIFO message ordering (100 messages)...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      const ch = "medusa:ordering";
      await sub.subscribe(ch);
      const total = 100;
      const received = [];
      const done = new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`Only received ${received.length}/${total} ordered messages`)),
          8000
        );
        sub.on("message", (channel, message) => {
          if (channel !== ch) return;
          received.push(Number(message));
          if (received.length === total) {
            clearTimeout(timer);
            resolve();
          }
        });
      });

      const pipe = pub.pipeline();
      for (let i = 0; i < total; i++) {
        pipe.publish(ch, String(i));
      }
      const pipeRes = await pipe.exec();
      assert.equal(pipeRes.length, total);
      for (const [err, cnt] of pipeRes) {
        assert.equal(err, null);
        assert.equal(cnt, 1);
      }

      await done;
      assert.equal(received.length, total);
      for (let i = 0; i < total; i++) {
        assert.equal(received[i], i, `Out of order at index ${i}: got ${received[i]}`);
      }
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 6: Binary-safe payloads AND large payload (> 8KB PG NOTIFY limit)
  // -------------------------------------------------------------------------
  console.log("  [6/7] Binary-safe payloads & large 32KB+ payload (> PG NOTIFY 8KB limit)...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      // Binary payload with NUL bytes, non-UTF8 octets, and RESP control characters
      const binCh = "medusa:binary";
      await sub.subscribe(binCh);
      const binaryPayload = Buffer.from([
        0x00, 0x01, 0x02, 0xff, 0xfe, 0x80, 0x0d, 0x0a, 0x24, 0x35, 0x0d, 0x0a, 0x00, 0x7f,
      ]);
      const binPromise = waitForMessageBuffer(sub, binCh);
      assert.equal(await pub.publish(binCh, binaryPayload), 1);
      const binRecv = await binPromise;
      assert.equal(Buffer.compare(binRecv.payload, binaryPayload), 0);

      // Large 32KB+ Medusa workflow JSON payload (4x PostgreSQL's 8000-byte NOTIFY limit)
      const largeCh = "medusa:large-workflow";
      await sub.subscribe(largeCh);
      const largePayload = JSON.stringify({
        workflowId: "bulk-product-import-workflow",
        transactionId: "tx_large_32kb",
        items: Array.from({ length: 400 }, (_, i) => ({
          id: `prod_${String(i).padStart(4, "0")}`,
          title: `Solar Panel Unit #${i}`,
          metadata: "x".repeat(48),
        })),
      });
      assert.ok(
        Buffer.byteLength(largePayload, "utf8") > 32 * 1024,
        `Expected >32KB payload, got ${Buffer.byteLength(largePayload, "utf8")} bytes`
      );
      const largePromise = waitForMessage(sub, largeCh);
      assert.equal(await pub.publish(largeCh, largePayload), 1);
      const largeRecv = await largePromise;
      assert.equal(largeRecv.message, largePayload);
      assert.equal(JSON.parse(largeRecv.message).items.length, 400);
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 7: PUBLISH from Lua (`redis.call('PUBLISH', ...)`) and MULTI/EXEC
  // -------------------------------------------------------------------------
  console.log("  [7/7] PUBLISH from Lua script and MULTI/EXEC transaction...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      const ch = "medusa:lua-and-multi";
      await sub.subscribe(ch);

      const luaRecvPromise = waitForMessage(sub, ch);
      const luaCount = await pub.eval(
        "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('PUBLISH', KEYS[2], ARGV[1])",
        2,
        "pubsub:lua:state",
        ch,
        '{"source":"lua"}'
      );
      assert.equal(luaCount, 1);
      assert.equal((await luaRecvPromise).message, '{"source":"lua"}');
      assert.equal(await pub.get("pubsub:lua:state"), '{"source":"lua"}');

      const multiRecvPromise = waitForMessage(sub, ch);
      const multiRes = await pub
        .multi()
        .set("pubsub:multi:state", "committed")
        .publish(ch, '{"source":"multi"}')
        .exec();
      assert.equal(multiRes[0][1], "OK");
      assert.equal(multiRes[1][1], 1);
      assert.equal((await multiRecvPromise).message, '{"source":"multi"}');
    } finally {
      sub.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  console.log("==> ALL 7 PUB/SUB INTEGRATION TESTS PASSED!");
}

runTests().catch((err) => {
  console.error("PUB/SUB TEST FAILED:", err);
  process.exit(1);
});
