import assert from "node:assert/strict";
import { createRequire } from "node:module";
import net from "node:net";
import Redis from "ioredis";
import { Queue, Worker, QueueEvents, FlowProducer } from "bullmq";

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

function waitReady(client, timeoutMs = 5000) {
  if (client.status === "ready") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      client.off("ready", onReady);
      client.off("error", onError);
      reject(new Error(`Timed out waiting for ioredis ready (status=${client.status})`));
    }, timeoutMs);
    const onReady = () => {
      clearTimeout(timer);
      client.off("error", onError);
      resolve();
    };
    const onError = (err) => {
      clearTimeout(timer);
      client.off("ready", onReady);
      reject(err);
    };
    client.once("ready", onReady);
    client.once("error", onError);
  });
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
  console.log("  [1/10] Wire-level SUBSCRIBE/UNSUBSCRIBE acks, counts & subscribed-mode enforcement...");
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
  console.log("  [2/10] ioredis 5.11.1 Medusa 2.22.7 workflow Pub/Sub flow & recipient counts...");
  {
    const sub1 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const sub2 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await Promise.all([waitReady(sub1), waitReady(sub2), waitReady(pub)]);
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
  console.log("  [3/10] Idle subscriber concurrency alongside normal clients & BullMQ 5.13.0...");
  {
    const idleSub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const otherClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const queueName = `pubsub-coexist-${Date.now()}`;
    const connection = { host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: null };
    const queue = new Queue(queueName, { connection });
    const queueEvents = new QueueEvents(queueName, { connection });
    let worker;

    try {
      await Promise.all([waitReady(idleSub), waitReady(otherClient)]);
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
  console.log("  [4/10] Abrupt disconnect cleanup & automatic reconnect/resubscribe...");
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
      await Promise.all([waitReady(pub), waitReady(subDrop), waitReady(subReconn)]);
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
  console.log("  [5/10] Strict FIFO message ordering (100 messages)...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await Promise.all([waitReady(sub), waitReady(pub)]);
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
  console.log("  [6/10] Binary-safe payloads & large 32KB+ payload (> PG NOTIFY 8KB limit)...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await Promise.all([waitReady(sub), waitReady(pub)]);
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
  console.log("  [7/10] PUBLISH from Lua script and MULTI/EXEC transaction...");
  {
    const sub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await Promise.all([waitReady(sub), waitReady(pub)]);
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

  // -------------------------------------------------------------------------
  // Test 8: Pattern Pub/Sub (PSUBSCRIBE / PUNSUBSCRIBE / pmessage / NUMPAT)
  // -------------------------------------------------------------------------
  console.log("  [8/10] Pattern Pub/Sub (PSUBSCRIBE, PUNSUBSCRIBE, pmessage, PUBSUB NUMPAT)...");
  {
    const psub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const pub = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const raw = await RawRespClient.connect(REDIS_HOST, REDIS_PORT);
    try {
      await Promise.all([waitReady(psub), waitReady(pub)]);
      // Wire-level PSUBSCRIBE + mixed SUBSCRIBE count + idempotent duplicate
      raw.send("PSUBSCRIBE", "orders.*", "orders.*", "billing:*");
      assert.deepEqual(await raw.nextFrame(), ["psubscribe", "orders.*", { type: ":", value: 1 }]);
      assert.deepEqual(await raw.nextFrame(), ["psubscribe", "orders.*", { type: ":", value: 1 }]);
      assert.deepEqual(await raw.nextFrame(), ["psubscribe", "billing:*", { type: ":", value: 2 }]);

      raw.send("SUBSCRIBE", "orders.created");
      assert.deepEqual(await raw.nextFrame(), ["subscribe", "orders.created", { type: ":", value: 3 }]);

      // ioredis psubscribe alongside raw pattern subscriber
      const pCount = await psub.psubscribe("orders.*");
      assert.equal(pCount, 1);

      const numpat = await pub.pubsub("NUMPAT");
      assert.equal(numpat, 3); // raw (2 patterns) + psub (1 pattern)

      const pmsgPromise = new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("Timed out waiting for pmessage")), 5000);
        psub.once("pmessage", (pattern, channel, message) => {
          clearTimeout(timer);
          resolve({ pattern, channel, message });
        });
      });

      // Publish matches raw's channel + raw's pattern + psub's pattern = 3 deliveries
      const delivered = await pub.publish("orders.created", '{"id":"ord_99"}');
      assert.equal(delivered, 3);

      const pmsg = await pmsgPromise;
      assert.deepEqual(pmsg, {
        pattern: "orders.*",
        channel: "orders.created",
        message: '{"id":"ord_99"}',
      });

      const rawFrames = [await raw.nextFrame(), await raw.nextFrame()];
      assert.ok(
        rawFrames.some((f) => f[0] === "message" && f[1] === "orders.created" && f[2] === '{"id":"ord_99"}')
      );
      assert.ok(
        rawFrames.some(
          (f) => f[0] === "pmessage" && f[1] === "orders.*" && f[2] === "orders.created" && f[3] === '{"id":"ord_99"}'
        )
      );

      // Zero-arg UNSUBSCRIBE leaves pattern count intact, then zero-arg PUNSUBSCRIBE drops to 0
      raw.send("UNSUBSCRIBE");
      assert.deepEqual(await raw.nextFrame(), ["unsubscribe", "orders.created", { type: ":", value: 2 }]);
      raw.send("PUNSUBSCRIBE");
      assert.deepEqual(await raw.nextFrame(), ["punsubscribe", "orders.*", { type: ":", value: 1 }]);
      assert.deepEqual(await raw.nextFrame(), ["punsubscribe", "billing:*", { type: ":", value: 0 }]);

      // Normal command works again after leaving subscribed mode
      raw.send("PING");
      assert.deepEqual(await raw.nextFrame(), { type: "+", value: "PONG" });

      await psub.punsubscribe("orders.*");
      assert.equal(await pub.pubsub("NUMPAT"), 0);
    } finally {
      raw.close();
      psub.disconnect();
      pub.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 9: Universal Key TTL (Hash/List/Set/ZSet/Stream), NX/XX/GT/LT & GETEX
  // -------------------------------------------------------------------------
  console.log("  [9/10] Universal TTL across all 6 data types, NX/XX/GT/LT flags & GETEX...");
  {
    const r = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await r.del("ttl:h", "ttl:h2", "ttl:l", "ttl:s", "ttl:z", "ttl:x", "ttl:str");

      await r.hset("ttl:h", "f1", "v1", "f2", "v2");
      await r.rpush("ttl:l", "a", "b");
      await r.sadd("ttl:s", "m1", "m2");
      await r.zadd("ttl:z", 10, "z1", 20, "z2");
      await r.xadd("ttl:x", "*", "ev", "created");

      // Set TTL on every non-string type
      for (const k of ["ttl:h", "ttl:l", "ttl:s", "ttl:z", "ttl:x"]) {
        assert.equal(await r.pexpire(k, 300), 1, `PEXPIRE should succeed on ${k}`);
        const pttl = await r.pttl(k);
        assert.ok(pttl > 0 && pttl <= 350, `Unexpected PTTL ${pttl} on ${k}`);
        const pexptime = await r.call("PEXPIRETIME", k);
        assert.ok(Number(pexptime) > Date.now() - 1000);
      }

      // Wait for all 5 non-string keys to expire lazily
      await sleep(380);
      assert.equal(await r.exists("ttl:h", "ttl:l", "ttl:s", "ttl:z", "ttl:x"), 0);
      assert.deepEqual(await r.hgetall("ttl:h"), {});
      assert.deepEqual(await r.lrange("ttl:l", 0, -1), []);
      assert.deepEqual(await r.smembers("ttl:s"), []);
      assert.deepEqual(await r.zrange("ttl:z", 0, -1), []);
      assert.deepEqual(await r.xrange("ttl:x", "-", "+"), []);
      assert.equal(await r.ttl("ttl:h"), -2);
      assert.equal(await r.type("ttl:h"), "none");

      // Re-creating expired hash starts persistent (TTL = -1)
      await r.hset("ttl:h", "fresh", "1");
      assert.equal(await r.ttl("ttl:h"), -1);

      // EXPIRE NX / XX / GT / LT flags on Hash
      assert.equal(await r.call("EXPIRE", "ttl:h", "60", "XX"), 0);
      assert.equal(await r.call("EXPIRE", "ttl:h", "60", "GT"), 0); // infinity > 60
      assert.equal(await r.call("EXPIRE", "ttl:h", "60", "NX"), 1);
      assert.equal(await r.call("EXPIRE", "ttl:h", "30", "NX"), 0);
      assert.equal(await r.call("EXPIRE", "ttl:h", "30", "GT"), 0);
      assert.equal(await r.call("EXPIRE", "ttl:h", "120", "GT"), 1);
      assert.equal(await r.call("EXPIRE", "ttl:h", "200", "LT"), 0);
      assert.equal(await r.call("EXPIRE", "ttl:h", "50", "LT"), 1);

      // RENAME preserves Hash TTL
      await r.rename("ttl:h", "ttl:h2");
      const renamedTtl = await r.ttl("ttl:h2");
      assert.ok(renamedTtl > 0 && renamedTtl <= 50, `Expected preserved TTL, got ${renamedTtl}`);

      // Emptying a hash clears its expiry metadata
      await r.hdel("ttl:h2", "fresh");
      await r.hset("ttl:h2", "reborn", "yes");
      assert.equal(await r.ttl("ttl:h2"), -1);

      // PERSIST on hash
      await r.expire("ttl:h2", 60);
      assert.equal(await r.persist("ttl:h2"), 1);
      assert.equal(await r.ttl("ttl:h2"), -1);

      // GETEX (rolling session TTL)
      await r.set("ttl:str", "session_abc");
      assert.equal(await r.call("GETEX", "ttl:str", "EX", "45"), "session_abc");
      const strTtl = await r.ttl("ttl:str");
      assert.ok(strTtl > 0 && strTtl <= 45);
      assert.equal(await r.call("GETEX", "ttl:str", "PERSIST"), "session_abc");
      assert.equal(await r.ttl("ttl:str"), -1);
    } finally {
      r.disconnect();
    }
  }
  console.log("        PASS");

  // -------------------------------------------------------------------------
  // Test 10: HSCAN/SSCAN/ZSCAN, SCAN TYPE, CLIENT LIST/KILL, Lua Sandbox & BullMQ FlowProducer
  // -------------------------------------------------------------------------
  console.log("  [10/10] HSCAN/SSCAN/ZSCAN, SCAN TYPE, CLIENT LIST/KILL, Lua Sandbox & BullMQ Flow...");
  {
    const r1 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    const r2 = new Redis({ host: REDIS_HOST, port: REDIS_PORT });
    try {
      await r1.flushdb();

      // Lua sandbox: dangerous stdlibs & file loaders must be nil
      const sandboxCheck = JSON.parse(
        await r1.eval(
          "return cjson.encode({os=type(os),io=type(io),pkg=type(package),dbg=type(debug),dofile=type(dofile),loadfile=type(loadfile),req=type(require)})",
          0
        )
      );
      assert.deepEqual(sandboxCheck, {
        os: "nil",
        io: "nil",
        pkg: "nil",
        dbg: "nil",
        dofile: "nil",
        loadfile: "nil",
        req: "nil",
      });

      // HSCAN pagination + MATCH + NOVALUES
      for (let i = 0; i < 15; i++) {
        await r1.hset("scan:hash", `field_${String(i).padStart(2, "0")}`, `val_${i}`);
      }
      let cursor = "0";
      const hEntries = {};
      do {
        const [next, items] = await r1.hscan("scan:hash", cursor, "MATCH", "field_*", "COUNT", 4);
        cursor = next;
        for (let i = 0; i < items.length; i += 2) {
          hEntries[items[i]] = items[i + 1];
        }
      } while (cursor !== "0");
      assert.equal(Object.keys(hEntries).length, 15);
      assert.equal(hEntries.field_07, "val_7");

      const [, noValFields] = await r1.call("HSCAN", "scan:hash", "0", "COUNT", "50", "NOVALUES");
      assert.equal(noValFields.length, 15);

      // SSCAN pagination
      for (let i = 0; i < 12; i++) {
        await r1.sadd("scan:set", `member_${String(i).padStart(2, "0")}`);
      }
      cursor = "0";
      const sMembers = [];
      do {
        const [next, items] = await r1.sscan("scan:set", cursor, "MATCH", "member_*", "COUNT", 5);
        cursor = next;
        sMembers.push(...items);
      } while (cursor !== "0");
      assert.equal(sMembers.length, 12);

      // ZSCAN pagination
      for (let i = 0; i < 10; i++) {
        await r1.zadd("scan:zset", i * 1.5, `zm_${i}`);
      }
      cursor = "0";
      const zMap = {};
      do {
        const [next, items] = await r1.zscan("scan:zset", cursor, "MATCH", "zm_*", "COUNT", 3);
        cursor = next;
        for (let i = 0; i < items.length; i += 2) {
          zMap[items[i]] = Number(items[i + 1]);
        }
      } while (cursor !== "0");
      assert.equal(Object.keys(zMap).length, 10);
      assert.equal(zMap.zm_4, 6);

      // SCAN ... TYPE filter
      await r1.set("scan:str", "ok");
      const [, onlyHashes] = await r1.scan("0", "MATCH", "scan:*", "COUNT", 100, "TYPE", "hash");
      assert.deepEqual(onlyHashes, ["scan:hash"]);
      const [, onlySets] = await r1.scan("0", "MATCH", "scan:*", "COUNT", 100, "TYPE", "set");
      assert.deepEqual(onlySets, ["scan:set"]);
      const [, onlyZsets] = await r1.scan("0", "MATCH", "scan:*", "COUNT", 100, "TYPE", "zset");
      assert.deepEqual(onlyZsets, ["scan:zset"]);

      // Multi-connection CLIENT LIST, INFO clients & CLIENT KILL
      await r1.client("SETNAME", "medusa-api-1");
      await r2.client("SETNAME", "medusa-worker-2");
      const r2Id = await r2.client("ID");
      const clientList = await r1.client("LIST");
      assert.ok(clientList.includes("name=medusa-api-1"), `Missing r1 in CLIENT LIST: ${clientList}`);
      assert.ok(clientList.includes("name=medusa-worker-2"), `Missing r2 in CLIENT LIST: ${clientList}`);
      const info = await r1.info();
      assert.match(info, /connected_clients:[2-9]\d*/);

      const killed = await r1.call("CLIENT", "KILL", "ID", String(r2Id));
      assert.equal(killed, 1);

      // Real BullMQ FlowProducer + job.getDependencies() (exercises paginate-1.lua SSCAN/HSCAN)
      // + queue.getWorkers() (exercises multi-client CLIENT LIST!)
      const flowQueueName = "v040-flow-q";
      const bullConn = { host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: null };
      const flowQueue = new Queue(flowQueueName, { connection: bullConn });
      const flowProducer = new FlowProducer({ connection: bullConn });
      const flowWorker = new Worker(
        flowQueueName,
        async (job) => ({ ok: true, name: job.name }),
        { connection: bullConn, name: "v040-worker" }
      );
      try {
        await flowWorker.waitUntilReady();
        const workers = await flowQueue.getWorkers();
        assert.ok(
          workers.length >= 1,
          `Expected queue.getWorkers() >= 1 via CLIENT LIST, got ${JSON.stringify(workers)}`
        );

        const tree = await flowProducer.add({
          name: "parent-order",
          queueName: flowQueueName,
          data: { orderId: "ord_flow_1" },
          children: [
            { name: "child-inventory", queueName: flowQueueName, data: { step: 1 } },
            { name: "child-invoice", queueName: flowQueueName, data: { step: 2 } },
          ],
        });

        // Poll until parent finishes (children complete first)
        for (let i = 0; i < 40; i++) {
          const state = await tree.job.getState();
          if (state === "completed") break;
          await sleep(100);
        }
        assert.equal(await tree.job.getState(), "completed");

        // getDependencies() invokes BullMQ paginate-1.lua (HSCAN/SSCAN)!
        const deps = await tree.job.getDependencies();
        assert.equal(Object.keys(deps.processed || {}).length, 2);
      } finally {
        await flowWorker.close();
        await flowProducer.close();
        await flowQueue.close();
      }
    } finally {
      r1.disconnect();
      r2.disconnect();
    }
  }
  console.log("        PASS");

  console.log("==> ALL 10 PUB/SUB & V0.4.0 INTEGRATION TESTS PASSED!");
}

runTests().catch((err) => {
  console.error("PUB/SUB TEST FAILED:", err);
  process.exit(1);
});
