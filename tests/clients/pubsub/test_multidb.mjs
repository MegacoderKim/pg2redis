import assert from "node:assert/strict";
import Redis from "ioredis";

const HOST = process.env.REDIS_HOST || "pg-redis";
const PORT = Number(process.env.REDIS_PORT || "6379");

function waitReady(client) {
  if (client.status === "ready") return Promise.resolve();
  return new Promise((resolve, reject) => {
    const onReady = () => {
      client.off("error", onErr);
      resolve();
    };
    const onErr = (err) => {
      client.off("ready", onReady);
      reject(err);
    };
    client.once("ready", onReady);
    client.once("error", onErr);
  });
}

async function main() {
  console.log("=== v0.6.0 Multi-Database (SELECT 0..15) E2E Suite ===");

  const db0 = new Redis({ host: HOST, port: PORT, db: 0, maxRetriesPerRequest: 2 });
  const db1 = new Redis({ host: HOST, port: PORT, db: 1, maxRetriesPerRequest: 2 });
  const db2 = new Redis({ host: HOST, port: PORT, db: 2, maxRetriesPerRequest: 2 });
  const db15 = new Redis({ host: HOST, port: PORT, db: 15, maxRetriesPerRequest: 2 });

  await Promise.all([waitReady(db0), waitReady(db1), waitReady(db2), waitReady(db15)]);

  // Clean slate across all DBs
  await db0.flushall();

  // 1. CONFIG GET databases & INFO databases
  const cfg = await db0.config("GET", "databases");
  assert.equal(cfg[1], "16");
  const info = await db0.info();
  assert.match(info, /databases:16/);

  // Out-of-range SELECT rejected cleanly
  await assert.rejects(() => db0.select(16), /out of range/i);
  await assert.rejects(() => db0.select(-1), /out of range/i);

  // CLIENT INFO reflects selected db
  const cinfo1 = await db1.call("CLIENT", "INFO");
  assert.match(String(cinfo1), /\bdb=1\b/);
  const cinfo15 = await db15.call("CLIENT", "INFO");
  assert.match(String(cinfo15), /\bdb=15\b/);
  console.log("  [PASS] 1. SELECT 0..15, CONFIG GET databases, and CLIENT INFO db=N");

  // 2. Full data isolation across all 6 data types + L1 cache
  await db0.set("shared:str", "from_db0");
  await db1.set("shared:str", "from_db1");
  await db15.set("shared:str", "from_db15");
  // Read twice to exercise L1 hot-key cache hits on identical key names in different DBs
  assert.equal(await db0.get("shared:str"), "from_db0");
  assert.equal(await db0.get("shared:str"), "from_db0");
  assert.equal(await db1.get("shared:str"), "from_db1");
  assert.equal(await db1.get("shared:str"), "from_db1");
  assert.equal(await db15.get("shared:str"), "from_db15");

  await db0.hset("shared:hash", "env", "db0");
  await db1.hset("shared:hash", "env", "db1");
  assert.equal(await db0.hget("shared:hash", "env"), "db0");
  assert.equal(await db1.hget("shared:hash", "env"), "db1");

  await db0.rpush("shared:list", "a0");
  await db1.rpush("shared:list", "a1", "b1");
  assert.deepEqual(await db0.lrange("shared:list", 0, -1), ["a0"]);
  assert.deepEqual(await db1.lrange("shared:list", 0, -1), ["a1", "b1"]);

  await db0.zadd("shared:zset", 1, "m0");
  await db1.zadd("shared:zset", 2, "m1");
  assert.deepEqual(await db0.zrange("shared:zset", 0, -1), ["m0"]);
  assert.deepEqual(await db1.zrange("shared:zset", 0, -1), ["m1"]);

  await db0.sadd("shared:set", "s0");
  await db1.sadd("shared:set", "s1");
  assert.deepEqual(await db0.smembers("shared:set"), ["s0"]);
  assert.deepEqual(await db1.smembers("shared:set"), ["s1"]);

  await db0.xadd("shared:stream", "*", "src", "db0");
  await db1.xadd("shared:stream", "*", "src", "db1");
  assert.equal(await db0.xlen("shared:stream"), 1);
  assert.equal(await db1.xlen("shared:stream"), 1);
  const xr0 = await db0.xrange("shared:stream", "-", "+");
  const xr1 = await db1.xrange("shared:stream", "-", "+");
  assert.deepEqual(xr0[0][1], ["src", "db0"]);
  assert.deepEqual(xr1[0][1], ["src", "db1"]);
  console.log("  [PASS] 2. All 6 data types + L1 cache isolated across DB 0, 1, 15");

  // 3. Universal TTL isolation: expiring shared:hash in DB 1 must NOT expire DB 0
  await db1.pexpire("shared:hash", 150);
  await new Promise((r) => setTimeout(r, 250));
  assert.equal(await db1.exists("shared:hash"), 0);
  assert.equal(await db0.hget("shared:hash", "env"), "db0");

  // DBSIZE, KEYS, SCAN scoped per DB
  assert.equal(await db0.dbsize(), 6);
  assert.equal(await db1.dbsize(), 5);
  assert.equal(await db15.dbsize(), 1);
  assert.deepEqual(await db15.keys("*"), ["shared:str"]);
  console.log("  [PASS] 3. Universal TTL, DBSIZE, KEYS scoped per DB");

  // 4. Blocking KeyNotifier isolation: ZADD in DB 0 must NOT wake BZPOPMIN in DB 1
  const blockerDb1 = new Redis({ host: HOST, port: PORT, db: 1, maxRetriesPerRequest: null });
  await waitReady(blockerDb1);
  const popPromise = blockerDb1.bzpopmin("blocking:q", 3);
  await new Promise((r) => setTimeout(r, 150));
  await db0.zadd("blocking:q", 1, "wrong_db_job");
  await new Promise((r) => setTimeout(r, 200));
  await db1.zadd("blocking:q", 5, "right_db_job");
  const popped = await popPromise;
  assert.deepEqual(popped, ["blocking:q", "right_db_job", "5"]);
  // Verify DB 0 still holds its untouched item
  assert.deepEqual(await db0.zrange("blocking:q", 0, -1), ["wrong_db_job"]);
  await blockerDb1.quit();
  console.log("  [PASS] 4. Blocking KeyNotifier isolated per DB");

  // 5. Lua 5.1 scripting runs in connection's selected DB
  assert.equal(
    await db0.eval("return redis.call('GET', KEYS[1])", 1, "shared:str"),
    "from_db0"
  );
  assert.equal(
    await db1.eval("return redis.call('GET', KEYS[1])", 1, "shared:str"),
    "from_db1"
  );
  console.log("  [PASS] 5. Lua 5.1 EVAL respects connection DB");

  // 6. Pub/Sub remains global across DBs (matching real Redis)
  const subDb1 = new Redis({ host: HOST, port: PORT, db: 1, maxRetriesPerRequest: 2 });
  await waitReady(subDb1);
  await subDb1.subscribe("global:ch");
  const msgPromise = new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("Pub/Sub cross-DB timeout")), 5000);
    subDb1.once("message", (ch, msg) => {
      clearTimeout(t);
      resolve({ ch, msg });
    });
  });
  assert.equal(await db0.publish("global:ch", "hello_across_dbs"), 1);
  assert.deepEqual(await msgPromise, { ch: "global:ch", msg: "hello_across_dbs" });
  await subDb1.quit();
  console.log("  [PASS] 6. Pub/Sub is global across DBs");

  // 7. MOVE & SWAPDB
  await db1.set("movable:str", "move_me", "EX", 120);
  assert.equal(await db1.move("movable:str", 2), 1);
  assert.equal(await db1.get("movable:str"), null);
  assert.equal(await db2.get("movable:str"), "move_me");
  assert.ok((await db2.ttl("movable:str")) > 0);
  // MOVE does not overwrite existing key in destination DB
  await db1.set("movable:str", "do_not_overwrite");
  assert.equal(await db1.move("movable:str", 2), 0);
  assert.equal(await db2.get("movable:str"), "move_me");

  await db1.swapdb(1, 2);
  assert.equal(await db1.get("movable:str"), "move_me");
  assert.equal(await db2.get("movable:str"), "do_not_overwrite");
  console.log("  [PASS] 7. MOVE (with TTL preservation & no-overwrite) and SWAPDB");

  // 8. FLUSHDB clears ONLY selected DB; FLUSHALL clears all DBs
  await db1.flushdb();
  assert.equal(await db1.dbsize(), 0);
  assert.equal(await db0.get("shared:str"), "from_db0");
  assert.equal(await db15.get("shared:str"), "from_db15");

  await db0.flushall();
  assert.equal(await db0.dbsize(), 0);
  assert.equal(await db2.dbsize(), 0);
  assert.equal(await db15.dbsize(), 0);
  console.log("  [PASS] 8. FLUSHDB (current DB only) vs FLUSHALL (all DBs)");

  await Promise.all([db0.quit(), db1.quit(), db2.quit(), db15.quit()]);
  console.log("ALL v0.6.0 MULTI-DATABASE TESTS PASSED!");
}

main().catch((err) => {
  console.error("MULTI-DB TEST FAILED:", err);
  process.exit(1);
});
