import assert from "node:assert/strict";
import net from "node:net";
import Redis from "ioredis";

const REDIS_HOST = process.env.REDIS_HOST || "127.0.0.1";
const REDIS_PORT = Number(process.env.REDIS_PORT || "6379");
const REDIS_PASS = process.env.REDIS_PASS || "s3cr3t_v040";

function encodeResp(...args) {
  const parts = [Buffer.from(`*${args.length}\r\n`)];
  for (const arg of args) {
    const buf = Buffer.isBuffer(arg) ? arg : Buffer.from(String(arg));
    parts.push(Buffer.from(`$${buf.length}\r\n`), buf, Buffer.from("\r\n"));
  }
  return Buffer.concat(parts);
}

function sendRawAndReadLine(payload, timeoutMs = 4000) {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ host: REDIS_HOST, port: REDIS_PORT });
    let data = "";
    const timer = setTimeout(() => {
      sock.destroy();
      reject(new Error(`Timeout waiting for raw reply; got so far: ${JSON.stringify(data)}`));
    }, timeoutMs);

    sock.on("connect", () => {
      if (payload) sock.write(payload);
    });
    sock.on("data", (chunk) => {
      data += chunk.toString("utf8");
      if (data.includes("\r\n")) {
        clearTimeout(timer);
        sock.destroy();
        resolve(data);
      }
    });
    sock.on("error", (err) => {
      clearTimeout(timer);
      reject(err);
    });
  });
}

async function runSecurityTests() {
  console.log(`==> v0.4.0 Security & Limits suite against ${REDIS_HOST}:${REDIS_PORT}`);

  // 1. Unauthenticated commands must fail with NOAUTH
  console.log("  [1/5] Enforcing NOAUTH on unauthenticated commands & HELLO without AUTH...");
  {
    const pingReply = await sendRawAndReadLine(encodeResp("PING"));
    assert.ok(pingReply.startsWith("-NOAUTH"), `Expected NOAUTH, got ${pingReply}`);

    const getReply = await sendRawAndReadLine(encodeResp("GET", "foo"));
    assert.ok(getReply.startsWith("-NOAUTH"), `Expected NOAUTH, got ${getReply}`);

    const blpopReply = await sendRawAndReadLine(encodeResp("BLPOP", "q", "1"));
    assert.ok(blpopReply.startsWith("-NOAUTH"), `Expected NOAUTH on BLPOP, got ${blpopReply}`);

    const helloReply = await sendRawAndReadLine(encodeResp("HELLO", "3"));
    assert.ok(helloReply.startsWith("-NOAUTH"), `Expected NOAUTH on HELLO 3, got ${helloReply}`);
  }
  console.log("        PASS");

  // 2. Wrong password returns WRONGPASS; valid AUTH / HELLO AUTH succeeds; RESET clears auth
  console.log("  [2/5] Enforcing WRONGPASS, valid AUTH, HELLO 3 AUTH, and RESET de-auth...");
  {
    const bad1 = await sendRawAndReadLine(encodeResp("AUTH", "wrongpass"));
    assert.ok(bad1.startsWith("-WRONGPASS"), `Expected WRONGPASS, got ${bad1}`);

    const bad2 = await sendRawAndReadLine(encodeResp("AUTH", "default", "wrongpass"));
    assert.ok(bad2.startsWith("-WRONGPASS"), `Expected WRONGPASS, got ${bad2}`);

    const badUser = await sendRawAndReadLine(encodeResp("AUTH", "otheruser", REDIS_PASS));
    assert.ok(badUser.startsWith("-WRONGPASS"), `Expected WRONGPASS for bad user, got ${badUser}`);

    const badHello = await sendRawAndReadLine(encodeResp("HELLO", "3", "AUTH", "default", "wrong"));
    assert.ok(badHello.startsWith("-WRONGPASS"), `Expected WRONGPASS on HELLO, got ${badHello}`);

    // Valid HELLO 3 AUTH + RESET de-auth on same socket
    const helloOk = await sendRawAndReadLine(
      encodeResp("HELLO", "3", "AUTH", "default", REDIS_PASS, "SETNAME", "sec-test")
    );
    assert.ok(helloOk.startsWith("%"), `Expected RESP3 map on valid HELLO AUTH, got ${helloOk}`);

    // Valid ioredis client with password + RESET re-enforces NOAUTH
    const authed = new Redis({ host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASS });
    try {
      assert.equal(await authed.ping(), "PONG");
      await authed.set("sec:key", "protected");
      assert.equal(await authed.get("sec:key"), "protected");

      // Send RESET and verify subsequent PING returns NOAUTH
      assert.equal(await authed.call("RESET"), "RESET");
      await assert.rejects(() => authed.ping(), /NOAUTH/);
    } finally {
      authed.disconnect();
    }
  }
  console.log("        PASS");

  // 3. Lua sandbox & execution timeout hook (pg_redis.lua_time_limit_ms=300)
  console.log("  [3/5] Enforcing Lua sandbox & lua_time_limit_ms against infinite loops...");
  {
    const r = new Redis({ host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASS });
    try {
      const t0 = Date.now();
      await assert.rejects(
        () => r.eval("while true do end", 0),
        /Lua script exceeded time limit/
      );
      const elapsed = Date.now() - t0;
      assert.ok(elapsed >= 200 && elapsed < 3000, `Unexpected timeout duration: ${elapsed}ms`);

      // Worker must remain healthy immediately after aborting runaway script
      assert.equal(await r.ping(), "PONG");
    } finally {
      r.disconnect();
    }
  }
  console.log("        PASS");

  // 4. pg_redis.max_frame_bytes enforcement (configured to 16384)
  console.log("  [4/5] Enforcing pg_redis.max_frame_bytes on oversized frame...");
  {
    const oversized = encodeResp("SET", "big", "x".repeat(20 * 1024));
    const reply = await sendRawAndReadLine(oversized);
    assert.ok(
      reply.includes("max frame size exceeded"),
      `Expected max frame size exceeded error, got ${reply}`
    );
  }
  console.log("        PASS");

  // 5. pg_redis.max_clients enforcement (configured to 4)
  console.log("  [5/5] Enforcing pg_redis.max_clients on excess connections...");
  {
    const held = [];
    try {
      for (let i = 0; i < 4; i++) {
        const c = new Redis({ host: REDIS_HOST, port: REDIS_PORT, password: REDIS_PASS });
        assert.equal(await c.ping(), "PONG");
        held.push(c);
      }

      // 5th connection should receive -ERR max number of clients reached immediately upon accept
      const excessReply = await sendRawAndReadLine(null);
      assert.ok(
        excessReply.includes("max number of clients reached"),
        `Expected max clients error, got ${excessReply}`
      );
    } finally {
      for (const c of held) c.disconnect();
    }
  }
  console.log("        PASS");

  console.log("==> ALL 5 V0.4.0 SECURITY & LIMITS TESTS PASSED!");
}

runSecurityTests().catch((err) => {
  console.error("SECURITY TEST FAILED:", err);
  process.exit(1);
});
