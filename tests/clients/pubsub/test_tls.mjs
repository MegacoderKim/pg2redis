import assert from "node:assert/strict";
import fs from "node:fs";
import net from "node:net";
import tls from "node:tls";
import Redis from "ioredis";

const MODE = process.env.TLS_TEST_MODE || "dual";
const HOST = process.env.REDIS_HOST || "127.0.0.1";
const PLAIN_PORT = Number(process.env.REDIS_PLAIN_PORT || "6379");
const TLS_PORT = Number(process.env.REDIS_TLS_PORT || "6380");
const PASS = process.env.REDIS_PASS || "";
const CERT_DIR = process.env.CERT_DIR || "/certs";

const ca = fs.readFileSync(`${CERT_DIR}/ca.crt`);

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

function getServerCertSerial(tlsOptions) {
  return new Promise((resolve, reject) => {
    const socket = tls.connect({ host: HOST, port: TLS_PORT, ...tlsOptions }, () => {
      const peer = socket.getPeerCertificate();
      socket.end();
      resolve(peer.serialNumber);
    });
    socket.on("error", reject);
  });
}

function expectTcpRefused(host, port) {
  return new Promise((resolve, reject) => {
    const s = net.createConnection({ host, port });
    s.setTimeout(3000);
    s.once("connect", () => {
      s.destroy();
      reject(new Error(`Expected ECONNREFUSED on ${host}:${port}, but connected`));
    });
    s.once("error", (err) => {
      if (err.code === "ECONNREFUSED") {
        resolve();
      } else {
        reject(err);
      }
    });
    s.once("timeout", () => {
      s.destroy();
      reject(new Error(`Timed out waiting for ECONNREFUSED on ${host}:${port}`));
    });
  });
}

async function runDualPortTests() {
  console.log("=== TLS Dual-Port Mode Tests (plain :6379 + rediss :6380) ===");

  // 1. Plain TCP and TLS clients both work concurrently with shared state
  const plain = new Redis({
    host: HOST,
    port: PLAIN_PORT,
    password: PASS || undefined,
    maxRetriesPerRequest: 2,
  });
  const secure = new Redis({
    host: HOST,
    port: TLS_PORT,
    password: PASS || undefined,
    tls: { ca, servername: HOST },
    maxRetriesPerRequest: 2,
  });

  await Promise.all([waitReady(plain), waitReady(secure)]);

  assert.equal(await plain.ping(), "PONG");
  assert.equal(await secure.ping(), "PONG");

  await plain.set("tls:shared:from_plain", "hello_plain");
  assert.equal(await secure.get("tls:shared:from_plain"), "hello_plain");

  await secure.set("tls:shared:from_tls", "hello_tls", "EX", 60);
  assert.equal(await plain.get("tls:shared:from_tls"), "hello_tls");

  // 2. INFO & CONFIG GET reflect both ports
  const info = await secure.info();
  assert.match(info, new RegExp(`tcp_port:${PLAIN_PORT}`));
  assert.match(info, new RegExp(`tls_port:${TLS_PORT}`));

  const cfg = await secure.config("GET", "tls-*");
  const cfgMap = {};
  for (let i = 0; i < cfg.length; i += 2) cfgMap[cfg[i]] = cfg[i + 1];
  assert.equal(cfgMap["tls-port"], String(TLS_PORT));
  assert.equal(cfgMap["tls-auth-clients"], "no");

  // 3. Cross-transport Pub/Sub (plain publisher -> TLS pattern subscriber)
  const tlsSub = new Redis({
    host: HOST,
    port: TLS_PORT,
    password: PASS || undefined,
    tls: { ca, servername: HOST },
    maxRetriesPerRequest: 2,
  });
  await waitReady(tlsSub);
  await tlsSub.psubscribe("tls:events:*");

  const msgPromise = new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error("TLS pmessage timeout")), 5000);
    tlsSub.once("pmessage", (pat, ch, payload) => {
      clearTimeout(t);
      resolve({ pat, ch, payload });
    });
  });

  const delivered = await plain.publish("tls:events:order", "encrypted_payload_ok");
  assert.equal(delivered, 1);
  const msg = await msgPromise;
  assert.equal(msg.pat, "tls:events:*");
  assert.equal(msg.ch, "tls:events:order");
  assert.equal(msg.payload, "encrypted_payload_ok");
  await tlsSub.quit();

  // 4. Blocking BZPOPMIN over TLS (woken by plain TCP ZADD)
  const tlsBlocker = new Redis({
    host: HOST,
    port: TLS_PORT,
    password: PASS || undefined,
    tls: { ca, servername: HOST },
    maxRetriesPerRequest: null,
  });
  await waitReady(tlsBlocker);
  await plain.del("tls:zqueue");

  const popPromise = tlsBlocker.bzpopmin("tls:zqueue", 5);
  await new Promise((r) => setTimeout(r, 150));
  await plain.zadd("tls:zqueue", 10, "job_over_tls");
  const popped = await popPromise;
  assert.deepEqual(popped, ["tls:zqueue", "job_over_tls", "10"]);
  await tlsBlocker.quit();

  // 5. Plaintext garbage sent to TLS port is cleanly rejected without affecting TLS server
  await new Promise((resolve) => {
    const raw = net.createConnection({ host: HOST, port: TLS_PORT }, () => {
      raw.write("*1\r\n$4\r\nPING\r\n");
    });
    raw.setTimeout(3000, () => {
      raw.destroy();
      resolve();
    });
    raw.on("error", () => resolve());
    raw.on("close", () => resolve());
  });
  assert.equal(await secure.ping(), "PONG");

  await plain.quit();
  await secure.quit();
  console.log("  [PASS] Dual-Port TLS tests passed!");
}

async function runMtlsOnlyTests() {
  console.log("=== TLS-Only + mTLS Tests (port=0, tls_port=6380, tls_auth_clients=yes) ===");

  const clientCert = fs.readFileSync(`${CERT_DIR}/client.crt`);
  const clientKey = fs.readFileSync(`${CERT_DIR}/client.key`);

  // 1. Plain TCP port must NOT be listening (TLS-only mode)
  await expectTcpRefused(HOST, PLAIN_PORT);
  console.log("  [PASS] Plain TCP port 6379 refused (TLS-only enforced)");

  // 2. TLS client WITHOUT client certificate must fail handshake
  await assert.rejects(
    async () => {
      await new Promise((resolve, reject) => {
        const sock = tls.connect(
          { host: HOST, port: TLS_PORT, ca, servername: HOST },
          () => {
            // In TLS 1.3, client cert rejection alert arrives after server verifies handshake
            sock.write("*1\r\n$4\r\nPING\r\n");
          }
        );
        sock.setTimeout(3000, () => {
          sock.destroy();
          reject(new Error("timeout waiting for mTLS rejection"));
        });
        let gotData = false;
        sock.on("data", () => {
          gotData = true;
          sock.destroy();
          resolve("unexpectedly_succeeded");
        });
        sock.on("error", (err) => reject(err));
        sock.on("close", () => {
          if (!gotData) reject(new Error("connection closed without response (mTLS rejected)"));
        });
      });
    },
    "Expected TLS connection without client cert to be rejected under tls_auth_clients=yes"
  );
  console.log("  [PASS] TLS connection without client cert rejected (mTLS enforced)");

  // 3. TLS client WITH valid CA-signed client certificate succeeds
  const mtls = new Redis({
    host: HOST,
    port: TLS_PORT,
    password: PASS || undefined,
    tls: { ca, cert: clientCert, key: clientKey, servername: HOST },
    maxRetriesPerRequest: 2,
  });
  await waitReady(mtls);
  assert.equal(await mtls.ping(), "PONG");

  const info = await mtls.info();
  assert.match(info, /tcp_port:0/);
  assert.match(info, new RegExp(`tls_port:${TLS_PORT}`));

  const cfg = await mtls.config("GET", "tls-auth-clients");
  assert.equal(cfg[1], "yes");

  await mtls.set("mtls:key", "verified_client");
  assert.equal(await mtls.get("mtls:key"), "verified_client");

  // Lua script over mTLS
  const luaRes = await mtls.eval("return redis.call('GET', KEYS[1]) .. ':lua'", 1, "mtls:key");
  assert.equal(luaRes, "verified_client:lua");

  await mtls.quit();
  console.log("  [PASS] mTLS client certificate authentication passed!");
}

async function runCertSerialCheck() {
  const clientCert = fs.readFileSync(`${CERT_DIR}/client.crt`);
  const clientKey = fs.readFileSync(`${CERT_DIR}/client.key`);
  const serial = await getServerCertSerial({
    ca,
    cert: clientCert,
    key: clientKey,
    servername: HOST,
  });
  console.log(`SERVER_CERT_SERIAL=${serial}`);
}

try {
  if (MODE === "dual") {
    await runDualPortTests();
  } else if (MODE === "mtls") {
    await runMtlsOnlyTests();
  } else if (MODE === "serial") {
    await runCertSerialCheck();
  } else {
    throw new Error(`Unknown TLS_TEST_MODE: ${MODE}`);
  }
  process.exit(0);
} catch (err) {
  console.error("TLS TEST FAILED:", err);
  process.exit(1);
}
