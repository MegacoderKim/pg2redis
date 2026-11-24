# Client Examples (Python, Kotlin, Go & BullMQ / Medusa)

`pg_redis_core` works out of the box with standard Redis drivers and job-queue frameworks (including **BullMQ 5.13.0** for **Medusa**) — no custom SDKs or PostgreSQL-specific client code required.

All runnable Dockerized suites live under [`tests/clients/`](https://github.com/MegacoderKim/pg2redis/tree/main/tests/clients).

## Multi-Language Quick Reference

=== "Node.js (ioredis 5.11.1 Pub/Sub / Medusa Workflows)"

    ```javascript title="tests/clients/pubsub/test_pubsub.mjs"
    import Redis from "ioredis";

    const sub = new Redis({ host: "localhost", port: 6379 });
    const psub = new Redis({ host: "localhost", port: 6379 });
    const pub = new Redis({ host: "localhost", port: 6379 });

    // Wait for ioredis readyCheck (INFO) before entering RESP2 subscribed mode
    const waitReady = (c) =>
      c.status === "ready" ? Promise.resolve() : new Promise((res) => c.once("ready", res));
    await Promise.all([waitReady(sub), waitReady(psub), waitReady(pub)]);

    // Named channel subscription
    await sub.subscribe("medusa:workflow:create-order:tx_01");
    sub.on("message", (channel, payload) => {
      console.log("Workflow event:", channel, JSON.parse(payload));
    });

    // Glob pattern subscription (PSUBSCRIBE / pmessage)
    await psub.psubscribe("medusa:workflow:*");
    psub.on("pmessage", (pattern, channel, payload) => {
      console.log("Pattern match:", pattern, "->", channel, JSON.parse(payload));
    });

    const recipients = await pub.publish(
      "medusa:workflow:create-order:tx_01",
      JSON.stringify({ workflowId: "create-order-workflow", event: "step.succeeded" })
    );
    console.log("Delivered to", recipients, "subscriber(s)"); // 2
    ```

    **Run with Docker Compose:**
    ```bash
    docker compose --profile test run --rm --build test-pubsub
    ```

=== "Node.js (BullMQ 5.13.0 / Medusa)"

    ```javascript title="tests/clients/bullmq/test_bullmq.mjs"
    import { Queue, Worker, QueueEvents } from "bullmq";

    const connection = {
      host: process.env.REDIS_HOST || "localhost",
      port: Number(process.env.REDIS_PORT || "6379"),
      maxRetriesPerRequest: null,
    };

    const queue = new Queue("medusa-events", { connection });
    const queueEvents = new QueueEvents("medusa-events", { connection });

    // Idle worker blocks cooperatively on BZPOPMIN; wakes instantly when Lua enqueues jobs
    const worker = new Worker(
      "medusa-events",
      async (job) => {
        console.log(`Processing ${job.name}:`, job.data);
        return { delivered: true };
      },
      { connection }
    );

    // Enqueues via BullMQ's Lua 5.1 scripts (cmsgpack + cjson + XADD + ZADD)
    const job = await queue.add("order.placed", {
      orderId: "order_01J9XYZ",
      email: "buyer@example.com",
    });

    const result = await job.waitUntilFinished(queueEvents, 10000);
    console.log("Completed:", result);
    ```

    **Run with Docker Compose:**
    ```bash
    docker compose --profile test run --rm --build test-bullmq
    ```

=== "Python (FastAPI + redis-py)"

    ```python title="tests/clients/python/app.py"
    import os
    from contextlib import asynccontextmanager
    from fastapi import FastAPI, HTTPException
    import redis.asyncio as redis

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await redis_client.ping()
        yield
        await redis_client.aclose()

    app = FastAPI(title="Postgres-Redis Gateway API", lifespan=lifespan)

    @app.post("/cache/{key}")
    async def set_cache_value(key: str, value: str):
        await redis_client.set(key, value)
        return {"status": "success", "key": key}

    @app.get("/cache/{key}")
    async def get_cache_value(key: str):
        val = await redis_client.get(key)
        if val is None:
            raise HTTPException(status_code=404, detail="Key not found")
        return {"key": key, "value": val}
    ```

    **Run with Docker Compose:**
    ```bash
    # Run full Python pytest suite (FastAPI + Strings/Hashes/Lists/ZSets + SQL parity)
    docker compose --profile test run --rm test-python

    # Or launch the live FastAPI HTTP server on http://localhost:8000
    docker compose --profile gateway up -d fastapi-gateway
    ```

=== "Kotlin (Jedis 5.2)"

    ```kotlin title="tests/clients/kotlin/src/main/kotlin/Main.kt"
    import redis.clients.jedis.Jedis

    object RedisPostgresClient {
        private val host = System.getenv("REDIS_HOST") ?: "localhost"
        private val port = (System.getenv("REDIS_PORT") ?: "6379").toInt()
        private val jedis = Jedis(host, port)

        fun setKey(key: String, value: String): String? = jedis.set(key, value)
        fun getKey(key: String): String? = jedis.get(key)
        fun client(): Jedis = jedis
    }

    fun main() {
        RedisPostgresClient.setKey("user:1001:session", "active_token_xyz")
        val session = RedisPostgresClient.getKey("user:1001:session")
        println("Retrieved from Postgres-Redis: $session")

        val jedis = RedisPostgresClient.client()
        jedis.hset("kt:hash", mapOf("lang" to "kotlin", "driver" to "jedis"))
        jedis.lpush("kt:list", "second", "first")
        jedis.zadd("kt:zset", mapOf("alice" to 10.0, "bob" to 30.0))
        jedis.close()
    }
    ```

    **Run with Docker Compose:**
    ```bash
    docker compose --profile test run --rm test-kotlin
    ```

=== "Go (go-redis/v9 RESP3)"

    ```go title="tests/clients/go/main.go"
    package main

    import (
        "context"
        "fmt"
        "os"

        "github.com/redis/go-redis/v9"
    )

    var ctx = context.Background()

    func main() {
        addr := os.Getenv("REDIS_ADDR")
        if addr == "" {
            addr = "localhost:6379"
        }

        // Negotiates RESP3 (HELLO 3 + CLIENT SETINFO) automatically
        rdb := redis.NewClient(&redis.Options{
            Addr:     addr,
            Password: "",
            DB:       0,
        })
        defer rdb.Close()

        if err := rdb.Set(ctx, "go_key", "hello from golang", 0).Err(); err != nil {
            panic(err)
        }

        val, err := rdb.Get(ctx, "go_key").Result()
        if err != nil {
            panic(err)
        }
        fmt.Printf("go_key: %s\n", val)
    }
    ```

    **Run with Docker Compose:**
    ```bash
    docker compose --profile test run --rm test-go
    ```

=== "Raw TCP (`nc`)"

    ```bash
    # Standard RESP2 multibulk PING
    printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 localhost 6379
    # -> +PONG

    # Inline PING
    printf 'PING\r\n' | nc -w 2 localhost 6379
    # -> +PONG

    # RESP3 handshake
    printf '*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n' | nc -w 2 localhost 6379
    # -> %7 ...
    ```

=== "Direct PostgreSQL SQL"

    ```sql
    -- Call Redis commands directly from SQL
    SELECT redis_dispatcher('SET', 'sql_key', 'from_postgres', NULL);
    SELECT redis_dispatcher('GET', 'sql_key', NULL, NULL);

    -- Join Redis keys with standard application tables
    SELECT
        u.id,
        u.email,
        convert_from(s.value, 'UTF8') AS active_session_token,
        s.expires_at
    FROM users u
    JOIN pg_redis_strings s
      ON s.key = 'user:' || u.id || ':session'
    WHERE s.expires_at IS NULL OR s.expires_at > NOW();
    ```

## Pipelining & Transactions

Both raw pipelining (`transaction=False` / `rdb.Pipeline()` / `jedis.pipelined()`) and transactional `MULTI`/`EXEC` pipelines (`transaction=True` / `rdb.TxPipeline()`) execute inside a single PostgreSQL SPI transaction:

=== "Python"

    ```python
    async with r.pipeline(transaction=True) as pipe:
        pipe.set("tx:a", "10")
        pipe.incr("tx:a")
        pipe.get("tx:a")
        results = await pipe.execute()  # [True, 11, "11"]
    ```

=== "Go"

    ```go
    tx := rdb.TxPipeline()
    tx.Set(ctx, "go:tx:1", "100", 0)
    incr := tx.Incr(ctx, "go:tx:1")
    if _, err := tx.Exec(ctx); err != nil {
        panic(err)
    }
    fmt.Println(incr.Val()) // 101
    ```

=== "Kotlin"

    ```kotlin
    val pipe = jedis.pipelined()
    pipe.set("kt:pipe:1", "one")
    val resp = pipe.get("kt:pipe:1")
    pipe.sync()
    println(resp.get()) // "one"
    ```

## Authentication (`pg_redis.requirepass`)

When `pg_redis.requirepass` is configured in PostgreSQL, every standard Redis driver authenticates seamlessly via `AUTH` or RESP3 `HELLO 3 AUTH`:

=== "Node.js (ioredis)"

    ```javascript
    import Redis from "ioredis";

    const redis = new Redis({
      host: "localhost",
      port: 6379,
      password: process.env.REDIS_PASSWORD,
    });
    console.log(await redis.ping()); // PONG
    ```

=== "Python (redis-py)"

    ```python
    import os
    import redis.asyncio as redis

    r = redis.Redis(host="localhost", port=6379, password=os.environ["REDIS_PASSWORD"])
    await r.ping()  # True
    ```

=== "Go (go-redis/v9 RESP3)"

    ```go
    rdb := redis.NewClient(&redis.Options{
        Addr:     "localhost:6379",
        Password: os.Getenv("REDIS_PASSWORD"), // sent via HELLO 3 AUTH default <pass>
    })
    ```

=== "redis-cli"

    ```bash
    redis-cli -a "$REDIS_PASSWORD" PING
    ```

## Optional TLS (`rediss://`) & Mutual TLS (mTLS)

TLS is disabled by default (`pg_redis.tls_port = 0`), leaving plain TCP on `:6379` unchanged. To opt in, set `pg_redis.tls_port = 6380`, `pg_redis.tls_cert_file`, and `pg_redis.tls_key_file` (and optionally `pg_redis.port = 0` for TLS-only mode or `pg_redis.tls_auth_clients = 'yes'` + `pg_redis.tls_ca_cert_file` for mTLS):

=== "Node.js (ioredis / BullMQ)"

    ```javascript
    import fs from "node:fs";
    import Redis from "ioredis";

    // Server TLS (or add cert + key for mTLS when pg_redis.tls_auth_clients = 'yes')
    const redis = new Redis({
      host: "localhost",
      port: 6380,
      password: process.env.REDIS_PASSWORD,
      tls: {
        ca: fs.readFileSync("/etc/certs/ca.crt"),
        // Optional mTLS client certificate:
        // cert: fs.readFileSync("/etc/certs/client.crt"),
        // key: fs.readFileSync("/etc/certs/client.key"),
      },
    });
    console.log(await redis.ping()); // PONG
    ```

=== "Python (redis-py)"

    ```python
    import os
    import redis.asyncio as redis

    r = redis.Redis(
        host="localhost",
        port=6380,
        password=os.environ["REDIS_PASSWORD"],
        ssl=True,
        ssl_ca_certs="/etc/certs/ca.crt",
        # Optional mTLS:
        # ssl_certfile="/etc/certs/client.crt",
        # ssl_keyfile="/etc/certs/client.key",
    )
    await r.ping()  # True
    ```

=== "Go (go-redis/v9 RESP3)"

    ```go
    rdb := redis.NewClient(&redis.Options{
        Addr:      "localhost:6380",
        Password:  os.Getenv("REDIS_PASSWORD"),
        TLSConfig: &tls.Config{RootCAs: caPool, MinVersion: tls.VersionTLS12},
    })
    ```

=== "redis-cli"

    ```bash
    # Server TLS
    redis-cli --tls --cacert /etc/certs/ca.crt -h localhost -p 6380 -a "$REDIS_PASSWORD" PING

    # Mutual TLS (mTLS)
    redis-cli --tls --cacert /etc/certs/ca.crt \
      --cert /etc/certs/client.crt --key /etc/certs/client.key \
      -h localhost -p 6380 -a "$REDIS_PASSWORD" PING
    ```

## Universal TTL, Rolling Sessions (`GETEX`) & Collection Scans

`pg_redis_core` supports universal expiration (`EXPIRE`/`PEXPIRE` with `NX|XX|GT|LT`, `EXPIRETIME`/`PEXPIRETIME`) across all 6 data types, rolling session TTLs via `GETEX` (`connect-redis`), and cursor-based scans (`SCAN TYPE`, `HSCAN [NOVALUES]`, `SSCAN`, `ZSCAN`) used by BullMQ `FlowProducer`:

```javascript
import Redis from "ioredis";

const redis = new Redis({ host: "localhost", port: 6379 });

// Rolling session TTL (connect-redis)
await redis.set("sess:abc", JSON.stringify({ userId: 42 }), "EX", 300);
const raw = await redis.getex("sess:abc", "EX", 900); // reads & extends TTL to 900s

// Universal TTL with Redis 7 flags on a Hash
await redis.hset("cart:42", "sku_1", "2", "sku_2", "1");
await redis.expire("cart:42", 1800, "NX"); // sets expiry only if none exists
const [cursor, fields] = await redis.hscan("cart:42", "0", "MATCH", "sku_*", "COUNT", 50);

// Type-filtered key scan & live multi-client inspection
const [, hashKeys] = await redis.scan("0", "MATCH", "cart:*", "TYPE", "hash");
const clients = await redis.client("LIST", "TYPE", "normal");
```

## Multiple Logical Databases (`SELECT 0`–`15`)

`pg_redis_core` ships with `pg_redis.databases = 16` by default (configurable up to `1024`), so you can isolate workloads across numbered databases just like standard Redis:

```javascript
import Redis from "ioredis";

const cacheDb = new Redis("redis://localhost:6379/0");
const queueDb = new Redis("redis://localhost:6379/1");

await cacheDb.set("config:version", "v1");
await queueDb.set("config:version", "v2");

console.log(await cacheDb.get("config:version")); // "v1"
console.log(await queueDb.get("config:version")); // "v2"

// Move a key (with its TTL) to DB 2, swap DBs, or flush only one DB
await cacheDb.move("config:version", 2);
await cacheDb.swapdb(1, 2);
await queueDb.flushdb(); // clears DB 1 only; DB 0 & DB 2 stay intact
```

## Sandboxed Lua 5.1 Scripting

Execute atomic multi-key logic inside a single PostgreSQL SPI transaction with `cjson`, `cmsgpack`, and `bit` built in — protected by the restricted Lua 5.1 sandbox and `pg_redis.lua_time_limit_ms`:

```javascript
const sha = await redis.script(
  "LOAD",
  `
  local current = tonumber(redis.call("GET", KEYS[1]) or "0")
  if current >= tonumber(ARGV[1]) then
    return 0
  end
  redis.call("INCR", KEYS[1])
  redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]), "NX")
  return 1
  `
);

const allowed = await redis.evalsha(sha, 1, "ratelimit:user:42", 100, 60);
```
