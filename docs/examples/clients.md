# Client Examples (Python, Kotlin, Go & BullMQ / Medusa)

`pg_redis_core` works out of the box with standard Redis drivers and job-queue frameworks (including **BullMQ 5.13.0** for **Medusa**) — no custom SDKs or PostgreSQL-specific client code required.

All runnable Dockerized suites live under [`tests/clients/`](https://github.com/MegacoderKim/pg2redis/tree/main/tests/clients).

## Multi-Language Quick Reference

=== "Node.js (ioredis 5.11.1 Pub/Sub / Medusa Workflows)"

    ```javascript title="tests/clients/pubsub/test_pubsub.mjs"
    import Redis from "ioredis";

    const sub = new Redis({ host: "localhost", port: 6379 });
    const pub = new Redis({ host: "localhost", port: 6379 });

    await sub.subscribe("medusa:workflow:create-order:tx_01");
    sub.on("message", (channel, payload) => {
      console.log("Workflow event:", channel, JSON.parse(payload));
    });

    const recipients = await pub.publish(
      "medusa:workflow:create-order:tx_01",
      JSON.stringify({ workflowId: "create-order-workflow", event: "step.succeeded" })
    );
    console.log("Delivered to", recipients, "subscriber(s)");
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
