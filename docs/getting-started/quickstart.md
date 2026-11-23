# Quick Start

Get a fully functional **PostgreSQL 15 or 16** instance running with the embedded `pg_redis_core` RESP server in under a minute using either prebuilt GHCR Docker images or local Docker Compose.

## Prerequisites

- **Docker** (24+) and **Docker Compose** (v2+)
- Optional: `nc` (netcat) or `redis-cli` on your host machine

## 1. Start `pg_redis_core`

=== "Prebuilt Multi-Arch Image (GHCR)"

    Pull the prebuilt `linux/amd64` / `linux/arm64` image for PostgreSQL 15 or 16:

    ```bash
    # PostgreSQL 16 (or use :latest-pg15 for PostgreSQL 15)
    docker run -d --name pg-redis \
      -e POSTGRES_PASSWORD=postgres \
      -p 5432:5432 -p 6379:6379 \
      ghcr.io/megacoderkim/pg2redis:latest-pg16
    ```

    To enable password protection (`AUTH` / `HELLO ... AUTH`), append `-c pg_redis.requirepass=my-secret` to the container's `postgres` command (see [Configuration & SQL API](configuration.md#security-sandboxing-resource-limits)).

=== "Build Locally with Docker Compose"

    ```bash
    git clone https://github.com/MegacoderKim/pg2redis.git
    cd pg2redis

    # Builds PostgreSQL 15 by default; pass PG_MAJOR=16 for PostgreSQL 16
    docker compose up -d --build --wait pg-redis
    ```

The container exposes two ports on `localhost`:

| Port | Protocol | Purpose |
| --- | --- | --- |
| `5432` | PostgreSQL Wire Protocol | Standard `psql` and SQL client connections (`postgres` / `postgres`) |
| `6379` | Redis RESP2 / RESP3 | Any Redis client driver (`ioredis`, `BullMQ`, `redis-py`, `Jedis`, `go-redis`, `redis-cli`) |

The Compose healthcheck verifies **both** `pg_isready` and `redis-cli PING` -> `PONG` before reporting healthy.

## 2. Verify Raw RESP Compliance

Test the TCP listener directly with raw RESP2 frames:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 localhost 6379
# Expected output:
# +PONG
```

## 3. Exercise All Data Structures, Universal TTL, Pub/Sub & Lua

```bash
# Strings, Rolling TTL (GETEX) & Expiry
docker compose exec pg-redis redis-cli SET session:1001 "active_token_xyz" EX 300
docker compose exec pg-redis redis-cli GETEX session:1001 EX 600
docker compose exec pg-redis redis-cli TTL session:1001

# Hashes + Universal TTL (NX|XX|GT|LT) & HSCAN
docker compose exec pg-redis redis-cli HSET user:42 name "Ada" role "engineer"
docker compose exec pg-redis redis-cli HGETALL user:42
docker compose exec pg-redis redis-cli EXPIRE user:42 120 NX
docker compose exec pg-redis redis-cli HSCAN user:42 0 MATCH "n*"

# Lists
docker compose exec pg-redis redis-cli LPUSH queue:jobs "job1" "job2"
docker compose exec pg-redis redis-cli RPUSH queue:jobs "job3"
docker compose exec pg-redis redis-cli LRANGE queue:jobs 0 -1

# Sorted Sets & ZSCAN
docker compose exec pg-redis redis-cli ZADD leaderboard 100 alice 250.5 bob 50 dave
docker compose exec pg-redis redis-cli ZRANGE leaderboard 0 -1 WITHSCORES
docker compose exec pg-redis redis-cli ZSCAN leaderboard 0

# Sets & SSCAN
docker compose exec pg-redis redis-cli SADD roles:42 admin editor viewer
docker compose exec pg-redis redis-cli SMEMBERS roles:42
docker compose exec pg-redis redis-cli SSCAN roles:42 0

# Streams
docker compose exec pg-redis redis-cli XADD events '*' type order.placed order_id 9001
docker compose exec pg-redis redis-cli XRANGE events - +

# Sandboxed Lua 5.1 Scripting (with cjson / cmsgpack / bit)
docker compose exec pg-redis redis-cli EVAL "return redis.call('GET', KEYS[1])" 1 session:1001

# Multi-Client Inspection
docker compose exec pg-redis redis-cli CLIENT LIST
```

To try **Named & Pattern Pub/Sub**, open a subscriber in one terminal and publish from another:

```bash
# Terminal 1: subscribe to a glob pattern
docker compose exec pg-redis redis-cli PSUBSCRIBE "orders:*"

# Terminal 2: publish an event
docker compose exec pg-redis redis-cli PUBLISH orders:created '{"id":"ord_01"}'
```

## 4. Inspect Data in PostgreSQL SQL

Every write immediately persists into standard PostgreSQL tables that you can query or join with the rest of your relational schema:

```sql
SELECT key, convert_from(value, 'UTF8') AS val, expires_at FROM pg_redis_strings;
SELECT * FROM pg_redis_hashes     WHERE key = 'user:42';
SELECT * FROM pg_redis_lists      WHERE key = 'queue:jobs' ORDER BY seq ASC;
SELECT * FROM pg_redis_zsets      WHERE key = 'leaderboard' ORDER BY score ASC, member ASC;
SELECT * FROM pg_redis_sets       WHERE key = 'roles:42';
SELECT * FROM pg_redis_streams    WHERE key = 'events' ORDER BY ms ASC, seq ASC;
SELECT * FROM pg_redis_key_expiry WHERE key = 'user:42';
```

You can also invoke commands directly from SQL via [`redis_dispatcher`](configuration.md#sql-functions):

```sql
SELECT redis_dispatcher('PING', NULL, NULL, NULL);
--  redis_dispatcher
-- ------------------
--  +PONG\r\n
```

## 5. Run the Live FastAPI Gateway (Optional)

Start the included FastAPI HTTP cache gateway (`POST /cache/{key}` and `GET /cache/{key}`) on port `8000`:

```bash
docker compose --profile gateway up -d --build fastapi-gateway

curl -X POST "http://localhost:8000/cache/hello?value=postgres_powered"
curl "http://localhost:8000/cache/hello"
```

!!! tip "Run the full verification suite"
    Execute `./tests/run_all_tests.sh` to run raw TCP RESP checks, Python (`FastAPI` + `redis-py` + `psycopg`), Go (`go-redis/v9`), Kotlin (`Jedis`), BullMQ 5.13.0, `ioredis@5.11.1` Pub/Sub & v0.4.0 feature checks, the v0.4.0 security & limits suite (`./tests/test_v040_security.sh`), and `cargo pgrx test pg15`. Run `./tests/test_solar_nest_isolation.sh 15 16` to additionally verify database isolation + Pub/Sub + BullMQ across both PostgreSQL 15 and PostgreSQL 16.
