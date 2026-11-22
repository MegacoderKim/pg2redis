# Quick Start

Get a fully functional PostgreSQL 15 instance running with the embedded `pg_redis_core` RESP server in under a minute using Docker Compose.

## Prerequisites

- **Docker** (24+) and **Docker Compose** (v2+)
- Optional: `nc` (netcat) or `redis-cli` on your host machine

## 1. Clone and Start

```bash
git clone https://github.com/MegacoderKim/pg2redis.git
cd pg2redis

docker compose up -d --build --wait pg-redis
```

The container exposes two ports on `localhost`:

| Port | Protocol | Purpose |
| --- | --- | --- |
| `5432` | PostgreSQL Wire Protocol | Standard `psql` and SQL client connections (`postgres` / `postgres`) |
| `6379` | Redis RESP2 / RESP3 | Any Redis client driver (`redis-py`, `Jedis`, `go-redis`, `redis-cli`) |

The Compose healthcheck verifies **both** `pg_isready` and `redis-cli PING` -> `PONG` before reporting healthy.

## 2. Verify Raw RESP Compliance

Test the TCP listener directly with raw RESP2 frames:

```bash
printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 localhost 6379
# Expected output:
# +PONG
```

## 3. Exercise All Data Structures

```bash
# Strings & TTL
docker compose exec pg-redis redis-cli SET session:1001 "active_token_xyz" EX 300
docker compose exec pg-redis redis-cli GET session:1001
docker compose exec pg-redis redis-cli TTL session:1001

# Hashes
docker compose exec pg-redis redis-cli HSET user:42 name "Ada" role "engineer"
docker compose exec pg-redis redis-cli HGETALL user:42

# Lists
docker compose exec pg-redis redis-cli LPUSH queue:jobs "job1" "job2"
docker compose exec pg-redis redis-cli RPUSH queue:jobs "job3"
docker compose exec pg-redis redis-cli LRANGE queue:jobs 0 -1

# Sorted Sets
docker compose exec pg-redis redis-cli ZADD leaderboard 100 alice 250.5 bob 50 dave
docker compose exec pg-redis redis-cli ZRANGE leaderboard 0 -1 WITHSCORES
```

## 4. Inspect Data in PostgreSQL SQL

Every write immediately persists into standard PostgreSQL tables that you can query or join with the rest of your relational schema:

```sql
SELECT key, convert_from(value, 'UTF8') AS val, expires_at FROM pg_redis_strings;
SELECT * FROM pg_redis_hashes WHERE key = 'user:42';
SELECT * FROM pg_redis_lists  WHERE key = 'queue:jobs' ORDER BY seq ASC;
SELECT * FROM pg_redis_zsets  WHERE key = 'leaderboard' ORDER BY score ASC, member ASC;
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
    Execute `./tests/run_all_tests.sh` to run raw TCP RESP checks, Python (`FastAPI` + `redis-py` + `psycopg`), Go (`go-redis/v9`), Kotlin (`Jedis`), and `cargo pgrx test pg15` + `cargo pgrx run pg15`.
