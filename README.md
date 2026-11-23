# `pg_redis_core` — High-Performance Redis Emulation Layer on PostgreSQL

[![Docs](https://img.shields.io/badge/docs-MkDocs%20Material-6d28d9?logo=materialformkdocs&logoColor=white)](https://megacoderkim.github.io/pg2redis/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Rust](https://img.shields.io/badge/Rust-pgrx%200.11-DEA584?logo=rust&logoColor=black)](https://github.com/pgcentralfoundation/pgrx)
[![License: MIT](https://img.shields.io/badge/License-MIT-0891b2.svg)](LICENSE)

`pg_redis_core` is a PostgreSQL 15 & 16 extension written in Rust (`pgrx` 0.11, `tokio`, `redis-protocol` 6.0, `mlua` Lua 5.1, `moka`) that embeds a Redis Serialization Protocol (RESP2 / RESP3) TCP server directly inside a PostgreSQL background worker. Standard Redis client libraries (`ioredis`, `BullMQ`, `redis-py` / `aioredis`, `Jedis`, `go-redis/v9`, `redis-cli`) connect on port `6379` while all data is persisted in ACID-compliant PostgreSQL catalog tables.

**[Read the full documentation →](https://megacoderkim.github.io/pg2redis/)** (or preview locally with `docker compose --profile docs up --build docs` on `http://localhost:8080`).

## Architecture

- **Embedded RESP Listener (`src/worker.rs`, `src/protocol.rs`):** A PostgreSQL background worker runs a single-threaded Tokio (`current_thread` + `LocalSet`) TCP server on `0.0.0.0:6379` with zero-copy `redis-protocol` 6.0 frame parsing, RESP2 & RESP3 (`HELLO 3`) negotiation, and inline command support.
- **Security, Auth & Resource Safeguards (`src/config.rs`, `src/clients.rs`, `src/worker.rs`):** Constant-time `AUTH` and `HELLO ... AUTH` enforcement via `pg_redis.requirepass` (`SUPERUSER_ONLY`), connection caps (`pg_redis.max_clients`), per-connection frame size limits (`pg_redis.max_frame_bytes`), bounded Pub/Sub slow-consumer queues (`pg_redis.pubsub_queue_size`), and a live multi-connection `ClientRegistry` powering `CLIENT LIST`/`INFO`/`KILL` and `INFO # Clients`.
- **In-Process Redis Pub/Sub (`src/pubsub.rs`):** Named-channel and glob-pattern `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, and `PUBSUB` (`CHANNELS`/`NUMSUB`/`NUMPAT`) with exact Redis acknowledgements, binary-safe payloads beyond PostgreSQL's 8 KB `NOTIFY` limit, bounded slow-consumer queues, strict FIFO ordering, non-blocking idle subscribers, and automatic RAII cleanup on disconnect — verified end-to-end with **`ioredis@5.11.1`** (Medusa 2.22.7 workflow notifications).
- **Sandboxed Lua 5.1 Scripting (`src/lua.rs`):** Vendored Lua 5.1 via `mlua` restricted to safe stdlibs (`table`/`string`/`math`, no `os`/`io`/`package`/`debug`) with configurable execution timeouts (`pg_redis.lua_time_limit_ms`), supporting `EVAL`, `EVALSHA`, `SCRIPT LOAD/EXISTS/FLUSH`, `cjson`, `cmsgpack`, `bit`, and atomic `redis.call`/`pcall` execution inside PostgreSQL SPI transactions.
- **Streams, Sets, Universal TTL & Scans (`src/commands/`):** Full `XADD`/`XREAD [BLOCK]`/`XRANGE`, `BZPOPMIN`/`BLPOP`, universal `EXPIRE`/`PEXPIRE` (`NX|XX|GT|LT`) and `EXPIRETIME`/`PEXPIRETIME` across all 6 data types (`pg_redis_key_expiry`), `HSCAN`/`SSCAN`/`ZSCAN`/`SCAN TYPE`, `GETEX`, and multi-client `CLIENT LIST`/`KILL` — verified end-to-end with **BullMQ 5.13.0** (Medusa queues) on both **PostgreSQL 15** and **PostgreSQL 16**.
- **Pipelining & Frame Batching (`src/commands/mod.rs`):** Multiple pipelined frames arriving over a socket are batched into a single PostgreSQL SPI transaction. Full `MULTI` / `EXEC` / `DISCARD` transactions are also supported.
- **Prepared SPI Plans (`src/storage.rs`):** High-frequency queries (`GET`, `SET`, `HGET`, `HSET`, `HGETALL`, `ZSCORE`, TTL eviction) are pre-prepared and retained across transactions via `SPI_keepplan`.
- **In-Process L1 Hot-Key Cache (`src/cache.rs`):** Backed by `moka::sync::Cache`, serving hot string `GET` hits directly from worker memory with exact TTL awareness and immediate write-through invalidation.
- **Storage Optimizations (`src/storage.rs`, `src/lib.rs`):**
  - `fillfactor = 80` on all seven catalog tables for Heap-Only Tuple (HOT) updates.
  - Partial index `idx_redis_strings_expiry` + `idx_redis_key_expiry_at` with periodic `ctid`-batched TTL eviction (`LIMIT 1000`) across all 6 data types.
  - Optional WAL bypass via `pg_redis.unlogged = on` or `SELECT pg_redis_set_unlogged(true);`.

## Catalog Schema

| Table | Purpose | Primary Key |
| --- | --- | --- |
| `pg_redis_strings` | `GET`, `GETEX`, `SET`, `INCR`, `DECR`, `EXPIRE`, `TTL`, `DEL` | `(key)` |
| `pg_redis_hashes` | `HSET`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HINCRBY`, `HSCAN` | `(key, field)` |
| `pg_redis_lists` | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LPOS`, `LMOVE` | `(key, seq)` |
| `pg_redis_zsets` | `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZREM`, `ZPOPMIN`, `BZPOPMIN`, `ZSCORE`, `ZRANK`, `ZSCAN` | `(key, member)` |
| `pg_redis_sets` | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`, `SSCAN` | `(key, member)` |
| `pg_redis_streams` | `XADD`, `XTRIM`, `XREAD [BLOCK]`, `XRANGE`, `XREVRANGE`, `XLEN`, `XDEL` | `(key, ms, seq)` |
| `pg_redis_key_expiry` | Universal TTL expiration (`EXPIRE`/`PEXPIRE`/`TTL`/`PERSIST`) for non-string keys | `(key)` |

## Quick Start (Docker & Prebuilt Images)

Pull a prebuilt multi-arch (`linux/amd64` + `linux/arm64`) image for PostgreSQL 15 or 16 from GHCR, or build locally with Docker Compose:

```bash
# Option A: Pull prebuilt multi-arch GHCR image (pg15 or pg16)
docker run -d --name pg-redis -e POSTGRES_PASSWORD=postgres -p 5432:5432 -p 6379:6379 \
  ghcr.io/megacoderkim/pg2redis:latest-pg16

# Option B: Build and start locally with Docker Compose (PG 15 default; pass PG_MAJOR=16 for PG 16)
docker compose up -d --build --wait pg-redis

# Test with raw TCP RESP2
printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 localhost 6379
# -> +PONG

# Test with redis-cli inside the container
docker compose exec pg-redis redis-cli SET hello world
docker compose exec pg-redis redis-cli GET hello

# Inspect relational persistence directly in PostgreSQL
docker compose exec pg-redis psql -U postgres -c \
  "SELECT key, convert_from(value, 'UTF8'), expires_at FROM pg_redis_strings;"
```

Prebuilt extension tarballs (`pg15`/`pg16` × `amd64`/`arm64`) and `SHA256SUMS.txt` are also published on [GitHub Releases](https://github.com/MegacoderKim/pg2redis/releases).

## Running the Full Verification Suite

Runs raw TCP RESP tests, Python (`FastAPI` + `redis-py` + `psycopg`), Go (`go-redis/v9` RESP3), Kotlin (`Jedis`), **BullMQ 5.13.0**, **`ioredis@5.11.1` Pub/Sub & v0.4.0 features** (pattern Pub/Sub, universal TTL, `HSCAN`/`SSCAN`/`ZSCAN`, `GETEX`, `CLIENT LIST`/`KILL`, Lua sandbox), **v0.4.0 Security & Limits** (`./tests/test_v040_security.sh`), and `cargo pgrx test pg15`:

```bash
./tests/run_all_tests.sh

# Plus PostgreSQL 15 & 16 isolated-database verification (Pub/Sub + BullMQ):
./tests/test_solar_nest_isolation.sh 15 16
```

## Configuration GUCs

| GUC | Default | Description |
| --- | --- | --- |
| `pg_redis.bind_address` | `0.0.0.0` | TCP bind address for RESP listener |
| `pg_redis.port` | `6379` | TCP port for RESP listener |
| `pg_redis.database` | `postgres` | PostgreSQL database used by the background worker |
| `pg_redis.unlogged` | `off` | Create auto-provisioned catalog tables as `UNLOGGED` |
| `pg_redis.cache_enabled` | `on` | Toggle in-process `moka` L1 hot-key cache |
| `pg_redis.cache_capacity` | `10000` | Maximum L1 cache entries |
| `pg_redis.cache_ttl_ms` | `500` | Max L1 cache entry revalidation window (ms) |
| `pg_redis.eviction_interval_ms` | `5000` | Interval between `ctid`-batched TTL eviction runs (ms) |
| `pg_redis.requirepass` | *(unset)* | Optional password enforced on `AUTH` / `HELLO ... AUTH` (`SUPERUSER_ONLY`) |
| `pg_redis.lua_time_limit_ms` | `5000` | Max execution time per Lua `EVAL`/`EVALSHA` script in ms (`0` disables) |
| `pg_redis.max_clients` | `1000` | Max concurrent RESP TCP connections |
| `pg_redis.max_frame_bytes` | `67108864` | Max buffered RESP request bytes per connection (64 MiB) |
| `pg_redis.pubsub_queue_size` | `4096` | Bounded per-subscriber Pub/Sub message queue capacity |

## SQL Functions

- `redis_dispatcher(cmd TEXT, key TEXT, arg1 TEXT, arg2 TEXT) RETURNS TEXT` — Execute Redis commands directly from SQL and receive RESP2-encoded output.
- `pg_redis_evict_expired() RETURNS BIGINT` — Trigger one `ctid`-batched TTL eviction cycle across both `pg_redis_strings` and `pg_redis_key_expiry` (non-string data types).
- `pg_redis_set_unlogged(unlogged BOOLEAN) RETURNS BOOLEAN` — Toggle all seven catalog tables (`strings`, `key_expiry`, `hashes`, `lists`, `zsets`, `sets`, `streams`) between `UNLOGGED` and `LOGGED`.
- `pg_redis_ensure_schema(unlogged BOOLEAN) RETURNS BOOLEAN` — Provision all seven catalog tables and their indexes if absent.
