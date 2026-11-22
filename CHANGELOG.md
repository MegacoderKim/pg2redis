# Changelog

All notable changes to `pg_redis_core` are documented in this file.

## [0.2.0] - 2026-11-22

### Added
- **Embedded Lua 5.1 Scripting (`mlua` vendored Lua 5.1):**
  - `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `SCRIPT LOAD`, `SCRIPT EXISTS`, `SCRIPT FLUSH` with SHA1-cached compiled Lua functions and exact `NOSCRIPT` errors for `ioredis`.
  - Full Redis Lua sandbox globals: binary-safe `KEYS`/`ARGV`, `redis.call`, `redis.pcall`, `redis.status_reply`, `redis.error_reply`, `redis.sha1hex`, `redis.log`, `cjson`, `cmsgpack` (`rmpv`), and `bit`.
  - Exact Redis Lua ↔ RESP conversions executing atomically inside the active PostgreSQL SPI transaction.
- **Redis Streams (`pg_redis_streams`):**
  - `XADD` (monotonic `*`, `<ms>-*`, `<ms>-<seq>`, `NOMKSTREAM`, `MAXLEN [~|=]`, `MINID`), `XTRIM`, `XLEN`, `XRANGE`, `XREVRANGE`, `XDEL`, and `XREAD [COUNT] [BLOCK] STREAMS` (including `$` cursors).
- **Cooperative Blocking Operations (`KeyNotifier`):**
  - Async non-polling `BZPOPMIN`, `BZPOPMAX`, `BLPOP`, `BRPOP`, and `XREAD BLOCK` woken immediately after PostgreSQL transaction commits.
- **Redis Sets (`pg_redis_sets`):**
  - `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SPOP`.
- **BullMQ 5.13.0 / Medusa Command Parity:**
  - Added `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `LTRIM`, `LPOS`, `RPOPLPUSH`, `LMOVE`, `RENAME`, and `maxmemory_policy:noeviction` in `INFO` and `CONFIG GET`.
  - Added PostgreSQL extension upgrade script `sql/pg_redis_core--0.1.0--0.2.0.sql`.
  - Added end-to-end Dockerized `bullmq@5.13.0` integration suite (`tests/clients/bullmq`).

## [0.1.0] - 2026-11-22

### Added
- Initial `pgrx` 0.11 PostgreSQL 15+ extension embedding a single-threaded Tokio RESP2/RESP3 TCP listener (`0.0.0.0:6379`).
- Catalog tables with `fillfactor = 80`: `pg_redis_strings`, `pg_redis_hashes`, `pg_redis_lists`, `pg_redis_zsets`.
- `SPI_keepplan` prepared statement cache, pipelined transaction batching, `MULTI`/`EXEC`/`DISCARD`, in-process `moka` L1 hot-key cache, optional `UNLOGGED` WAL bypass, and `ctid`-batched TTL eviction.
- Multi-language Dockerized verification suites (Python `FastAPI` + `redis-py`, Kotlin `Jedis`, Go `go-redis/v9`, raw TCP RESP, and `cargo pgrx test`/`run pg15`) plus MkDocs Material documentation.
