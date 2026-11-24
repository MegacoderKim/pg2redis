# Changelog

All notable changes to `pg_redis_core` are documented in this file.

## [0.5.0] - 2026-11-24

### Added
- **Optional Native TLS (`rediss://`) & mTLS (`src/tls.rs`, `src/worker.rs`, `src/config.rs`):**
  - Strictly **opt-in** via `pg_redis.tls_port` (default `0`, disabled) so plain non-TLS TCP on port `6379` continues working unchanged by default.
  - Supports all three deployment modes:
    1. **Plain TCP only (default):** `pg_redis.port = 6379`, `pg_redis.tls_port = 0`.
    2. **Dual-port (Plain TCP + TLS simultaneously):** `pg_redis.port = 6379`, `pg_redis.tls_port = 6380` with `pg_redis.tls_cert_file` and `pg_redis.tls_key_file`.
    3. **TLS-only (Encrypted only):** `pg_redis.port = 0`, `pg_redis.tls_port = 6380` (mirrors Redis `port 0` + `tls-port`).
  - Optional **mutual TLS (mTLS)** client certificate authentication via `pg_redis.tls_ca_cert_file` and `pg_redis.tls_auth_clients` (`no` | `optional` | `yes`).
  - Built on pure-Rust `tokio-rustls` + `ring` (TLS 1.2 & TLS 1.3) to avoid OpenSSL/`libssl` symbol conflicts with PostgreSQL, with non-blocking 10-second handshake timeouts so stalled handshakes never affect other clients.
  - **Live Certificate Hot-Reload on `SIGHUP`:** `SELECT pg_reload_conf();` reloads `pg_redis.tls_cert_file`, `pg_redis.tls_key_file`, `pg_redis.tls_ca_cert_file`, and `pg_redis.tls_auth_clients` without restarting PostgreSQL (keeping the previous working configuration on reload error).
  - Exposed `tls_port` in `INFO # Server` and `tls-port` / `tls-auth-clients` in `CONFIG GET`.
- **Upgrade Script & E2E Tests:**
  - Added `sql/pg_redis_core--0.4.1--0.5.0.sql`.
  - Added `tests/test_v050_tls.sh` and `tests/clients/pubsub/test_tls.mjs` verifying dual-port mode (`ioredis` + `redis-cli --tls`, cross-transport Pub/Sub, `BZPOPMIN` over TLS, plaintext rejection on TLS port), TLS-only + mTLS enforcement, and live `SIGHUP` certificate hot-reload, plus `#[pg_test]` opt-in default checks.

## [0.4.1] - 2026-11-23

### Fixed
- **CI / Pub/Sub Test Reliability:** Awaited `ioredis` `ready` state on all Pub/Sub subscriber clients in `tests/clients/pubsub/test_pubsub.mjs` before calling `subscribe()` / `psubscribe()`, eliminating an intermittent `ioredis@5.11.1` `readyCheck` (`INFO`) vs. RESP2 subscribed-mode race.
- **Upgrade Script:** Added `sql/pg_redis_core--0.4.0--0.4.1.sql`.

## [0.4.0] - 2026-11-23

### Security & Stability
- **Sandboxed Lua 5.1 VM (`src/lua.rs`):** Restricted `mlua` to safe standard libraries (`table`, `string`, `math`) and stripped `os`, `io`, `package`, `debug`, `dofile`, `loadfile`, `collectgarbage`, `module`, and `require`. Added configurable instruction-hook execution timeout (`pg_redis.lua_time_limit_ms`, default `5000` ms) to prevent runaway scripts from stalling the background worker.
- **Real `AUTH` & `HELLO ... AUTH` Enforcement (`pg_redis.requirepass`):** Added `SUPERUSER_ONLY` GUC `pg_redis.requirepass` with constant-time password verification supporting both `AUTH <pass>` and `AUTH default <pass>` as well as `HELLO <proto> AUTH default <pass>`, returning `NOAUTH` / `WRONGPASS` when configured.
- **Connection & Memory Safeguards:** Added `pg_redis.max_clients` (default `1000`), `pg_redis.max_frame_bytes` (default `64 MiB`), and bounded per-subscriber Pub/Sub queues (`pg_redis.pubsub_queue_size`, default `4096`) with automatic slow-consumer eviction.

### Added
- **Universal Key Expiration (`pg_redis_key_expiry`):** Extended `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, and background TTL eviction to work across **all 6 data types** (Strings, Hashes, Lists, Sorted Sets, Sets, and Streams), added Redis 7 `NX` / `XX` / `GT` / `LT` flags, and added `EXPIRETIME` / `PEXPIRETIME`.
- **Collection Scans & Session Helpers:** Added `HSCAN` (including `MATCH`, `COUNT`, `NOVALUES`), `SSCAN`, `ZSCAN`, fixed `SCAN ... TYPE <type>` filtering, and added `GETEX` (`EX`, `PX`, `EXAT`, `PXAT`, `PERSIST`) for rolling session TTLs (`connect-redis`) and BullMQ `FlowProducer` / dependency pagination.
- **Multi-Client Registry (`src/clients.rs`):** Upgraded `CLIENT LIST` (with `TYPE` / `ID` filters), `CLIENT INFO`, `CLIENT KILL` (`ID` / `ADDR`), and `INFO` `# Clients` (`connected_clients`, `maxclients`) across all active TCP connections so BullMQ `queue.getWorkers()` and `queue.getQueueEvents()` report live workers.
- **Pattern Pub/Sub (`src/pubsub.rs`):** Added `PSUBSCRIBE`, `PUNSUBSCRIBE`, `pmessage` push delivery, and live `PUBSUB NUMPAT` counts over the in-process `PubSubHub`.
- **Upgrade Script:** Added `sql/pg_redis_core--0.3.0--0.4.0.sql` creating `pg_redis_key_expiry`.

## [0.3.0] - 2026-11-22

### Added
- **Redis Pub/Sub (`src/pubsub.rs`) for Medusa 2.22.7 workflow notifications (`ioredis 5.11.1`):**
  - Named-channel `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBSUB CHANNELS [pattern]`, `PUBSUB NUMSUB [ch ...]`, `PUBSUB NUMPAT`, and `RESET`.
  - Exact Redis wire acknowledgements (`["subscribe", ch, count]`, `["unsubscribe", ch, remaining]`, `["unsubscribe", null, 0]`, `["message", ch, payload]`), RESP3 Push frames (`>N`), idempotent duplicate subscriptions, multi-channel subscriptions, single-channel and zero-arg unsubscribe-all, and exact `PUBLISH` live recipient counts.
  - Strict RESP2 subscribed-mode enforcement (allowing `SUBSCRIBE`, `UNSUBSCRIBE`, `PING` returning `["pong", msg]`, `QUIT`, and `RESET`) while permitting concurrent commands in RESP3.
  - Zero-copy in-process `PubSubHub` over per-connection Tokio `mpsc::unbounded_channel` on the single-threaded background worker `LocalSet`: zero PostgreSQL SPI overhead, arbitrary binary-safe payloads exceeding PostgreSQL's 8 KB `NOTIFY` limit (verified at 32 KB+), strict FIFO ordering, idle subscribers never block other clients or BullMQ workers, and RAII `PubSubClientGuard` cleanup on disconnect.
  - Support for `redis.call('PUBLISH', ...)` inside Lua 5.1 scripts and `PUBLISH` inside `MULTI`/`EXEC` transactions.
- **PostgreSQL 16 Build & Release Matrix:**
  - Parameterized `docker/Dockerfile` with `ARG PG_MAJOR=15` supporting both PostgreSQL 15 and PostgreSQL 16 (`cargo pgrx package --features pg15|pg16`).
  - Expanded `.github/workflows/release.yml` to build, smoke-test, and publish prebuilt extension binaries and multi-arch GHCR images for `pg15-linux-amd64`, `pg15-linux-arm64`, `pg16-linux-amd64`, and `pg16-linux-arm64`.
  - Added PostgreSQL extension upgrade script `sql/pg_redis_core--0.2.0--0.3.0.sql`.
- **End-to-End Test Suites:**
  - Added `tests/clients/pubsub/` pinning `ioredis@5.11.1` and `bullmq@5.13.0` covering all Pub/Sub edge cases, idle concurrency, disconnect/reconnect/resubscribe, 100-message FIFO ordering, binary safety, and 32 KB+ payloads.
  - Added `tests/test_solar_nest_isolation.sh` verifying `pg_redis.database=solar_nest_redis` isolation from application database `solar_nest` alongside Pub/Sub + BullMQ 5.13.0 on both PostgreSQL 15 and PostgreSQL 16.

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
