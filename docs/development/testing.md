# Building & Testing

The entire build, test, and documentation pipeline is fully containerized with Docker so you don't need local Rust, `cargo-pgrx`, PostgreSQL 15 headers, Go, or JDK toolchains installed.

## One-Command Full Verification

Run the master end-to-end test harness:

```bash
./tests/run_all_tests.sh
```

This script verifies all RFC acceptance criteria in sequence:

1. **Runtime Docker Build (`docker/Dockerfile` `runtime` target):** Compiles `pg_redis_core` with `cargo pgrx package` (`PG_MAJOR=15` or `16`) and installs the `.so`, `.control`, and generated `.sql` into `postgres:${PG_MAJOR}-bookworm`.
2. **Raw TCP RESP Compliance (`tests/raw_resp_test.sh`):** Validates `*1\r\n$4\r\nPING\r\n` -> `+PONG\r\n`, inline `PING`, pipelined multi-command bursts, and RESP3 `HELLO 3`.
3. **Python Integration Suite (`test-python`):** Runs `pytest` covering FastAPI gateway endpoints, binary-safe Strings, TTL expiration + `pg_redis_evict_expired()`, Hashes, Lists, Sorted Sets, pipelining, L1 cache hits, and direct PostgreSQL table parity via `psycopg`.
4. **Go Integration Suite (`test-go`):** Runs `go-redis/v9` with default RESP3 negotiation across all data types, `ZRangeWithScores`, `Pipeline`, and `TxPipeline`.
5. **Kotlin Integration Suite (`test-kotlin`):** Runs `Jedis 5.2.0` across Strings, Counters, Hashes, Lists, Sorted Sets, and pipelining.
6. **BullMQ 5.13.0 Suite (`test-bullmq`):** Runs unmodified `bullmq@5.13.0` covering Lua 5.1 (`EVAL`/`EVALSHA`/`SCRIPT LOAD`/`cjson`/`cmsgpack`), Streams (`XADD`/`XREAD BLOCK`), and cooperative `BZPOPMIN`.
7. **Pub/Sub & v0.4.0 Suite (`test-pubsub`):** Runs `ioredis@5.11.1` + `bullmq@5.13.0` covering named & pattern Pub/Sub (`SUBSCRIBE`/`PSUBSCRIBE`/`PUBSUB`), universal TTL across all 6 data types (`NX|XX|GT|LT`, `GETEX`), `HSCAN`/`SSCAN`/`ZSCAN`/`SCAN TYPE`, multi-client `CLIENT LIST`/`KILL`, BullMQ `FlowProducer` `getDependencies()` & `getWorkers()`, and Lua sandbox verification.
8. **v0.4.0 Security & Limits Suite (`./tests/test_v040_security.sh`):** Verifies `pg_redis.requirepass` (`NOAUTH`/`WRONGPASS`/`AUTH`/`HELLO AUTH`/`RESET`), `pg_redis.lua_time_limit_ms` runaway script termination, `pg_redis.max_clients`, and `pg_redis.max_frame_bytes`.
9. **v0.5.0 Optional TLS & mTLS Suite (`./tests/test_v050_tls.sh`):** Verifies dual-port mode (plain `:6379` + `rediss://:6380` with cross-transport Pub/Sub, `BZPOPMIN` over TLS, and `redis-cli --tls`), TLS-only + mTLS mode (`port = 0`, `tls_auth_clients = 'yes'`), and live `SIGHUP` certificate hot-reload (`SELECT pg_reload_conf();`).
10. **PostgreSQL 15 & 16 DB Isolation Suite (`./tests/test_solar_nest_isolation.sh 15 16`):** Exercises both PG 15 and PG 16 with `pg_redis.database=solar_nest_redis` kept isolated from application DB `solar_nest` alongside `test-pubsub` and `test-bullmq`.
11. **In-PostgreSQL `pgrx` Unit Tests & `cargo pgrx run` (`dev` target):** Runs `cargo pgrx test pg15` / `pg16` (all 12 `#[pg_test]` cases) and smoke-tests `cargo pgrx run`.

## Running `cargo pgrx` Directly via Docker

```bash
# Build the dev image with Rust, cargo-pgrx 0.11.4, and PG15 headers (pass --build-arg PG_MAJOR=16 for PG16)
docker build --target dev -f docker/Dockerfile -t pg_redis_core:dev .

# Run in-PostgreSQL Rust unit tests (PG15)
docker run --rm pg_redis_core:dev cargo pgrx test pg15

# Or build & test against PostgreSQL 16
docker build --target dev --build-arg PG_MAJOR=16 -f docker/Dockerfile -t pg_redis_core:dev-pg16 .
docker run --rm pg_redis_core:dev-pg16 cargo pgrx test pg16 --no-default-features --features pg16

# Launch an interactive psql session via cargo pgrx run pg15
docker run --rm -it pg_redis_core:dev cargo pgrx run pg15
```

## Previewing Documentation Locally

Serve this MkDocs Material site with live reload on `http://localhost:8080`:

```bash
docker compose --profile docs up --build docs
```

Or run a strict production build check:

```bash
docker compose --profile docs run --rm docs mkdocs build --strict
```

## Branching & GitHub Actions Release Workflow

All changes follow a branch-and-PR workflow into `main`:

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feat/my-change
   ```
2. Commit changes, push the branch, and open a Pull Request against `main`.
3. GitHub Actions CI (`.github/workflows/ci.yml`) validates `mkdocs build --strict` and the full Dockerized integration + `cargo pgrx test pg15` suite before merge.
4. To cut a release (`vX.Y.Z`):
   - Bump `version` in `Cargo.toml`, add a `sql/pg_redis_core--<prev>--<next>.sql` upgrade script, and update `CHANGELOG.md` in your PR.
   - Merge the PR into `main`. Once **CI** (`.github/workflows/ci.yml`) passes on `main`, **Release** (`.github/workflows/release.yml`) automatically triggers via `workflow_run`:
     - If tag `vX.Y.Z` already exists on `origin`, it skips cleanly (so docs/CI-only merges never create duplicate releases).
     - If tag `vX.Y.Z` is new, it natively builds & smoke-tests `linux/amd64` and `linux/arm64` extension tarballs for both **PostgreSQL 15** and **PostgreSQL 16**, publishes multi-arch Docker images (`:vX.Y.Z-pg15`, `:vX.Y.Z-pg16`, and `:vX.Y.Z`) to `ghcr.io/megacoderkim/pg2redis`, pushes annotated tag `vX.Y.Z` on the tested commit, and publishes the GitHub Release with `SHA256SUMS.txt`.
   - Manual tag pushes (`git tag -a vX.Y.Z && git push origin vX.Y.Z`) and `workflow_dispatch` remain supported as fallbacks.

## Project Layout

```text
pg2redis/
├── Cargo.toml                           # Rust crate manifest (pgrx 0.11.4, tokio, redis-protocol 6.0, mlua 0.10, moka)
├── pg_redis_core.control                # PostgreSQL extension control file
├── mkdocs.yml                           # Material for MkDocs configuration
├── docker-compose.yml                   # pg-redis, fastapi-gateway, test runners, and docs preview
├── .github/workflows/
│   ├── ci.yml                           # MkDocs strict check + full Dockerized & PG 15/16 CI suite
│   ├── release.yml                      # Auto-release on main after CI passes (PG15/PG16 amd64/arm64 + GHCR)
│   └── docs.yml                         # GitHub Pages MkDocs Material deployment
├── docker/
│   ├── Dockerfile                       # Multi-stage PG15/PG16 builder, dev (pgrx test/run), and runtime targets
│   ├── Dockerfile.docs                  # MkDocs Material build/preview container
│   ├── init.sql                         # CREATE EXTENSION IF NOT EXISTS pg_redis_core;
│   └── postgresql.conf                  # Reference PostgreSQL + pg_redis.* GUC configuration
├── sql/                                 # Incremental PostgreSQL extension upgrade scripts (0.1.0 -> 0.5.0)
├── src/
│   ├── lib.rs                           # extension_sql!, _PG_init, redis_dispatcher, SQL helpers, #[pg_test]
│   ├── config.rs                        # PostgreSQL GUC definitions (pg_redis.* + constant-time AUTH compare)
│   ├── cache.rs                         # In-process moka L1 hot-key cache
│   ├── clients.rs                       # Live multi-connection ClientRegistry (CLIENT LIST/INFO/KILL, INFO)
│   ├── lua.rs                           # Sandboxed Lua 5.1 VM (mlua) with timeout hook, cjson, cmsgpack, bit
│   ├── notify.rs                        # Per-key cooperative async KeyNotifier (BZPOPMIN, BLPOP, XREAD BLOCK)
│   ├── protocol.rs                      # Zero-copy RESP2/RESP3 codec (redis-protocol 6.0)
│   ├── pubsub.rs                        # In-process named & pattern PubSubHub with bounded slow-consumer queues
│   ├── storage.rs                       # 7-table catalog DDL, SPI PreparedPlans, universal ctid TTL eviction
│   ├── tls.rs                           # Optional native TLS 1.2/1.3 & mTLS acceptor (tokio-rustls + ring)
│   ├── worker.rs                        # BGWorker + Tokio current_thread/LocalSet plain/TLS TCP listener & limits
│   └── commands/                        # Command handlers (strings, keys, hashes, lists, zsets, sets, streams, server)
├── docs/                                # MkDocs Material documentation source
└── tests/
    ├── raw_resp_test.sh                 # Raw TCP RESP2/RESP3 verification
    ├── test_v040_security.sh            # AUTH/requirepass, Lua timeout, max_clients & max_frame_bytes tests
    ├── test_v050_tls.sh                 # Optional TLS (rediss://), dual-port, TLS-only + mTLS & SIGHUP reload tests
    ├── test_solar_nest_isolation.sh     # PG 15 & 16 isolated-DB + Pub/Sub + BullMQ suite
    ├── run_all_tests.sh                 # Master end-to-end verification runner
    └── clients/
        ├── bullmq/                      # Unmodified bullmq@5.13.0 integration suite
        ├── pubsub/                      # ioredis@5.11.1 Pub/Sub, v0.4.0 features & security tests
        ├── python/                      # FastAPI gateway + redis-py + psycopg pytest suite
        ├── kotlin/                      # Jedis 5.2.0 Gradle project
        └── go/                          # go-redis/v9 RESP3 Go module
```
