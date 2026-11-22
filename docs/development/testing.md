# Building & Testing

The entire build, test, and documentation pipeline is fully containerized with Docker so you don't need local Rust, `cargo-pgrx`, PostgreSQL 15 headers, Go, or JDK toolchains installed.

## One-Command Full Verification

Run the master end-to-end test harness:

```bash
./tests/run_all_tests.sh
```

This script verifies all RFC acceptance criteria in sequence:

1. **Runtime Docker Build (`docker/Dockerfile` `runtime` target):** Compiles `pg_redis_core` with `cargo pgrx package` (`pg15`) and installs the `.so`, `.control`, and generated `.sql` into `postgres:15-bookworm`.
2. **Raw TCP RESP Compliance (`tests/raw_resp_test.sh`):** Validates `*1\r\n$4\r\nPING\r\n` -> `+PONG\r\n`, inline `PING`, pipelined multi-command bursts, and RESP3 `HELLO 3`.
3. **Python Integration Suite (`test-python`):** Runs `pytest` covering FastAPI gateway endpoints, binary-safe Strings, TTL expiration + `pg_redis_evict_expired()`, Hashes, Lists, Sorted Sets, pipelining, L1 cache hits, and direct PostgreSQL table parity via `psycopg`.
4. **Go Integration Suite (`test-go`):** Runs `go-redis/v9` with default RESP3 negotiation across all data types, `ZRangeWithScores`, `Pipeline`, and `TxPipeline`.
5. **Kotlin Integration Suite (`test-kotlin`):** Runs `Jedis 5.2.0` across Strings, Counters, Hashes, Lists, Sorted Sets, and pipelining.
6. **In-PostgreSQL `pgrx` Unit Tests & `cargo pgrx run pg15` (`dev` target):** Runs `cargo pgrx test pg15` (all 7 `#[pg_test]` cases) and smoke-tests `cargo pgrx run pg15`.

## Running `cargo pgrx` Directly via Docker

```bash
# Build the dev image with Rust, cargo-pgrx 0.11.4, and PG15 headers
docker build --target dev -f docker/Dockerfile -t pg_redis_core:dev .

# Run in-PostgreSQL Rust unit tests
docker run --rm pg_redis_core:dev cargo pgrx test pg15

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

## Project Layout

```text
pg2redis/
├── Cargo.toml                 # Rust crate manifest (pgrx 0.11.4, tokio, redis-protocol 6.0, moka)
├── pg_redis_core.control      # PostgreSQL extension control file
├── mkdocs.yml                 # Material for MkDocs configuration
├── docker-compose.yml         # pg-redis, fastapi-gateway, test runners, and docs preview
├── docker/
│   ├── Dockerfile             # Multi-stage PG15 builder, dev (pgrx test/run), and runtime targets
│   ├── Dockerfile.docs        # MkDocs Material build/preview container
│   ├── init.sql               # CREATE EXTENSION IF NOT EXISTS pg_redis_core;
│   └── postgresql.conf        # Reference PostgreSQL + pg_redis.* GUC configuration
├── src/
│   ├── lib.rs                 # extension_sql!, _PG_init, redis_dispatcher, SQL helpers, #[pg_test]
│   ├── config.rs              # PostgreSQL GUC definitions (pg_redis.*)
│   ├── cache.rs               # In-process moka L1 hot-key cache
│   ├── protocol.rs            # Zero-copy RESP2/RESP3 codec (redis-protocol 6.0)
│   ├── storage.rs             # Catalog DDL, SPI PreparedPlans, ctid TTL eviction
│   ├── worker.rs              # BGWorker + Tokio current_thread/LocalSet TCP listener
│   └── commands/              # Modular command handlers (strings, keys, hashes, lists, zsets, server)
├── docs/                      # MkDocs Material documentation source
└── tests/
    ├── raw_resp_test.sh       # Raw TCP RESP2/RESP3 verification
    ├── run_all_tests.sh       # Master end-to-end verification runner
    └── clients/
        ├── python/            # FastAPI gateway + redis-py + psycopg pytest suite
        ├── kotlin/            # Jedis 5.2.0 Gradle project
        └── go/                # go-redis/v9 RESP3 Go module
```
