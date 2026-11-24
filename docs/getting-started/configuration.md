# Configuration & SQL API

`pg_redis_core` is configured via native PostgreSQL GUC parameters (`postgresql.conf` or `-c` CLI flags) and exposes SQL helper functions for runtime administration.

## Enabling the Background Worker

To start the embedded TCP RESP listener automatically when PostgreSQL boots, add `pg_redis_core` to `shared_preload_libraries`:

```ini title="postgresql.conf"
shared_preload_libraries = 'pg_redis_core'
max_worker_processes = 16

pg_redis.bind_address = '0.0.0.0'
pg_redis.port = 6379
pg_redis.database = 'postgres'
pg_redis.databases = 16
pg_redis.unlogged = off
pg_redis.cache_enabled = on
pg_redis.cache_capacity = 10000
pg_redis.cache_ttl_ms = 500
pg_redis.eviction_interval_ms = 5000
pg_redis.requirepass = ''
pg_redis.lua_time_limit_ms = 5000
pg_redis.max_clients = 1000
pg_redis.max_frame_bytes = 67108864
pg_redis.pubsub_queue_size = 4096

# Optional native TLS / mTLS (disabled by default — strictly opt-in)
pg_redis.tls_port = 0
# pg_redis.tls_cert_file = '/etc/pg_redis/server.crt'
# pg_redis.tls_key_file = '/etc/pg_redis/server.key'
# pg_redis.tls_ca_cert_file = '/etc/pg_redis/ca.crt'
# pg_redis.tls_auth_clients = 'no'   # 'no' | 'optional' | 'yes'
```

!!! note "Dynamic extension loading (`CREATE EXTENSION` without preloading)"
    If you load `pg_redis_core` with `CREATE EXTENSION pg_redis_core;` on a server without `shared_preload_libraries = 'pg_redis_core'`, the extension cleanly skips registering the background worker and adjusts GUC contexts so `redis_dispatcher(...)` and all catalog tables remain usable from SQL.

## GUC Reference

| Parameter | Type | Default | Context | Description |
| --- | --- | --- | --- | --- |
| `pg_redis.bind_address` | `text` | `'0.0.0.0'` | Postmaster | Interface address for the TCP RESP listener. |
| `pg_redis.port` | `integer` | `6379` | Postmaster | Plain TCP port bound by the embedded RESP server (`0`–`65535`; `0` disables plain TCP for TLS-only mode). |
| `pg_redis.tls_port` | `integer` | `0` | Postmaster | Optional TLS (`rediss://`) TCP port (`0`–`65535`; `0` disables TLS — strictly opt-in). |
| `pg_redis.tls_cert_file` | `text` | *(unset)* | SIGHUP (`SUPERUSER_ONLY`) | Path to PEM server certificate chain (required when `pg_redis.tls_port > 0`). |
| `pg_redis.tls_key_file` | `text` | *(unset)* | SIGHUP (`SUPERUSER_ONLY`) | Path to PEM server private key (required when `pg_redis.tls_port > 0`). |
| `pg_redis.tls_ca_cert_file` | `text` | *(unset)* | SIGHUP (`SUPERUSER_ONLY`) | Optional path to PEM CA bundle for mTLS client certificate verification. |
| `pg_redis.tls_auth_clients` | `text` | `'no'` | SIGHUP (`SUPERUSER_ONLY`) | mTLS policy: `'no'` (default), `'optional'`, or `'yes'` (require valid CA-signed client cert). |
| `pg_redis.database` | `text` | `'postgres'` | Postmaster | Target PostgreSQL database connected by the background worker via SPI. |
| `pg_redis.databases` | `integer` | `16` | SIGHUP | Number of logical Redis databases available via `SELECT` (`1`–`1024`; indices `0`..`N-1`). |
| `pg_redis.unlogged` | `boolean` | `off` | SIGHUP | When `on`, auto-provisioned catalog tables are created as `UNLOGGED` to bypass WAL. |
| `pg_redis.cache_enabled` | `boolean` | `on` | SIGHUP | Enables the in-process `moka` L1 hot-key cache for `GET` reads. |
| `pg_redis.cache_capacity` | `integer` | `10000` | Postmaster | Maximum number of entries retained in the L1 hot-key cache (`0`–`10000000`). |
| `pg_redis.cache_ttl_ms` | `integer` | `500` | SIGHUP | Max milliseconds an L1 entry stays cached before revalidating against PostgreSQL. |
| `pg_redis.eviction_interval_ms` | `integer` | `5000` | SIGHUP | Interval in milliseconds between automated `ctid`-batched TTL eviction passes (strings + non-string `pg_redis_key_expiry`). |
| `pg_redis.requirepass` | `text` | *(unset)* | SIGHUP (`SUPERUSER_ONLY`) | Optional password enforced via `AUTH` / `HELLO ... AUTH` using constant-time comparison. Unset/empty disables auth. |
| `pg_redis.lua_time_limit_ms` | `integer` | `5000` | SIGHUP | Maximum execution time in milliseconds for a single Lua `EVAL`/`EVALSHA` call (`0` disables). |
| `pg_redis.max_clients` | `integer` | `1000` | SIGHUP | Maximum concurrent RESP TCP connections accepted by the worker (`1`–`100000`). |
| `pg_redis.max_frame_bytes` | `integer` | `67108864` | SIGHUP | Maximum buffered RESP request size per connection (default 64 MiB). |
| `pg_redis.pubsub_queue_size` | `integer` | `4096` | SIGHUP | Bounded message queue capacity per Pub/Sub subscriber before slow-consumer eviction. |

Reload `SIGHUP` parameters live without restarting PostgreSQL:

```sql
ALTER SYSTEM SET pg_redis.cache_ttl_ms = 2000;
SELECT pg_reload_conf();
```

## Security, Sandboxing & Resource Limits

- **Optional Native TLS & mTLS (`pg_redis.tls_port`):** TLS is **strictly opt-in** (`pg_redis.tls_port = 0` by default) so non-TLS deployments on port `6379` work unchanged. Powered by pure-Rust `tokio-rustls` + `ring` (TLS 1.2 & TLS 1.3), supporting three modes:
  - **Plain TCP only (default):** `pg_redis.port = 6379`, `pg_redis.tls_port = 0`.
  - **Dual-port (Plain + TLS side-by-side):** `pg_redis.port = 6379`, `pg_redis.tls_port = 6380` with `pg_redis.tls_cert_file` and `pg_redis.tls_key_file`.
  - **TLS-only (Encrypted only):** `pg_redis.port = 0`, `pg_redis.tls_port = 6380` (mirrors Redis `port 0` + `tls-port`), optionally combined with `pg_redis.tls_auth_clients = 'yes'` and `pg_redis.tls_ca_cert_file` for mutual TLS (mTLS).
  - **Live Certificate Rotation:** Rotate PEM files on disk and run `SELECT pg_reload_conf();` (`SIGHUP`) to hot-reload certificates without restarting PostgreSQL or dropping active connections. If a reload fails, the worker logs a warning and continues serving with the previous valid configuration.
- **Authentication (`pg_redis.requirepass`):** Defined with `SUPERUSER_ONLY` so non-superuser SQL roles cannot read or alter the password. When non-empty, every new RESP connection must authenticate via `AUTH <password>`, `AUTH default <password>`, or `HELLO <2|3> AUTH default <password>` (verified using constant-time byte comparison). Unauthenticated commands fail with `NOAUTH Authentication required`, invalid passwords return `WRONGPASS`, and `RESET` clears per-connection authentication state.
- **Lua 5.1 Sandboxing & Timeouts (`pg_redis.lua_time_limit_ms`):** The embedded `mlua` Lua 5.1 VM loads only safe standard libraries (`table`, `string`, `math`) and removes `os`, `io`, `package`, `debug`, `dofile`, `loadfile`, `collectgarbage`, `module`, and `require`. An instruction hook aborts runaway scripts once execution exceeds `pg_redis.lua_time_limit_ms` (default `5000` ms; `0` disables).
- **Connection & Frame Bounds (`pg_redis.max_clients`, `pg_redis.max_frame_bytes`):** Rejects incoming TCP connections once `connected_clients >= pg_redis.max_clients` (default `1000`) with `-ERR max number of clients reached`, and disconnects clients whose buffered RESP frame exceeds `pg_redis.max_frame_bytes` (default `64 MiB`).
- **Pub/Sub Backpressure (`pg_redis.pubsub_queue_size`):** Each subscriber uses a bounded in-process channel (default `4096` messages). If a slow subscriber overflows its queue, it is automatically evicted so healthy subscribers and publishers are never stalled.

## Database Isolation (`pg_redis.database`)

To keep Redis catalog tables completely separate from your application schema, point `pg_redis.database` at a dedicated database on the same PostgreSQL cluster:

```ini title="postgresql.conf"
pg_redis.database = 'app_redis'
```

Create the database and enable the extension inside it (`CREATE DATABASE app_redis;` followed by `\c app_redis` and `CREATE EXTENSION pg_redis_core;`). The background worker attaches exclusively to `app_redis`, leaving your application database untouched.

## Upgrading Between Extension Versions

`pg_redis_core` ships incremental SQL upgrade scripts (`sql/pg_redis_core--0.1.0--0.2.0.sql`, `0.2.0--0.3.0.sql`, `0.3.0--0.4.0.sql`, `0.4.0--0.4.1.sql`, `0.4.1--0.5.0.sql`, `0.5.0--0.6.0.sql`). After installing a newer extension package or image, run:

```sql
ALTER EXTENSION pg_redis_core UPDATE;
```

## SQL Functions

### `redis_dispatcher(cmd, key, arg1, arg2)`

Execute Redis commands directly inside any PostgreSQL SQL session and receive the RESP2-encoded response string:

```sql
SELECT redis_dispatcher('PING', NULL, NULL, NULL);
-- +PONG\r\n

SELECT redis_dispatcher('SET', 'promo:code', 'SAVE20', NULL);
-- +OK\r\n

SELECT redis_dispatcher('GET', 'promo:code', NULL, NULL);
-- $6\r\nSAVE20\r\n

SELECT redis_dispatcher('ZADD', 'scores', '99.5', 'alice');
-- :1\r\n
```

### `pg_redis_set_unlogged(unlogged BOOLEAN)`

Switch all seven catalog tables (`pg_redis_strings`, `pg_redis_key_expiry`, `pg_redis_hashes`, `pg_redis_lists`, `pg_redis_zsets`, `pg_redis_sets`, `pg_redis_streams`) in canonical lock order between `UNLOGGED` (WAL bypass for maximum write throughput) and `LOGGED` (crash-safe ACID durability):

```sql
-- Enable WAL bypass for pure caching workloads
SELECT pg_redis_set_unlogged(true);

-- Restore full WAL durability
SELECT pg_redis_set_unlogged(false);
```

### `pg_redis_evict_expired()`

Trigger one `ctid`-batched TTL eviction cycle (`LIMIT 1000`) manually across both `pg_redis_strings` and `pg_redis_key_expiry` (cascading expired non-string keys out of hashes, lists, sorted sets, sets, and streams) and return the total number of expired entries removed:

```sql
SELECT pg_redis_evict_expired();
```

### `pg_redis_ensure_schema(unlogged BOOLEAN)`

Idempotently create all seven `pg_redis_*` tables and their indexes if they do not already exist:

```sql
SELECT pg_redis_ensure_schema(false);
```
