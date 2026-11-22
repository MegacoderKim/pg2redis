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
pg_redis.unlogged = off
pg_redis.cache_enabled = on
pg_redis.cache_capacity = 10000
pg_redis.cache_ttl_ms = 500
pg_redis.eviction_interval_ms = 5000
```

!!! note "Dynamic extension loading (`CREATE EXTENSION` without preloading)"
    If you load `pg_redis_core` with `CREATE EXTENSION pg_redis_core;` on a server without `shared_preload_libraries = 'pg_redis_core'`, the extension cleanly skips registering the background worker and adjusts GUC contexts so `redis_dispatcher(...)` and all catalog tables remain usable from SQL.

## GUC Reference

| Parameter | Type | Default | Context | Description |
| --- | --- | --- | --- | --- |
| `pg_redis.bind_address` | `text` | `'0.0.0.0'` | Postmaster | Interface address for the TCP RESP listener. |
| `pg_redis.port` | `integer` | `6379` | Postmaster | TCP port bound by the embedded RESP server (`1`–`65535`). |
| `pg_redis.database` | `text` | `'postgres'` | Postmaster | Target PostgreSQL database connected by the background worker via SPI. |
| `pg_redis.unlogged` | `boolean` | `off` | SIGHUP | When `on`, auto-provisioned catalog tables are created as `UNLOGGED` to bypass WAL. |
| `pg_redis.cache_enabled` | `boolean` | `on` | SIGHUP | Enables the in-process `moka` L1 hot-key cache for `GET` reads. |
| `pg_redis.cache_capacity` | `integer` | `10000` | Postmaster | Maximum number of entries retained in the L1 hot-key cache (`0`–`10000000`). |
| `pg_redis.cache_ttl_ms` | `integer` | `500` | SIGHUP | Max milliseconds an L1 entry stays cached before revalidating against PostgreSQL. |
| `pg_redis.eviction_interval_ms` | `integer` | `5000` | SIGHUP | Interval in milliseconds between automated `ctid`-batched TTL eviction passes. |

Reload `SIGHUP` parameters live without restarting PostgreSQL:

```sql
ALTER SYSTEM SET pg_redis.cache_ttl_ms = 2000;
SELECT pg_reload_conf();
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

Switch all four catalog tables (`pg_redis_strings`, `pg_redis_hashes`, `pg_redis_lists`, `pg_redis_zsets`) between `UNLOGGED` (WAL bypass for maximum write throughput) and `LOGGED` (crash-safe ACID durability):

```sql
-- Enable WAL bypass for pure caching workloads
SELECT pg_redis_set_unlogged(true);

-- Restore full WAL durability
SELECT pg_redis_set_unlogged(false);
```

### `pg_redis_evict_expired()`

Trigger one `ctid`-batched TTL eviction cycle (`LIMIT 1000`) manually and return the number of expired rows deleted:

```sql
SELECT pg_redis_evict_expired();
```

### `pg_redis_ensure_schema(unlogged BOOLEAN)`

Idempotently create all four `pg_redis_*` tables and their indexes if they do not already exist:

```sql
SELECT pg_redis_ensure_schema(false);
```
