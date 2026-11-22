# Performance & Benchmarks

`pg_redis_core` implements all six architectural optimizations specified in the RFC to close the gap between disk-backed relational storage and in-memory Redis.

## Optimization Layers

### 1. In-Process L1 Hot-Key Cache (`moka`)
Hot string keys are cached directly inside the background worker process using `moka::sync::Cache`:

- **Sub-Millisecond Hot Reads:** L1 hits return straight from worker memory without opening a PostgreSQL transaction or invoking SPI.
- **Exact TTL Tracking:** Each entry records both the key's underlying Redis expiration deadline and a bounded `pg_redis.cache_ttl_ms` revalidation window.
- **Write-Through Consistency:** All RESP writes (`SET`, `INCR`, `DECR`, `APPEND`, `EXPIRE`, `PERSIST`, `DEL`, `FLUSHDB`) persist to PostgreSQL first and immediately update or invalidate the L1 entry.

### 2. SPI Prepared Statement Caching (`SPI_keepplan`)
On startup, the background worker pre-prepares high-frequency statements (`GET`, `SET`, `SET` with TTL, `SET KEEPTTL`, `HGET`, `HGETALL`, `HSET`, `ZSCORE`, and TTL eviction) via `SpiClient::prepare(...).keep()`. Retaining plans across transactions eliminates repeated SQL parsing, analysis, and planning overhead.

### 3. Pipelining & Frame Batching
When a client pipelines bursts of commands over a single socket, the Tokio connection handler drains all complete frames currently buffered in `BytesMut` and executes the entire DB-touching batch inside **one** `BackgroundWorker::transaction` block, amortizing transaction commit overhead across the batch.

### 4. Heap-Only Tuple (HOT) Updates
All four catalog tables are configured with `fillfactor = 80`. In-place updates to counters, sessions, and hash fields fit on the same 8KB heap page without modifying B-tree indexes, dramatically reducing vacuum pressure and write amplification.

### 5. Optional WAL Bypass (`UNLOGGED` Tables)
For pure caching workloads where crash-recovery durability is unnecessary, flip tables to `UNLOGGED` to bypass PostgreSQL Write-Ahead Logging (WAL):

```sql
SELECT pg_redis_set_unlogged(true);
```

Or configure `pg_redis.unlogged = on` in `postgresql.conf`.

### 6. Automated `ctid`-Batched TTL Eviction
Every `pg_redis.eviction_interval_ms` (default `5000ms`), the background worker executes a bounded, non-blocking cleanup pass backed by the partial index `idx_redis_strings_expiry`:

```sql
DELETE FROM pg_redis_strings
WHERE ctid IN (
    SELECT ctid FROM pg_redis_strings
    WHERE expires_at IS NOT NULL AND expires_at < NOW()
    LIMIT 1000
);
```

## Measured Docker Benchmark Results

Measured inside the `pg_redis_core:latest` Docker container (`postgres:15-bookworm`, Apple Silicon arm64) using `redis-benchmark -t set,get -n 1000 -c 10 -P 16 -q`:

| Workload | Mode | Throughput | Median Latency (`p50`) |
| --- | --- | --- | --- |
| Pipelined `SET` (`-P 16`, 10 clients) | Logged (ACID durable) | **8,000 req/s** | `17.18 ms` |
| Pipelined `SET` (`-P 16`, 10 clients) | `UNLOGGED` (WAL bypass) | **8,542 req/s** | `9.22 ms` |
| Hot-key `GET` (`-P 16`, 10 clients) | In-process L1 `moka` cache | **504,000 req/s** | `0.27 ms` |

Inspect live L1 cache hit/miss counters anytime via `redis-cli INFO`:

```text
# Stats
l1_cache_enabled:1
l1_cache_hits:1028
l1_cache_misses:10
l1_cache_invalidations:26
l1_cache_entries:34
```
