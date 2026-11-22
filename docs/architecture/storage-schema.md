# Relational Storage Schema

All Redis data structures map directly onto four purpose-built PostgreSQL catalog tables provisioned during `CREATE EXTENSION pg_redis_core` (and auto-provisioned on worker startup if missing).

Every table is created with `WITH (fillfactor = 80)` to reserve 20% free space per 8KB heap page for **Heap-Only Tuple (HOT)** updates.

## Catalog DDL

```sql
-- 1. Strings (GET, SET, INCR, DECR, APPEND, EXPIRE, TTL, DEL)
CREATE TABLE IF NOT EXISTS pg_redis_strings (
    key TEXT PRIMARY KEY,
    value BYTEA,
    expires_at TIMESTAMPTZ
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_strings_expiry
ON pg_redis_strings(expires_at)
WHERE expires_at IS NOT NULL;

-- 2. Hashes (HSET, HGET, HMGET, HGETALL, HDEL, HINCRBY)
CREATE TABLE IF NOT EXISTS pg_redis_hashes (
    key TEXT,
    field TEXT,
    value TEXT,
    PRIMARY KEY (key, field)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_hashes_key
ON pg_redis_hashes(key);

-- 3. Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM)
CREATE TABLE IF NOT EXISTS pg_redis_lists (
    key TEXT,
    seq BIGSERIAL,
    value TEXT,
    PRIMARY KEY (key, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq
ON pg_redis_lists(key, seq);

-- 4. Sorted Sets (ZADD, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREM, ZSCORE, ZRANK)
CREATE TABLE IF NOT EXISTS pg_redis_zsets (
    key TEXT,
    member TEXT,
    score DOUBLE PRECISION,
    PRIMARY KEY (key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_zsets_score
ON pg_redis_zsets(key, score);
```

## Mapping Design Highlights

### Strings (`pg_redis_strings`)
- **Binary Safe:** Values are stored as `BYTEA`, preserving arbitrary binary blobs (`\x00`, non-UTF8 bytes, embedded `\r\n`).
- **Lazy + Active Expiry:** All reads filter `expires_at IS NULL OR expires_at > NOW()`, guaranteeing an expired key is never returned even before the background eviction loop removes its row.
- **Partial Expiry Index:** `idx_redis_strings_expiry` only indexes rows where `expires_at IS NOT NULL`, keeping the index tiny when most keys are persistent.

### Hashes (`pg_redis_hashes`)
- **Atomic New-Field Counting:** `HSET` uses PostgreSQL's `RETURNING (xmax = 0)` on `INSERT ... ON CONFLICT (key, field) DO UPDATE`, distinguishing newly inserted fields from overwritten fields in a single SQL statement to match Redis's exact integer return value.

### Lists (`pg_redis_lists`)
- **O(1) Bidirectional Push Semantics:** Although `seq` is declared as `BIGSERIAL` (signed 64-bit `BIGINT`), `pg_redis_core` supports both ends cleanly:
    - **`LPUSH`** inserts at `COALESCE(MIN(seq), 1) - 1` (growing downward: `0, -1, -2, ...`).
    - **`RPUSH`** inserts at `COALESCE(MAX(seq), 0) + 1` (growing upward: `1, 2, 3, ...`).
- Ordering by `seq ASC` always yields exact Redis list order for `LRANGE`, `LINDEX`, `LPOP`, and `RPOP` (including negative index clamping).

### Sorted Sets (`pg_redis_zsets`)
- **Deterministic Tie-Breaking:** Queries order by `score ASC, member ASC` (or `score DESC, member DESC` for `REV` / `ZREVRANGE`), matching Redis's exact lexicographical tie-breaking on equal scores.
- **Flag Support:** `ZADD` supports `NX`, `XX`, `GT`, `LT`, `CH`, and `INCR`, as well as `+inf` / `-inf` scores and exclusive score bounds like `(100`.
