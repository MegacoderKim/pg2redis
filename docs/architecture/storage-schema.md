# Relational Storage Schema

All Redis data structures map directly onto purpose-built PostgreSQL catalog tables provisioned during `CREATE EXTENSION pg_redis_core` (and auto-provisioned on worker startup if missing).

Every table is created with `WITH (fillfactor = 80)` to reserve 20% free space per 8KB heap page for **Heap-Only Tuple (HOT)** updates.

## Catalog DDL

```sql
-- 1. Strings (GET, SET, INCR, DECR, APPEND, EXPIRE, TTL, DEL)
CREATE TABLE IF NOT EXISTS pg_redis_strings (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    value BYTEA,
    expires_at TIMESTAMPTZ,
    PRIMARY KEY (db, key)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_strings_expiry
ON pg_redis_strings(expires_at)
WHERE expires_at IS NOT NULL;

-- 2. Hashes (HSET, HGET, HMGET, HGETALL, HDEL, HINCRBY)
CREATE TABLE IF NOT EXISTS pg_redis_hashes (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    field TEXT,
    value TEXT,
    PRIMARY KEY (db, key, field)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_hashes_key
ON pg_redis_hashes(db, key);

-- 3. Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LSET, LREM)
CREATE TABLE IF NOT EXISTS pg_redis_lists (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    seq BIGSERIAL,
    value TEXT,
    PRIMARY KEY (db, key, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq
ON pg_redis_lists(db, key, seq);

-- 4. Sorted Sets (ZADD, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREM, ZSCORE, ZRANK, ZPOPMIN, BZPOPMIN)
CREATE TABLE IF NOT EXISTS pg_redis_zsets (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    member TEXT,
    score DOUBLE PRECISION,
    PRIMARY KEY (db, key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_zsets_score
ON pg_redis_zsets(db, key, score);

-- 5. Sets (SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP)
CREATE TABLE IF NOT EXISTS pg_redis_sets (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    member TEXT,
    PRIMARY KEY (db, key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_sets_key
ON pg_redis_sets(db, key);

-- 6. Streams (XADD, XTRIM, XREAD, XRANGE, XREVRANGE, XLEN, XDEL)
CREATE TABLE IF NOT EXISTS pg_redis_streams (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    ms BIGINT,
    seq BIGINT,
    fields TEXT,
    PRIMARY KEY (db, key, ms, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id
ON pg_redis_streams(db, key, ms, seq);

-- 7. Universal Key Expiry (Hashes, Lists, ZSets, Sets, Streams)
CREATE TABLE IF NOT EXISTS pg_redis_key_expiry (
    db SMALLINT NOT NULL DEFAULT 0,
    key TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (db, key)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_key_expiry_at
ON pg_redis_key_expiry(expires_at);
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

### Sets (`pg_redis_sets`)
- **Uniqueness by Primary Key:** `(key, member)` guarantees O(log N) uniqueness for `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS`, `SPOP`, and cursor-based `SSCAN`.

### Streams (`pg_redis_streams`)
- **Monotonic Entry Ordering:** Composite primary key `(key, ms, seq)` orders stream entries chronologically for `XADD` (`*`, `<ms>-*`, explicit `<ms>-<seq>`, `NOMKSTREAM`, `MAXLEN`/`MINID`), `XTRIM`, `XRANGE`/`XREVRANGE`, `XLEN`, `XDEL`, and `XREAD [COUNT] [BLOCK]`.
- **Field Serialization:** Stream entry field/value pairs are stored in `fields` and decoded back into ordered RESP arrays.

### Universal Key Expiry (`pg_redis_key_expiry`)
- **All 6 Data Types:** While string keys store `expires_at` inline in `pg_redis_strings`, non-string keys (Hashes, Lists, Sorted Sets, Sets, and Streams) store their expiration timestamp in `pg_redis_key_expiry(key, expires_at)`.
- **Redis 7 Flags & Queries:** `EXPIRE`, `PEXPIRE`, `EXPIREAT`, and `PEXPIREAT` support `NX`, `XX`, `GT`, and `LT` across every data type; `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST`, `RENAME`, `EXISTS`, `TYPE`, `KEYS`, `SCAN`, and `DBSIZE` honor universal expiration consistently.
- **Lazy + Active Cleanup:** Reads filter out keys whose `pg_redis_key_expiry.expires_at <= NOW()` and background eviction deletes expired rows in `LIMIT 1000` batches via `idx_redis_key_expiry_at`, cascading deletions across all five non-string tables.

### Canonical Lock Ordering
To prevent AB-BA deadlocks between concurrent DDL operations (`pg_redis_set_unlogged`) and multi-table commands (`EXPIRE`, `DEL`, `RENAME`), all multi-table operations acquire locks in a single canonical order:

```text
pg_redis_strings -> pg_redis_key_expiry -> pg_redis_hashes -> pg_redis_lists -> pg_redis_zsets -> pg_redis_sets -> pg_redis_streams
```

## Extension Upgrade Scripts

Schema evolutions are packaged as standard PostgreSQL extension migration files under `sql/`:

- `sql/pg_redis_core--0.1.0--0.2.0.sql` — adds `pg_redis_sets` and `pg_redis_streams`.
- `sql/pg_redis_core--0.2.0--0.3.0.sql` — version bump for Pub/Sub & PostgreSQL 16 support.
- `sql/pg_redis_core--0.3.0--0.4.0.sql` — adds `pg_redis_key_expiry` and `idx_redis_key_expiry_at`.
- `sql/pg_redis_core--0.4.0--0.4.1.sql` — patch release upgrade marker.
- `sql/pg_redis_core--0.4.1--0.5.0.sql` — optional native TLS (`rediss://`) & mTLS upgrade marker.
- `sql/pg_redis_core--0.5.0--0.6.0.sql` — adds `db SMALLINT NOT NULL DEFAULT 0` and `(db, ...)` composite primary keys/indexes across all 7 tables for multi-database (`SELECT 0..N-1`) support.
