-- Upgrade script from pg_redis_core 0.1.0 to 0.2.0
-- Adds catalog tables and indexes for Sets and Streams (required for BullMQ 5.x).

CREATE TABLE IF NOT EXISTS pg_redis_sets (
    key TEXT,
    member TEXT,
    PRIMARY KEY (key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_sets_key
ON pg_redis_sets(key);

CREATE TABLE IF NOT EXISTS pg_redis_streams (
    key TEXT,
    ms BIGINT,
    seq BIGINT,
    fields TEXT,
    PRIMARY KEY (key, ms, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id
ON pg_redis_streams(key, ms, seq);
