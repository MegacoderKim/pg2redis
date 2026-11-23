-- Upgrade script from pg_redis_core 0.3.0 to 0.4.0
-- Adds shared non-string key expiration table `pg_redis_key_expiry` supporting
-- universal EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT/TTL/PTTL/EXPIRETIME/PEXPIRETIME/PERSIST
-- across Hashes, Lists, Sorted Sets, Sets, and Streams.

CREATE TABLE IF NOT EXISTS pg_redis_key_expiry (
    key TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_key_expiry_at
ON pg_redis_key_expiry(expires_at);
