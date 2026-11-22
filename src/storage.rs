use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiClient, SpiResult};

pub const SQL_STRING_GET: &str = "\
    SELECT value, EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION AS ttl_secs \
    FROM pg_redis_strings \
    WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())";

pub const SQL_STRING_SET_NO_TTL: &str = "\
    INSERT INTO pg_redis_strings (key, value, expires_at) \
    VALUES ($1, $2, NULL) \
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL";

pub const SQL_STRING_SET_WITH_MS: &str = "\
    INSERT INTO pg_redis_strings (key, value, expires_at) \
    VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond')) \
    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at";

pub const SQL_STRING_SET_KEEP_TTL: &str = "\
    INSERT INTO pg_redis_strings (key, value, expires_at) \
    VALUES ($1, $2, NULL) \
    ON CONFLICT (key) DO UPDATE SET \
        value = EXCLUDED.value, \
        expires_at = CASE \
            WHEN pg_redis_strings.expires_at IS NOT NULL AND pg_redis_strings.expires_at <= NOW() THEN NULL \
            ELSE pg_redis_strings.expires_at \
        END";

pub const SQL_HASH_GET: &str = "\
    SELECT value FROM pg_redis_hashes WHERE key = $1 AND field = $2";

pub const SQL_HASH_GETALL: &str = "\
    SELECT field, value FROM pg_redis_hashes WHERE key = $1 ORDER BY field ASC";

pub const SQL_HASH_UPSERT: &str = "\
    INSERT INTO pg_redis_hashes (key, field, value) \
    VALUES ($1, $2, $3) \
    ON CONFLICT (key, field) DO UPDATE SET value = EXCLUDED.value \
    RETURNING (xmax = 0)";

pub const SQL_ZSET_SCORE: &str = "\
    SELECT score FROM pg_redis_zsets WHERE key = $1 AND member = $2";

/// Exact ctid-batched TTL eviction query from RFC §5.6.
pub const SQL_EVICT_EXPIRED: &str = "\
    DELETE FROM pg_redis_strings \
    WHERE ctid IN ( \
        SELECT ctid FROM pg_redis_strings \
        WHERE expires_at IS NOT NULL AND expires_at < NOW() \
        LIMIT 1000 \
    )";

/// Cached SPI prepared statements persisted across transactions via `SPI_keepplan` (RFC §5.3).
pub struct PreparedPlans {
    pub string_get: OwnedPreparedStatement,
    pub string_set_no_ttl: OwnedPreparedStatement,
    pub string_set_with_ms: OwnedPreparedStatement,
    pub string_set_keep_ttl: OwnedPreparedStatement,
    pub hash_get: OwnedPreparedStatement,
    pub hash_getall: OwnedPreparedStatement,
    pub hash_upsert: OwnedPreparedStatement,
    pub zset_score: OwnedPreparedStatement,
    pub evict_expired: OwnedPreparedStatement,
}

impl PreparedPlans {
    pub fn prepare_all(client: &SpiClient) -> SpiResult<Self> {
        let text = PgBuiltInOids::TEXTOID.oid();
        let bytea = PgBuiltInOids::BYTEAOID.oid();
        let int8 = PgBuiltInOids::INT8OID.oid();

        Ok(Self {
            string_get: client.prepare(SQL_STRING_GET, Some(vec![text]))?.keep(),
            string_set_no_ttl: client
                .prepare(SQL_STRING_SET_NO_TTL, Some(vec![text, bytea]))?
                .keep(),
            string_set_with_ms: client
                .prepare(SQL_STRING_SET_WITH_MS, Some(vec![text, bytea, int8]))?
                .keep(),
            string_set_keep_ttl: client
                .prepare(SQL_STRING_SET_KEEP_TTL, Some(vec![text, bytea]))?
                .keep(),
            hash_get: client
                .prepare(SQL_HASH_GET, Some(vec![text, text]))?
                .keep(),
            hash_getall: client.prepare(SQL_HASH_GETALL, Some(vec![text]))?.keep(),
            hash_upsert: client
                .prepare(SQL_HASH_UPSERT, Some(vec![text, text, text]))?
                .keep(),
            zset_score: client
                .prepare(SQL_ZSET_SCORE, Some(vec![text, text]))?
                .keep(),
            evict_expired: client.prepare(SQL_EVICT_EXPIRED, None)?.keep(),
        })
    }
}

pub fn tables_exist(client: &SpiClient) -> SpiResult<bool> {
    let exists = client
        .select(
            "SELECT to_regclass('pg_redis_strings') IS NOT NULL \
             AND to_regclass('pg_redis_hashes') IS NOT NULL \
             AND to_regclass('pg_redis_lists') IS NOT NULL \
             AND to_regclass('pg_redis_zsets') IS NOT NULL \
             AND to_regclass('pg_redis_sets') IS NOT NULL \
             AND to_regclass('pg_redis_streams') IS NOT NULL",
            None,
            None,
        )?
        .first()
        .get_one::<bool>()?
        .unwrap_or(false);
    Ok(exists)
}

pub fn ensure_schema(client: &mut SpiClient, unlogged: bool) -> SpiResult<()> {
    let table_kw = if unlogged { "UNLOGGED TABLE" } else { "TABLE" };
    let ddl = format!(
        r#"
        CREATE {table_kw} IF NOT EXISTS pg_redis_strings (
            key TEXT PRIMARY KEY,
            value BYTEA,
            expires_at TIMESTAMPTZ
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_strings_expiry
            ON pg_redis_strings(expires_at)
            WHERE expires_at IS NOT NULL;

        CREATE {table_kw} IF NOT EXISTS pg_redis_hashes (
            key TEXT,
            field TEXT,
            value TEXT,
            PRIMARY KEY (key, field)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_hashes_key
            ON pg_redis_hashes(key);

        CREATE {table_kw} IF NOT EXISTS pg_redis_lists (
            key TEXT,
            seq BIGSERIAL,
            value TEXT,
            PRIMARY KEY (key, seq)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq
            ON pg_redis_lists(key, seq);

        CREATE {table_kw} IF NOT EXISTS pg_redis_zsets (
            key TEXT,
            member TEXT,
            score DOUBLE PRECISION,
            PRIMARY KEY (key, member)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_zsets_score
            ON pg_redis_zsets(key, score);

        CREATE {table_kw} IF NOT EXISTS pg_redis_sets (
            key TEXT,
            member TEXT,
            PRIMARY KEY (key, member)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_sets_key
            ON pg_redis_sets(key);

        CREATE {table_kw} IF NOT EXISTS pg_redis_streams (
            key TEXT,
            ms BIGINT,
            seq BIGINT,
            fields TEXT,
            PRIMARY KEY (key, ms, seq)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id
            ON pg_redis_streams(key, ms, seq);
        "#
    );

    client.update(&ddl, None, None)?;
    Ok(())
}

pub fn set_tables_unlogged(client: &mut SpiClient, unlogged: bool) -> SpiResult<()> {
    let mode = if unlogged { "UNLOGGED" } else { "LOGGED" };
    for tbl in [
        "pg_redis_strings",
        "pg_redis_hashes",
        "pg_redis_lists",
        "pg_redis_zsets",
        "pg_redis_sets",
        "pg_redis_streams",
    ] {
        let sql = format!("ALTER TABLE {tbl} SET {mode}");
        client.update(&sql, None, None)?;
    }
    Ok(())
}

pub fn evict_expired_batch(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
) -> SpiResult<usize> {
    let table = match plans {
        Some(p) => client.update(&p.evict_expired, None, None)?,
        None => client.update(SQL_EVICT_EXPIRED, None, None)?,
    };
    Ok(table.len())
}
