use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiClient, SpiResult};

pub const SQL_STRING_GET: &str = "\
    SELECT value, EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION AS ttl_secs \
    FROM pg_redis_strings \
    WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())";

pub const SQL_STRING_SET_NO_TTL: &str = "\
    INSERT INTO pg_redis_strings (db, key, value, expires_at) \
    VALUES ($1, $2, $3, NULL) \
    ON CONFLICT (db, key) DO UPDATE SET value = EXCLUDED.value, expires_at = NULL";

pub const SQL_STRING_SET_WITH_MS: &str = "\
    INSERT INTO pg_redis_strings (db, key, value, expires_at) \
    VALUES ($1, $2, $3, NOW() + ($4 * INTERVAL '1 millisecond')) \
    ON CONFLICT (db, key) DO UPDATE SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at";

pub const SQL_STRING_SET_KEEP_TTL: &str = "\
    INSERT INTO pg_redis_strings (db, key, value, expires_at) \
    VALUES ($1, $2, $3, NULL) \
    ON CONFLICT (db, key) DO UPDATE SET \
        value = EXCLUDED.value, \
        expires_at = CASE \
            WHEN pg_redis_strings.expires_at IS NOT NULL AND pg_redis_strings.expires_at <= NOW() THEN NULL \
            ELSE pg_redis_strings.expires_at \
        END";

pub const SQL_HASH_GET: &str = "\
    SELECT value FROM pg_redis_hashes WHERE db = $1 AND key = $2 AND field = $3";

pub const SQL_HASH_GETALL: &str = "\
    SELECT field, value FROM pg_redis_hashes WHERE db = $1 AND key = $2 ORDER BY field ASC";

pub const SQL_HASH_UPSERT: &str = "\
    INSERT INTO pg_redis_hashes (db, key, field, value) \
    VALUES ($1, $2, $3, $4) \
    ON CONFLICT (db, key, field) DO UPDATE SET value = EXCLUDED.value \
    RETURNING (xmax = 0)";

pub const SQL_ZSET_SCORE: &str = "\
    SELECT score FROM pg_redis_zsets WHERE db = $1 AND key = $2 AND member = $3";

/// Exact ctid-batched TTL eviction query from RFC §5.6 (evicts expired rows across all DBs).
pub const SQL_EVICT_EXPIRED: &str = "\
    DELETE FROM pg_redis_strings \
    WHERE ctid IN ( \
        SELECT ctid FROM pg_redis_strings \
        WHERE expires_at IS NOT NULL AND expires_at < NOW() \
        LIMIT 1000 \
    )";

/// Batched TTL eviction for non-string keys tracked in `pg_redis_key_expiry` (scoped per `(db, key)`).
pub const SQL_EVICT_EXPIRED_NON_STRINGS: &str = "\
    WITH expired AS ( \
        DELETE FROM pg_redis_key_expiry \
        WHERE ctid IN ( \
            SELECT ctid FROM pg_redis_key_expiry \
            WHERE expires_at <= NOW() \
            LIMIT 1000 \
        ) \
        RETURNING db, key \
    ), \
    d_h AS (DELETE FROM pg_redis_hashes WHERE (db, key) IN (SELECT db, key FROM expired)), \
    d_l AS (DELETE FROM pg_redis_lists WHERE (db, key) IN (SELECT db, key FROM expired)), \
    d_z AS (DELETE FROM pg_redis_zsets WHERE (db, key) IN (SELECT db, key FROM expired)), \
    d_s AS (DELETE FROM pg_redis_sets WHERE (db, key) IN (SELECT db, key FROM expired)), \
    d_x AS (DELETE FROM pg_redis_streams WHERE (db, key) IN (SELECT db, key FROM expired)) \
    SELECT COUNT(*)::BIGINT FROM expired";

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
    pub evict_expired_non_strings: OwnedPreparedStatement,
}

impl PreparedPlans {
    pub fn prepare_all(client: &SpiClient) -> SpiResult<Self> {
        let int2 = PgBuiltInOids::INT2OID.oid();
        let text = PgBuiltInOids::TEXTOID.oid();
        let bytea = PgBuiltInOids::BYTEAOID.oid();
        let int8 = PgBuiltInOids::INT8OID.oid();

        Ok(Self {
            string_get: client.prepare(SQL_STRING_GET, Some(vec![int2, text]))?.keep(),
            string_set_no_ttl: client
                .prepare(SQL_STRING_SET_NO_TTL, Some(vec![int2, text, bytea]))?
                .keep(),
            string_set_with_ms: client
                .prepare(SQL_STRING_SET_WITH_MS, Some(vec![int2, text, bytea, int8]))?
                .keep(),
            string_set_keep_ttl: client
                .prepare(SQL_STRING_SET_KEEP_TTL, Some(vec![int2, text, bytea]))?
                .keep(),
            hash_get: client
                .prepare(SQL_HASH_GET, Some(vec![int2, text, text]))?
                .keep(),
            hash_getall: client
                .prepare(SQL_HASH_GETALL, Some(vec![int2, text]))?
                .keep(),
            hash_upsert: client
                .prepare(SQL_HASH_UPSERT, Some(vec![int2, text, text, text]))?
                .keep(),
            zset_score: client
                .prepare(SQL_ZSET_SCORE, Some(vec![int2, text, text]))?
                .keep(),
            evict_expired: client.prepare(SQL_EVICT_EXPIRED, None)?.keep(),
            evict_expired_non_strings: client
                .prepare(SQL_EVICT_EXPIRED_NON_STRINGS, None)?
                .keep(),
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
             AND to_regclass('pg_redis_streams') IS NOT NULL \
             AND to_regclass('pg_redis_key_expiry') IS NOT NULL \
             AND (SELECT COUNT(*) FROM information_schema.columns \
                  WHERE table_schema = current_schema() \
                    AND column_name = 'db' \
                    AND table_name IN ( \
                        'pg_redis_strings', 'pg_redis_hashes', 'pg_redis_lists', \
                        'pg_redis_zsets', 'pg_redis_sets', 'pg_redis_streams', \
                        'pg_redis_key_expiry' \
                    )) = 7",
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
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            value BYTEA,
            expires_at TIMESTAMPTZ,
            PRIMARY KEY (db, key)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_strings_expiry
            ON pg_redis_strings(expires_at)
            WHERE expires_at IS NOT NULL;

        CREATE {table_kw} IF NOT EXISTS pg_redis_key_expiry (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            expires_at TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (db, key)
        ) WITH (fillfactor = 80);
        CREATE INDEX IF NOT EXISTS idx_redis_key_expiry_at
            ON pg_redis_key_expiry(expires_at);

        CREATE {table_kw} IF NOT EXISTS pg_redis_hashes (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            field TEXT,
            value TEXT,
            PRIMARY KEY (db, key, field)
        ) WITH (fillfactor = 80);

        CREATE {table_kw} IF NOT EXISTS pg_redis_lists (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            seq BIGSERIAL,
            value TEXT,
            PRIMARY KEY (db, key, seq)
        ) WITH (fillfactor = 80);

        CREATE {table_kw} IF NOT EXISTS pg_redis_zsets (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            member TEXT,
            score DOUBLE PRECISION,
            PRIMARY KEY (db, key, member)
        ) WITH (fillfactor = 80);

        CREATE {table_kw} IF NOT EXISTS pg_redis_sets (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            member TEXT,
            PRIMARY KEY (db, key, member)
        ) WITH (fillfactor = 80);

        CREATE {table_kw} IF NOT EXISTS pg_redis_streams (
            db SMALLINT NOT NULL DEFAULT 0,
            key TEXT,
            ms BIGINT,
            seq BIGINT,
            fields TEXT,
            PRIMARY KEY (db, key, ms, seq)
        ) WITH (fillfactor = 80);

        -- Safety-net upgrade for pre-0.6.0 tables that lack the `db` column
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_strings' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_strings ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_strings DROP CONSTRAINT IF EXISTS pg_redis_strings_pkey;
                ALTER TABLE pg_redis_strings ADD PRIMARY KEY (db, key);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_key_expiry' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_key_expiry ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_key_expiry DROP CONSTRAINT IF EXISTS pg_redis_key_expiry_pkey;
                ALTER TABLE pg_redis_key_expiry ADD PRIMARY KEY (db, key);
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_hashes' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_hashes ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_hashes DROP CONSTRAINT IF EXISTS pg_redis_hashes_pkey;
                ALTER TABLE pg_redis_hashes ADD PRIMARY KEY (db, key, field);
                DROP INDEX IF EXISTS idx_redis_hashes_key;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_lists' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_lists ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_lists DROP CONSTRAINT IF EXISTS pg_redis_lists_pkey;
                ALTER TABLE pg_redis_lists ADD PRIMARY KEY (db, key, seq);
                DROP INDEX IF EXISTS idx_redis_lists_key_seq;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_zsets' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_zsets ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_zsets DROP CONSTRAINT IF EXISTS pg_redis_zsets_pkey;
                ALTER TABLE pg_redis_zsets ADD PRIMARY KEY (db, key, member);
                DROP INDEX IF EXISTS idx_redis_zsets_score;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_sets' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_sets ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_sets DROP CONSTRAINT IF EXISTS pg_redis_sets_pkey;
                ALTER TABLE pg_redis_sets ADD PRIMARY KEY (db, key, member);
                DROP INDEX IF EXISTS idx_redis_sets_key;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'pg_redis_streams' AND column_name = 'db'
            ) THEN
                ALTER TABLE pg_redis_streams ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
                ALTER TABLE pg_redis_streams DROP CONSTRAINT IF EXISTS pg_redis_streams_pkey;
                ALTER TABLE pg_redis_streams ADD PRIMARY KEY (db, key, ms, seq);
                DROP INDEX IF EXISTS idx_redis_streams_key_id;
            END IF;
        END $$;

        CREATE INDEX IF NOT EXISTS idx_redis_hashes_key ON pg_redis_hashes(db, key);
        CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq ON pg_redis_lists(db, key, seq);
        CREATE INDEX IF NOT EXISTS idx_redis_zsets_score ON pg_redis_zsets(db, key, score);
        CREATE INDEX IF NOT EXISTS idx_redis_sets_key ON pg_redis_sets(db, key);
        CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id ON pg_redis_streams(db, key, ms, seq);
        "#
    );

    client.update(&ddl, None, None)?;
    Ok(())
}

pub fn set_tables_unlogged(client: &mut SpiClient, unlogged: bool) -> SpiResult<()> {
    let mode = if unlogged { "UNLOGGED" } else { "LOGGED" };
    // Lock in canonical order (strings -> key_expiry -> non-string tables) to prevent
    // AB-BA deadlocks with concurrent DML/TTL queries.
    for tbl in [
        "pg_redis_strings",
        "pg_redis_key_expiry",
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
    let string_evicted = match plans {
        Some(p) => client.update(&p.evict_expired, None, None)?,
        None => client.update(SQL_EVICT_EXPIRED, None, None)?,
    }
    .len();

    let non_string_evicted = match plans {
        Some(p) => client.update(&p.evict_expired_non_strings, None, None)?,
        None => client.update(SQL_EVICT_EXPIRED_NON_STRINGS, None, None)?,
    }
    .first()
    .get_one::<i64>()?
    .unwrap_or(0)
    .max(0) as usize;

    Ok(string_evicted + non_string_evicted)
}
