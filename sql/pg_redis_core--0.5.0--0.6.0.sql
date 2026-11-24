-- Upgrade script from pg_redis_core 0.5.0 to 0.6.0
-- Adds logical Redis database column `db SMALLINT NOT NULL DEFAULT 0` across all 7
-- catalog tables so `SELECT 0..N-1` (default 16 databases), `FLUSHDB` vs `FLUSHALL`,
-- `MOVE`, and `SWAPDB` are natively isolated while preserving all existing DB 0 data.
-- Tables are altered in canonical lock order to avoid deadlocks.

DO $$
BEGIN
    -- 1. pg_redis_strings
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_strings' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_strings ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_strings DROP CONSTRAINT IF EXISTS pg_redis_strings_pkey;
        ALTER TABLE pg_redis_strings ADD PRIMARY KEY (db, key);
    END IF;

    -- 2. pg_redis_key_expiry
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_key_expiry' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_key_expiry ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_key_expiry DROP CONSTRAINT IF EXISTS pg_redis_key_expiry_pkey;
        ALTER TABLE pg_redis_key_expiry ADD PRIMARY KEY (db, key);
    END IF;

    -- 3. pg_redis_hashes
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_hashes' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_hashes ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_hashes DROP CONSTRAINT IF EXISTS pg_redis_hashes_pkey;
        ALTER TABLE pg_redis_hashes ADD PRIMARY KEY (db, key, field);
        DROP INDEX IF EXISTS idx_redis_hashes_key;
        CREATE INDEX IF NOT EXISTS idx_redis_hashes_key ON pg_redis_hashes(db, key);
    END IF;

    -- 4. pg_redis_lists
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_lists' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_lists ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_lists DROP CONSTRAINT IF EXISTS pg_redis_lists_pkey;
        ALTER TABLE pg_redis_lists ADD PRIMARY KEY (db, key, seq);
        DROP INDEX IF EXISTS idx_redis_lists_key_seq;
        CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq ON pg_redis_lists(db, key, seq);
    END IF;

    -- 5. pg_redis_zsets
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_zsets' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_zsets ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_zsets DROP CONSTRAINT IF EXISTS pg_redis_zsets_pkey;
        ALTER TABLE pg_redis_zsets ADD PRIMARY KEY (db, key, member);
        DROP INDEX IF EXISTS idx_redis_zsets_score;
        CREATE INDEX IF NOT EXISTS idx_redis_zsets_score ON pg_redis_zsets(db, key, score);
    END IF;

    -- 6. pg_redis_sets
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_sets' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_sets ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_sets DROP CONSTRAINT IF EXISTS pg_redis_sets_pkey;
        ALTER TABLE pg_redis_sets ADD PRIMARY KEY (db, key, member);
        DROP INDEX IF EXISTS idx_redis_sets_key;
        CREATE INDEX IF NOT EXISTS idx_redis_sets_key ON pg_redis_sets(db, key);
    END IF;

    -- 7. pg_redis_streams
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'pg_redis_streams' AND column_name = 'db'
    ) THEN
        ALTER TABLE pg_redis_streams ADD COLUMN db SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE pg_redis_streams DROP CONSTRAINT IF EXISTS pg_redis_streams_pkey;
        ALTER TABLE pg_redis_streams ADD PRIMARY KEY (db, key, ms, seq);
        DROP INDEX IF EXISTS idx_redis_streams_key_id;
        CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id ON pg_redis_streams(db, key, ms, seq);
    END IF;
END $$;
