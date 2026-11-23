mod cache;
mod clients;
mod commands;
mod config;
mod lua;
mod notify;
mod protocol;
mod pubsub;
mod storage;
mod worker;

use bytes::Bytes;
use pgrx::prelude::*;

pgrx::pg_module_magic!();

// Provision catalog tables and indexes during `CREATE EXTENSION pg_redis_core` (RFC §3 & §5.2).
extension_sql!(
    r#"
-- 1. Strings (SET, GET, INCR, DEL)
CREATE TABLE IF NOT EXISTS pg_redis_strings (
    key TEXT PRIMARY KEY,
    value BYTEA,
    expires_at TIMESTAMPTZ
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_strings_expiry
ON pg_redis_strings(expires_at)
WHERE expires_at IS NOT NULL;

-- 2. Hashes (HSET, HGET, HGETALL, HDEL)
CREATE TABLE IF NOT EXISTS pg_redis_hashes (
    key TEXT,
    field TEXT,
    value TEXT,
    PRIMARY KEY (key, field)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_hashes_key
ON pg_redis_hashes(key);

-- 3. Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
CREATE TABLE IF NOT EXISTS pg_redis_lists (
    key TEXT,
    seq BIGSERIAL,
    value TEXT,
    PRIMARY KEY (key, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_lists_key_seq
ON pg_redis_lists(key, seq);

-- 4. Sorted Sets (ZADD, ZRANGE, ZREM, ZSCORE, ZPOPMIN)
CREATE TABLE IF NOT EXISTS pg_redis_zsets (
    key TEXT,
    member TEXT,
    score DOUBLE PRECISION,
    PRIMARY KEY (key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_zsets_score
ON pg_redis_zsets(key, score);

-- 5. Sets (SADD, SREM, SMEMBERS, SISMEMBER, SCARD)
CREATE TABLE IF NOT EXISTS pg_redis_sets (
    key TEXT,
    member TEXT,
    PRIMARY KEY (key, member)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_sets_key
ON pg_redis_sets(key);

-- 6. Streams (XADD, XTRIM, XREAD, XRANGE, XREVRANGE, XLEN, XDEL)
CREATE TABLE IF NOT EXISTS pg_redis_streams (
    key TEXT,
    ms BIGINT,
    seq BIGINT,
    fields TEXT,
    PRIMARY KEY (key, ms, seq)
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_streams_key_id
ON pg_redis_streams(key, ms, seq);

-- 7. Key Expiry for non-string structures (Hashes, Lists, ZSets, Sets, Streams)
CREATE TABLE IF NOT EXISTS pg_redis_key_expiry (
    key TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 80);

CREATE INDEX IF NOT EXISTS idx_redis_key_expiry_at
ON pg_redis_key_expiry(expires_at);
"#,
    name = "pg_redis_schema",
    bootstrap
);

#[pg_guard]
pub extern "C" fn _PG_init() {
    let in_preload = unsafe { pg_sys::process_shared_preload_libraries_in_progress };
    config::register_gucs(in_preload);

    if in_preload {
        worker::register_background_worker();
    }
}

/// Core SQL-callable Redis command dispatcher (RFC §4.2).
///
/// Accepts a Redis command and up to three optional arguments, executes it via
/// PostgreSQL SPI, and returns a RESP2-encoded response string.
#[pg_extern]
pub fn redis_dispatcher(
    cmd: String,
    key: Option<String>,
    arg1: Option<String>,
    arg2: Option<String>,
) -> String {
    let mut args = vec![Bytes::from(cmd)];
    if let Some(k) = key {
        args.push(Bytes::from(k));
    }
    if let Some(a1) = arg1 {
        args.push(Bytes::from(a1));
    }
    if let Some(a2) = arg2 {
        args.push(Bytes::from(a2));
    }

    let lua_engine = if args[0].eq_ignore_ascii_case(b"EVAL")
        || args[0].eq_ignore_ascii_case(b"EVAL_RO")
        || args[0].eq_ignore_ascii_case(b"SCRIPT")
    {
        lua::LuaEngine::new().ok()
    } else {
        None
    };

    let mut conn = commands::ConnectionState::new(0);
    let mut mutated = Vec::new();
    let result = Spi::connect(|mut client| {
        commands::dispatch_with_spi(
            &mut client,
            None,
            None,
            lua_engine.as_ref(),
            None,
            None,
            &mut conn,
            &args,
            &mut mutated,
        )
    });

    match result {
        Ok(val) => val.to_resp2_string(),
        Err(_) => "-ERR internal database SPI error\r\n".to_string(),
    }
}

/// Execute one batch of ctid-based TTL eviction on `pg_redis_strings` (RFC §5.6).
#[pg_extern]
pub fn pg_redis_evict_expired() -> i64 {
    Spi::connect(|mut client| storage::evict_expired_batch(&mut client, None))
        .unwrap_or(0) as i64
}

/// Toggle all `pg_redis_*` catalog tables between `UNLOGGED` and `LOGGED` mode (RFC §5.1).
#[pg_extern]
pub fn pg_redis_set_unlogged(unlogged: bool) -> bool {
    Spi::connect(|mut client| storage::set_tables_unlogged(&mut client, unlogged)).is_ok()
}

/// Ensure all `pg_redis_*` catalog tables and indexes exist.
#[pg_extern]
pub fn pg_redis_ensure_schema(unlogged: bool) -> bool {
    Spi::connect(|mut client| storage::ensure_schema(&mut client, unlogged)).is_ok()
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_ping() {
        let res = crate::redis_dispatcher("PING".to_string(), None, None, None);
        assert_eq!(res, "+PONG\r\n");
    }

    #[pg_test]
    fn test_strings_roundtrip() {
        let set_res = crate::redis_dispatcher(
            "SET".to_string(),
            Some("hello".to_string()),
            Some("world".to_string()),
            None,
        );
        assert_eq!(set_res, "+OK\r\n");

        let get_res =
            crate::redis_dispatcher("GET".to_string(), Some("hello".to_string()), None, None);
        assert_eq!(get_res, "$5\r\nworld\r\n");

        let del_res =
            crate::redis_dispatcher("DEL".to_string(), Some("hello".to_string()), None, None);
        assert_eq!(del_res, ":1\r\n");

        let missing =
            crate::redis_dispatcher("GET".to_string(), Some("hello".to_string()), None, None);
        assert_eq!(missing, "$-1\r\n");
    }

    #[pg_test]
    fn test_incr_decr() {
        assert_eq!(
            crate::redis_dispatcher("INCR".to_string(), Some("counter".to_string()), None, None),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "INCRBY".to_string(),
                Some("counter".to_string()),
                Some("9".to_string()),
                None
            ),
            ":10\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("DECR".to_string(), Some("counter".to_string()), None, None),
            ":9\r\n"
        );
    }

    #[pg_test]
    fn test_hashes_roundtrip() {
        assert_eq!(
            crate::redis_dispatcher(
                "HSET".to_string(),
                Some("user:1".to_string()),
                Some("name".to_string()),
                Some("Alice".to_string())
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "HGET".to_string(),
                Some("user:1".to_string()),
                Some("name".to_string()),
                None
            ),
            "$5\r\nAlice\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "HGETALL".to_string(),
                Some("user:1".to_string()),
                None,
                None
            ),
            "*2\r\n$4\r\nname\r\n$5\r\nAlice\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "HDEL".to_string(),
                Some("user:1".to_string()),
                Some("name".to_string()),
                None
            ),
            ":1\r\n"
        );
    }

    #[pg_test]
    fn test_lists_roundtrip() {
        assert_eq!(
            crate::redis_dispatcher(
                "LPUSH".to_string(),
                Some("jobs".to_string()),
                Some("first".to_string()),
                None
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "LPUSH".to_string(),
                Some("jobs".to_string()),
                Some("second".to_string()),
                None
            ),
            ":2\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "RPUSH".to_string(),
                Some("jobs".to_string()),
                Some("tail".to_string()),
                None
            ),
            ":3\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "LRANGE".to_string(),
                Some("jobs".to_string()),
                Some("0".to_string()),
                Some("-1".to_string())
            ),
            "*3\r\n$6\r\nsecond\r\n$5\r\nfirst\r\n$4\r\ntail\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("LPOP".to_string(), Some("jobs".to_string()), None, None),
            "$6\r\nsecond\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("RPOP".to_string(), Some("jobs".to_string()), None, None),
            "$4\r\ntail\r\n"
        );
    }

    #[pg_test]
    fn test_zsets_and_zpopmin_roundtrip() {
        assert_eq!(
            crate::redis_dispatcher(
                "ZADD".to_string(),
                Some("lb".to_string()),
                Some("100".to_string()),
                Some("alice".to_string())
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "ZADD".to_string(),
                Some("lb".to_string()),
                Some("50".to_string()),
                Some("bob".to_string())
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "ZRANGE".to_string(),
                Some("lb".to_string()),
                Some("0".to_string()),
                Some("-1".to_string())
            ),
            "*2\r\n$3\r\nbob\r\n$5\r\nalice\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("ZPOPMIN".to_string(), Some("lb".to_string()), None, None),
            "*2\r\n$3\r\nbob\r\n$2\r\n50\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "ZSCORE".to_string(),
                Some("lb".to_string()),
                Some("alice".to_string()),
                None
            ),
            "$3\r\n100\r\n"
        );
    }

    #[pg_test]
    fn test_sets_and_lua_xadd() {
        assert_eq!(
            crate::redis_dispatcher(
                "SADD".to_string(),
                Some("deps:1".to_string()),
                Some("job:2".to_string()),
                None
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "SISMEMBER".to_string(),
                Some("deps:1".to_string()),
                Some("job:2".to_string()),
                None
            ),
            ":1\r\n"
        );

        let lua_res = crate::redis_dispatcher(
            "EVAL".to_string(),
            Some(
                "local id = redis.call('XADD', KEYS[1], '*', 'event', 'waiting', 'jobId', '1') \
                 local packed = cmsgpack.pack({ok = true, id = id}) \
                 local unpacked = cmsgpack.unpack(packed) \
                 return cjson.encode({id = unpacked['id'], len = redis.call('XLEN', KEYS[1])})"
                    .to_string(),
            ),
            Some("1".to_string()),
            Some("bull:orders:events".to_string()),
        );
        assert!(lua_res.starts_with('$'));
        assert!(lua_res.contains("\"len\":1"));
    }

    #[pg_test]
    fn test_unlogged_toggle() {
        assert!(crate::pg_redis_set_unlogged(true));
        assert!(crate::pg_redis_set_unlogged(false));
    }

    #[pg_test]
    fn test_pubsub_hub_semantics() {
        use bytes::Bytes;
        use tokio::sync::mpsc;

        let hub = crate::pubsub::PubSubHub::new();
        let (tx1, mut rx1) = mpsc::channel(16);
        let (tx2, mut rx2) = mpsc::channel(16);
        let (tx_slow, _rx_slow) = mpsc::channel(2);
        hub.register_client(1, tx1);
        hub.register_client(2, tx2);
        hub.register_client(3, tx_slow);

        let ch_a = Bytes::from_static(b"wf:order:1");
        let ch_b = Bytes::from_static(b"wf:order:2");
        let pat_wf = Bytes::from_static(b"wf:*");

        // Subscribe + duplicate idempotency
        assert_eq!(hub.subscribe(1, ch_a.clone()), 1);
        assert_eq!(hub.subscribe(1, ch_a.clone()), 1);
        assert_eq!(hub.subscribe(1, ch_b.clone()), 2);
        assert_eq!(hub.subscribe(2, ch_a.clone()), 1);

        // Pattern subscribe + duplicate idempotency + NUMPAT
        assert_eq!(hub.psubscribe(2, pat_wf.clone()), 2);
        assert_eq!(hub.psubscribe(2, pat_wf.clone()), 2);
        assert_eq!(hub.numpat(), 1);

        // Publish to 2 channel subscribers + 1 pattern match (total 3 deliveries)
        let mut big_payload = vec![0u8, 1u8, 0xffu8, b'\r', b'\n'];
        big_payload.resize(32 * 1024, b'x');
        let payload = Bytes::from(big_payload);
        assert_eq!(hub.publish(&ch_a, &payload), 3);
        match rx1.try_recv().unwrap() {
            crate::pubsub::PubSubMessage::Channel { payload: p, .. } => assert_eq!(p, payload),
            other => panic!("expected Channel message, got {other:?}"),
        }
        let mut got_ch = false;
        let mut got_pat = false;
        for _ in 0..2 {
            match rx2.try_recv().unwrap() {
                crate::pubsub::PubSubMessage::Channel { payload: p, .. } => {
                    assert_eq!(p, payload);
                    got_ch = true;
                }
                crate::pubsub::PubSubMessage::Pattern {
                    pattern,
                    channel,
                    payload: p,
                } => {
                    assert_eq!(pattern, pat_wf);
                    assert_eq!(channel, ch_a);
                    assert_eq!(p, payload);
                    got_pat = true;
                }
            }
        }
        assert!(got_ch && got_pat);
        assert!(rx1.try_recv().is_err());

        // Unsubscribe one channel & pattern
        assert_eq!(hub.unsubscribe(1, &ch_a), 1);
        assert_eq!(hub.punsubscribe(2, &pat_wf), 1);
        assert_eq!(hub.numpat(), 0);
        assert_eq!(hub.publish(&ch_a, &Bytes::from_static(b"one")), 1);
        match rx2.try_recv().unwrap() {
            crate::pubsub::PubSubMessage::Channel { payload: p, .. } => {
                assert_eq!(p, Bytes::from_static(b"one"))
            }
            other => panic!("expected Channel message, got {other:?}"),
        }

        // Slow consumer eviction on bounded channel overflow
        let slow_ch = Bytes::from_static(b"slow:chan");
        assert_eq!(hub.subscribe(3, slow_ch.clone()), 1);
        assert_eq!(hub.publish(&slow_ch, &Bytes::from_static(b"m1")), 1);
        assert_eq!(hub.publish(&slow_ch, &Bytes::from_static(b"m2")), 1);
        // 3rd publish overflows capacity=2 and evicts client 3
        assert_eq!(hub.publish(&slow_ch, &Bytes::from_static(b"m3")), 0);
        assert_eq!(hub.subscription_count(3), 0);

        // Unsubscribe all
        let acks = hub.unsubscribe_all(1);
        assert_eq!(acks, vec![(ch_b.clone(), 0)]);
        assert_eq!(hub.subscription_count(1), 0);

        // Disconnect cleanup via RAII guard
        {
            let _guard = crate::pubsub::PubSubClientGuard::new(2, &hub);
        }
        assert_eq!(hub.publish(&ch_a, &Bytes::from_static(b"none")), 0);
    }

    #[pg_test]
    fn test_lua_sandbox_globals() {
        let res = crate::redis_dispatcher(
            "EVAL".to_string(),
            Some(
                "return cjson.encode({ \
                    os = type(os), \
                    io = type(io), \
                    pkg = type(package), \
                    dbg = type(debug), \
                    dofile = type(dofile), \
                    loadfile = type(loadfile), \
                    require = type(require), \
                    math = type(math), \
                    str = type(string), \
                    tbl = type(table) \
                 })"
                .to_string(),
            ),
            Some("0".to_string()),
            None,
        );
        assert!(res.contains("\"os\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"io\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"pkg\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"dbg\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"dofile\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"loadfile\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"require\":\"nil\""), "unexpected: {res}");
        assert!(res.contains("\"math\":\"table\""), "unexpected: {res}");
        assert!(res.contains("\"str\":\"table\""), "unexpected: {res}");
        assert!(res.contains("\"tbl\":\"table\""), "unexpected: {res}");
    }

    #[pg_test]
    fn test_universal_ttl_and_scans() {
        // Follow canonical lock order (strings -> key_expiry -> hashes -> zsets -> sets)
        // so parallel `#[pg_test]` runs never deadlock with `test_unlogged_toggle`.
        assert_eq!(
            crate::redis_dispatcher(
                "SET".to_string(),
                Some("sess:1".to_string()),
                Some("tok".to_string()),
                None
            ),
            "+OK\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "GETEX".to_string(),
                Some("sess:1".to_string()),
                Some("EX".to_string()),
                Some("30".to_string())
            ),
            "$3\r\ntok\r\n"
        );
        let sess_ttl =
            crate::redis_dispatcher("TTL".to_string(), Some("sess:1".to_string()), None, None);
        assert!(
            sess_ttl.starts_with(':') && sess_ttl != ":-1\r\n" && sess_ttl != ":-2\r\n",
            "sess_ttl={sess_ttl}"
        );

        // Hash + universal EXPIRE + TTL + PERSIST + HSCAN
        assert_eq!(
            crate::redis_dispatcher(
                "HSET".to_string(),
                Some("h:ttl".to_string()),
                Some("f1".to_string()),
                Some("v1".to_string())
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher(
                "EXPIRE".to_string(),
                Some("h:ttl".to_string()),
                Some("60".to_string()),
                None
            ),
            ":1\r\n"
        );
        let ttl = crate::redis_dispatcher("TTL".to_string(), Some("h:ttl".to_string()), None, None);
        assert!(ttl.starts_with(':') && ttl != ":-1\r\n" && ttl != ":-2\r\n", "ttl={ttl}");

        // EXPIRE NX should fail when TTL already exists
        assert_eq!(
            crate::redis_dispatcher(
                "EXPIRE".to_string(),
                Some("h:ttl".to_string()),
                Some("120".to_string()),
                Some("NX".to_string())
            ),
            ":0\r\n"
        );
        // EXPIRE GT should succeed for larger TTL
        assert_eq!(
            crate::redis_dispatcher(
                "EXPIRE".to_string(),
                Some("h:ttl".to_string()),
                Some("120".to_string()),
                Some("GT".to_string())
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("PERSIST".to_string(), Some("h:ttl".to_string()), None, None),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("TTL".to_string(), Some("h:ttl".to_string()), None, None),
            ":-1\r\n"
        );

        let hscan = crate::redis_dispatcher(
            "HSCAN".to_string(),
            Some("h:ttl".to_string()),
            Some("0".to_string()),
            None,
        );
        assert!(hscan.contains("$2\r\nf1\r\n$2\r\nv1\r\n"), "hscan={hscan}");

        // ZSet + ZSCAN
        assert_eq!(
            crate::redis_dispatcher(
                "ZADD".to_string(),
                Some("z:scan".to_string()),
                Some("42".to_string()),
                Some("alice".to_string())
            ),
            ":1\r\n"
        );
        let zscan = crate::redis_dispatcher(
            "ZSCAN".to_string(),
            Some("z:scan".to_string()),
            Some("0".to_string()),
            None,
        );
        assert!(zscan.contains("$5\r\nalice\r\n$2\r\n42\r\n"), "zscan={zscan}");

        // Set + SSCAN + immediate expiry with EXPIRE 0
        assert_eq!(
            crate::redis_dispatcher(
                "SADD".to_string(),
                Some("s:ttl".to_string()),
                Some("m1".to_string()),
                None
            ),
            ":1\r\n"
        );
        let sscan = crate::redis_dispatcher(
            "SSCAN".to_string(),
            Some("s:ttl".to_string()),
            Some("0".to_string()),
            None,
        );
        assert!(sscan.contains("$2\r\nm1\r\n"), "sscan={sscan}");
        assert_eq!(
            crate::redis_dispatcher(
                "EXPIRE".to_string(),
                Some("s:ttl".to_string()),
                Some("0".to_string()),
                None
            ),
            ":1\r\n"
        );
        assert_eq!(
            crate::redis_dispatcher("SCARD".to_string(), Some("s:ttl".to_string()), None, None),
            ":0\r\n"
        );
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
