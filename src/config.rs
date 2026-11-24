use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

pub static BIND_ADDRESS: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"0.0.0.0"));

pub static PORT: GucSetting<i32> = GucSetting::<i32>::new(6379);

pub static DATABASE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"postgres"));

pub static UNLOGGED: GucSetting<bool> = GucSetting::<bool>::new(false);

pub static CACHE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

pub static CACHE_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(10_000);

pub static CACHE_TTL_MS: GucSetting<i32> = GucSetting::<i32>::new(500);

pub static EVICTION_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);

pub static REQUIREPASS: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static LUA_TIME_LIMIT_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);

pub static MAX_CLIENTS: GucSetting<i32> = GucSetting::<i32>::new(1_000);

pub static MAX_FRAME_BYTES: GucSetting<i32> = GucSetting::<i32>::new(67_108_864);

pub static PUBSUB_QUEUE_SIZE: GucSetting<i32> = GucSetting::<i32>::new(4_096);

pub static TLS_PORT: GucSetting<i32> = GucSetting::<i32>::new(0);

pub static TLS_CERT_FILE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static TLS_KEY_FILE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static TLS_CA_CERT_FILE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

pub static TLS_AUTH_CLIENTS: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"no"));

pub static DATABASES: GucSetting<i32> = GucSetting::<i32>::new(16);

pub fn register_gucs(in_shared_preload: bool) {
    // PostgreSQL forbids defining PGC_POSTMASTER GUCs after server startup (e.g., when
    // `CREATE EXTENSION pg_redis_core` is run without `shared_preload_libraries`).
    let startup_ctx = || {
        if in_shared_preload {
            GucContext::Postmaster
        } else {
            GucContext::Sighup
        }
    };

    GucRegistry::define_string_guc(
        "pg_redis.bind_address",
        "Network interface address for the embedded RESP TCP listener.",
        "Defaults to 0.0.0.0 to accept connections on all interfaces.",
        &BIND_ADDRESS,
        startup_ctx(),
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.port",
        "TCP port for the embedded plain RESP listener (0 disables plain TCP).",
        "Defaults to the standard Redis port 6379. Set to 0 with pg_redis.tls_port > 0 for TLS-only mode.",
        &PORT,
        0,
        65535,
        startup_ctx(),
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pg_redis.database",
        "PostgreSQL database used by the pg_redis_core background worker.",
        "Defaults to 'postgres'.",
        &DATABASE,
        startup_ctx(),
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "pg_redis.unlogged",
        "Create pg_redis catalog tables as UNLOGGED to bypass WAL for pure caching workloads.",
        "Defaults to off (logged, ACID durable).",
        &UNLOGGED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "pg_redis.cache_enabled",
        "Enable the in-process L1 hot-key LRU cache in the background worker.",
        "Defaults to on.",
        &CACHE_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.cache_capacity",
        "Maximum number of entries in the in-process L1 hot-key cache.",
        "Defaults to 10000.",
        &CACHE_CAPACITY,
        0,
        10_000_000,
        startup_ctx(),
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.cache_ttl_ms",
        "Maximum time in milliseconds an entry remains valid in the L1 hot-key cache before revalidation.",
        "Defaults to 500ms.",
        &CACHE_TTL_MS,
        0,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.eviction_interval_ms",
        "Interval in milliseconds between automated TTL eviction batches.",
        "Defaults to 5000ms.",
        &EVICTION_INTERVAL_MS,
        100,
        3_600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pg_redis.requirepass",
        "Password required from RESP clients via AUTH or HELLO ... AUTH.",
        "Unset or empty disables authentication.",
        &REQUIREPASS,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_int_guc(
        "pg_redis.lua_time_limit_ms",
        "Maximum execution time in milliseconds for a single Lua EVAL/EVALSHA script (0 disables).",
        "Defaults to 5000ms.",
        &LUA_TIME_LIMIT_MS,
        0,
        600_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.max_clients",
        "Maximum concurrent RESP TCP connections accepted by the background worker.",
        "Defaults to 1000.",
        &MAX_CLIENTS,
        1,
        100_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.max_frame_bytes",
        "Maximum buffered RESP request bytes allowed per client connection.",
        "Defaults to 64 MiB (67108864 bytes).",
        &MAX_FRAME_BYTES,
        4_096,
        536_870_912,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.pubsub_queue_size",
        "Maximum queued Pub/Sub messages per subscriber before evicting a slow consumer.",
        "Defaults to 4096 messages.",
        &PUBSUB_QUEUE_SIZE,
        16,
        1_000_000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        "pg_redis.tls_port",
        "Optional TLS TCP port for encrypted rediss:// RESP connections (0 disables TLS).",
        "Disabled by default (0). Set to e.g. 6380 along with tls_cert_file and tls_key_file to enable TLS.",
        &TLS_PORT,
        0,
        65535,
        startup_ctx(),
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        "pg_redis.tls_cert_file",
        "Path to the PEM-encoded server certificate chain for the optional TLS listener.",
        "Required when pg_redis.tls_port > 0.",
        &TLS_CERT_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_string_guc(
        "pg_redis.tls_key_file",
        "Path to the PEM-encoded server private key for the optional TLS listener.",
        "Required when pg_redis.tls_port > 0.",
        &TLS_KEY_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_string_guc(
        "pg_redis.tls_ca_cert_file",
        "Optional path to PEM-encoded CA certificates for mTLS client certificate verification.",
        "Required when pg_redis.tls_auth_clients is 'yes' or 'optional'.",
        &TLS_CA_CERT_FILE,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_string_guc(
        "pg_redis.tls_auth_clients",
        "Client certificate verification mode for the optional TLS listener ('no', 'optional', or 'yes').",
        "Defaults to 'no' (server TLS without client certificate requirement).",
        &TLS_AUTH_CLIENTS,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );

    GucRegistry::define_int_guc(
        "pg_redis.databases",
        "Number of logical Redis databases available via SELECT (0 .. databases-1).",
        "Defaults to 16 (matching standard Redis).",
        &DATABASES,
        1,
        1024,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

pub fn bind_address() -> String {
    BIND_ADDRESS
        .get()
        .and_then(|c| c.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "0.0.0.0".to_string())
}

pub fn port() -> u16 {
    PORT.get().clamp(0, 65535) as u16
}

pub fn database() -> String {
    DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "postgres".to_string())
}

pub fn unlogged() -> bool {
    UNLOGGED.get()
}

pub fn cache_enabled() -> bool {
    CACHE_ENABLED.get() && CACHE_CAPACITY.get() > 0 && CACHE_TTL_MS.get() > 0
}

pub fn cache_capacity() -> u64 {
    CACHE_CAPACITY.get().max(0) as u64
}

pub fn cache_ttl_ms() -> u64 {
    CACHE_TTL_MS.get().max(0) as u64
}

pub fn eviction_interval_ms() -> u64 {
    EVICTION_INTERVAL_MS.get().max(100) as u64
}

pub fn requirepass() -> Option<String> {
    REQUIREPASS
        .get()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let len_diff = a.len() ^ b.len();
    let max_len = a.len().max(b.len());
    let mut diff: u8 = 0;
    for i in 0..max_len {
        let ab = *a.get(i).unwrap_or(&0);
        let bb = *b.get(i).unwrap_or(&0);
        diff |= ab ^ bb;
    }
    len_diff == 0 && diff == 0
}

pub fn lua_time_limit_ms() -> u64 {
    LUA_TIME_LIMIT_MS.get().max(0) as u64
}

pub fn max_clients() -> usize {
    MAX_CLIENTS.get().max(1) as usize
}

pub fn max_frame_bytes() -> usize {
    MAX_FRAME_BYTES.get().max(4_096) as usize
}

pub fn pubsub_queue_size() -> usize {
    PUBSUB_QUEUE_SIZE.get().max(16) as usize
}

pub fn tls_port() -> u16 {
    TLS_PORT.get().clamp(0, 65535) as u16
}

pub fn tls_cert_file() -> Option<String> {
    TLS_CERT_FILE
        .get()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

pub fn tls_key_file() -> Option<String> {
    TLS_KEY_FILE
        .get()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

pub fn tls_ca_cert_file() -> Option<String> {
    TLS_CA_CERT_FILE
        .get()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

pub fn tls_auth_clients() -> String {
    TLS_AUTH_CLIENTS
        .get()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_else(|| "no".to_string())
}

pub fn databases() -> i64 {
    DATABASES.get().clamp(1, 1024) as i64
}
