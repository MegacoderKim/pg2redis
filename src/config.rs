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
        "TCP port for the embedded RESP listener.",
        "Defaults to the standard Redis port 6379.",
        &PORT,
        1,
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
}

pub fn bind_address() -> String {
    BIND_ADDRESS
        .get()
        .and_then(|c| c.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "0.0.0.0".to_string())
}

pub fn port() -> u16 {
    PORT.get().clamp(1, 65535) as u16
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
