use bytes::Bytes;
use moka::sync::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::config;

#[derive(Clone, Debug)]
struct CachedString {
    value: Bytes,
    key_expires_at: Option<Instant>,
    cache_expires_at: Instant,
}

#[derive(Debug)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub invalidations: u64,
    pub entry_count: u64,
}

/// In-process L1 hot-key cache backed by `moka::sync::Cache`.
///
/// Supports write-through updates/invalidation from the background worker and
/// tracks both the Redis key's own TTL (`key_expires_at`) and a bounded L1
/// revalidation deadline (`cache_expires_at`).
pub struct L1Cache {
    inner: Cache<String, CachedString>,
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
}

impl L1Cache {
    pub fn new(max_capacity: u64) -> Self {
        Self {
            inner: Cache::builder().max_capacity(max_capacity).build(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        if !config::cache_enabled() {
            return None;
        }

        let now = Instant::now();
        if let Some(entry) = self.inner.get(key) {
            if let Some(key_exp) = entry.key_expires_at {
                if now >= key_exp {
                    self.inner.invalidate(key);
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            if now >= entry.cache_expires_at {
                self.inner.invalidate(key);
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.value);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    pub fn insert(&self, key: String, value: Bytes, remaining_ttl: Option<Duration>) {
        if !config::cache_enabled() {
            return;
        }

        let now = Instant::now();
        let key_expires_at = match remaining_ttl {
            Some(d) if d.is_zero() => {
                self.inner.invalidate(&key);
                return;
            }
            Some(d) => Some(now + d),
            None => None,
        };

        let cache_ttl = Duration::from_millis(config::cache_ttl_ms());
        let cache_expires_at = match key_expires_at {
            Some(key_exp) => (now + cache_ttl).min(key_exp),
            None => now + cache_ttl,
        };

        self.inner.insert(
            key,
            CachedString {
                value,
                key_expires_at,
                cache_expires_at,
            },
        );
    }

    pub fn invalidate(&self, key: &str) {
        self.inner.invalidate(key);
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn clear(&self) {
        self.inner.invalidate_all();
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            entry_count: self.inner.entry_count(),
        }
    }
}
