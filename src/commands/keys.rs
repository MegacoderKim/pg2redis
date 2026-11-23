use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::L1Cache;
use crate::protocol::RespValue;

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

const NON_STRING_TABLES: [&str; 5] = [
    "pg_redis_hashes",
    "pg_redis_lists",
    "pg_redis_zsets",
    "pg_redis_sets",
    "pg_redis_streams",
];

/// Fast read-only check: returns `true` if `key` has an expired TTL in `pg_redis_key_expiry`.
pub fn is_non_string_expired(client: &SpiClient, key: &str) -> SpiResult<bool> {
    let table = client.select(
        "SELECT 1 FROM pg_redis_key_expiry WHERE key = $1 AND expires_at <= NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    Ok(!table.is_empty())
}

/// Lazily deletes all non-string rows and the expiry metadata if `key` has expired.
/// Returns `true` if an expired entry was purged.
pub fn purge_if_expired_non_string(client: &mut SpiClient, key: &str) -> SpiResult<bool> {
    if !is_non_string_expired(client, key)? {
        return Ok(false);
    }
    delete_non_string_key(client, key)?;
    Ok(true)
}

/// Remove a key across all five non-string tables plus `pg_redis_key_expiry`.
/// Returns `true` if at least one data row existed before deletion.
fn delete_non_string_key(client: &mut SpiClient, key: &str) -> SpiResult<bool> {
    let mut existed = false;
    for table in NON_STRING_TABLES {
        let sql = format!("DELETE FROM {table} WHERE key = $1");
        let res = client.update(
            &sql,
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?;
        if !res.is_empty() {
            existed = true;
        }
    }
    client.update(
        "DELETE FROM pg_redis_key_expiry WHERE key = $1",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    Ok(existed)
}

/// If removing members emptied a non-string key, drop any leftover TTL metadata so a
/// future recreation of the same key starts persistent (matching Redis semantics).
pub fn cleanup_expiry_if_empty(
    client: &mut SpiClient,
    table: &str,
    key: &str,
) -> SpiResult<()> {
    let sql = format!(
        "DELETE FROM pg_redis_key_expiry \
         WHERE key = $1 \
           AND NOT EXISTS (SELECT 1 FROM {table} WHERE key = $1)"
    );
    client.update(
        &sql,
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    Ok(())
}

fn non_string_key_exists(client: &SpiClient, key: &str) -> SpiResult<bool> {
    if is_non_string_expired(client, key)? {
        return Ok(false);
    }
    let exists = client
        .select(
            "SELECT ( \
                EXISTS(SELECT 1 FROM pg_redis_hashes WHERE key = $1) \
                OR EXISTS(SELECT 1 FROM pg_redis_lists WHERE key = $1) \
                OR EXISTS(SELECT 1 FROM pg_redis_zsets WHERE key = $1) \
                OR EXISTS(SELECT 1 FROM pg_redis_sets WHERE key = $1) \
                OR EXISTS(SELECT 1 FROM pg_redis_streams WHERE key = $1) \
            )",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<bool>()?
        .unwrap_or(false);
    Ok(exists)
}

/// Delete keys across all six catalog tables plus `pg_redis_key_expiry`.
/// Returns the number of distinct active (non-expired) keys removed.
pub fn cmd_del(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 2 {
        return Ok(RespValue::wrong_arity("del"));
    }

    let mut deleted_keys: i64 = 0;
    for raw_key in &args[1..] {
        let key = as_str(raw_key);
        let mut existed = false;

        let s_del = client.update(
            "DELETE FROM pg_redis_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?;
        if !s_del.is_empty() {
            existed = true;
        } else {
            // Clean up any already-expired tombstone row without counting it
            client.update(
                "DELETE FROM pg_redis_strings WHERE key = $1",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
            )?;
        }

        // Purge any already-expired non-string key without counting it as active
        let was_expired_non_string = purge_if_expired_non_string(client, &key)?;
        if !was_expired_non_string && delete_non_string_key(client, &key)? {
            existed = true;
        }

        if let Some(c) = cache {
            c.invalidate(&key);
        }

        if existed {
            deleted_keys += 1;
        }
    }

    Ok(RespValue::Integer(deleted_keys))
}

pub fn cmd_exists(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 2 {
        return Ok(RespValue::wrong_arity("exists"));
    }

    let mut count: i64 = 0;
    for raw_key in &args[1..] {
        let key = as_str(raw_key);
        let found = client
            .select(
                "SELECT ( \
                    EXISTS(SELECT 1 FROM pg_redis_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())) \
                    OR ( \
                        NOT EXISTS(SELECT 1 FROM pg_redis_key_expiry WHERE key = $1 AND expires_at <= NOW()) \
                        AND ( \
                            EXISTS(SELECT 1 FROM pg_redis_hashes WHERE key = $1) \
                            OR EXISTS(SELECT 1 FROM pg_redis_lists WHERE key = $1) \
                            OR EXISTS(SELECT 1 FROM pg_redis_zsets WHERE key = $1) \
                            OR EXISTS(SELECT 1 FROM pg_redis_sets WHERE key = $1) \
                            OR EXISTS(SELECT 1 FROM pg_redis_streams WHERE key = $1) \
                        ) \
                    ) \
                )",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
            )?
            .first()
            .get_one::<bool>()?
            .unwrap_or(false);
        if found {
            count += 1;
        }
    }

    Ok(RespValue::Integer(count))
}

#[derive(Default)]
struct ExpireFlags {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

fn parse_expire_flags(args: &[Bytes]) -> Result<ExpireFlags, RespValue> {
    let mut flags = ExpireFlags::default();
    for raw in args {
        let opt = as_str(raw).to_uppercase();
        match opt.as_str() {
            "NX" => flags.nx = true,
            "XX" => flags.xx = true,
            "GT" => flags.gt = true,
            "LT" => flags.lt = true,
            _ => {
                return Err(RespValue::err(format!(
                    "ERR Unsupported option {opt}"
                )));
            }
        }
    }
    if flags.nx && (flags.xx || flags.gt || flags.lt) {
        return Err(RespValue::err(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if flags.gt && flags.lt {
        return Err(RespValue::err(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    Ok(flags)
}

fn expire_condition_passes(flags: &ExpireFlags, cur_rem_ms: Option<f64>, new_ttl_ms: i64) -> bool {
    if flags.nx && cur_rem_ms.is_some() {
        return false;
    }
    if flags.xx && cur_rem_ms.is_none() {
        return false;
    }
    if flags.gt {
        // A key without expiry has infinite TTL, so no finite TTL is greater.
        match cur_rem_ms {
            Some(cur) if (new_ttl_ms as f64) > cur => {}
            _ => return false,
        }
    }
    if flags.lt {
        // Any finite TTL is strictly less than infinite TTL (None).
        if let Some(cur) = cur_rem_ms {
            if (new_ttl_ms as f64) >= cur {
                return false;
            }
        }
    }
    true
}

pub fn cmd_expire(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
    millis: bool,
    absolute: bool,
) -> SpiResult<RespValue> {
    let cmd_name = match (millis, absolute) {
        (false, false) => "expire",
        (true, false) => "pexpire",
        (false, true) => "expireat",
        (true, true) => "pexpireat",
    };
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity(cmd_name));
    }
    let key = as_str(&args[1]);
    let Some(raw_val) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let flags = match parse_expire_flags(&args[3..]) {
        Ok(f) => f,
        Err(e) => return Ok(e),
    };

    let ttl_ms = if absolute {
        let target_ms = if millis {
            raw_val
        } else {
            raw_val.saturating_mul(1000)
        };
        target_ms - now_unix_ms()
    } else if millis {
        raw_val
    } else {
        raw_val.saturating_mul(1000)
    };

    // 1. Check pg_redis_strings first
    let str_table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION * 1000.0 \
         FROM pg_redis_strings \
         WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
    )?;

    if !str_table.is_empty() {
        let cur_rem_ms: Option<f64> = str_table.first().get(1)?;
        if !expire_condition_passes(&flags, cur_rem_ms, ttl_ms) {
            return Ok(RespValue::Integer(0));
        }

        if ttl_ms <= 0 {
            client.update(
                "DELETE FROM pg_redis_strings WHERE key = $1",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
            )?;
        } else {
            client.update(
                "UPDATE pg_redis_strings \
                 SET expires_at = NOW() + ($2 * INTERVAL '1 millisecond') \
                 WHERE key = $1",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), ttl_ms.into_datum()),
                ]),
            )?;
        }

        if let Some(c) = cache {
            c.invalidate(&key);
        }
        return Ok(RespValue::Integer(1));
    }

    // 2. Check non-string structures (hashes, lists, zsets, sets, streams)
    purge_if_expired_non_string(client, &key)?;
    if !non_string_key_exists(client, &key)? {
        return Ok(RespValue::Integer(0));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION * 1000.0 \
         FROM pg_redis_key_expiry \
         WHERE key = $1 AND expires_at > NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
    )?;
    let cur_rem_ms: Option<f64> = if exp_table.is_empty() {
        None
    } else {
        exp_table.first().get(1)?
    };

    if !expire_condition_passes(&flags, cur_rem_ms, ttl_ms) {
        return Ok(RespValue::Integer(0));
    }

    if ttl_ms <= 0 {
        delete_non_string_key(client, &key)?;
    } else {
        client.update(
            "INSERT INTO pg_redis_key_expiry (key, expires_at) \
             VALUES ($1, NOW() + ($2 * INTERVAL '1 millisecond')) \
             ON CONFLICT (key) DO UPDATE SET expires_at = EXCLUDED.expires_at",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), ttl_ms.into_datum()),
            ]),
        )?;
    }

    Ok(RespValue::Integer(1))
}

pub fn cmd_ttl(client: &SpiClient, args: &[Bytes], millis: bool) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity(if millis { "pttl" } else { "ttl" }));
    }
    let key = as_str(&args[1]);
    let table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION \
         FROM pg_redis_strings \
         WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
    )?;

    if !table.is_empty() {
        let rem_secs: Option<f64> = table.first().get(1)?;
        return match rem_secs {
            None => Ok(RespValue::Integer(-1)),
            Some(secs) => {
                if secs <= 0.0 {
                    Ok(RespValue::Integer(-2))
                } else if millis {
                    Ok(RespValue::Integer((secs * 1000.0).ceil() as i64))
                } else {
                    Ok(RespValue::Integer(secs.ceil() as i64))
                }
            }
        };
    }

    if !non_string_key_exists(client, &key)? {
        return Ok(RespValue::Integer(-2));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION \
         FROM pg_redis_key_expiry \
         WHERE key = $1 AND expires_at > NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;

    if exp_table.is_empty() {
        return Ok(RespValue::Integer(-1));
    }
    let rem_secs: Option<f64> = exp_table.first().get(1)?;
    match rem_secs {
        None => Ok(RespValue::Integer(-1)),
        Some(secs) if secs <= 0.0 => Ok(RespValue::Integer(-2)),
        Some(secs) if millis => Ok(RespValue::Integer((secs * 1000.0).ceil() as i64)),
        Some(secs) => Ok(RespValue::Integer(secs.ceil() as i64)),
    }
}

/// `EXPIRETIME key` / `PEXPIRETIME key` — returns absolute Unix expiry timestamp (`-1` persistent, `-2` missing).
pub fn cmd_expiretime(client: &SpiClient, args: &[Bytes], millis: bool) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity(if millis {
            "pexpiretime"
        } else {
            "expiretime"
        }));
    }
    let key = as_str(&args[1]);
    let table = client.select(
        "SELECT EXTRACT(EPOCH FROM expires_at)::DOUBLE PRECISION \
         FROM pg_redis_strings \
         WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
    )?;

    if !table.is_empty() {
        let epoch_secs: Option<f64> = table.first().get(1)?;
        return Ok(RespValue::Integer(match epoch_secs {
            None => -1,
            Some(s) if millis => (s * 1000.0).round() as i64,
            Some(s) => s.round() as i64,
        }));
    }

    if !non_string_key_exists(client, &key)? {
        return Ok(RespValue::Integer(-2));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM expires_at)::DOUBLE PRECISION \
         FROM pg_redis_key_expiry \
         WHERE key = $1 AND expires_at > NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    if exp_table.is_empty() {
        return Ok(RespValue::Integer(-1));
    }
    let epoch_secs: Option<f64> = exp_table.first().get(1)?;
    Ok(RespValue::Integer(match epoch_secs {
        None => -1,
        Some(s) if millis => (s * 1000.0).round() as i64,
        Some(s) => s.round() as i64,
    }))
}

pub fn cmd_persist(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("persist"));
    }
    let key = as_str(&args[1]);
    let updated = client.update(
        "UPDATE pg_redis_strings \
         SET expires_at = NULL \
         WHERE key = $1 AND expires_at IS NOT NULL AND expires_at > NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
    )?;
    if !updated.is_empty() {
        if let Some(c) = cache {
            c.invalidate(&key);
        }
        return Ok(RespValue::Integer(1));
    }

    purge_if_expired_non_string(client, &key)?;
    if !non_string_key_exists(client, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let removed = client.update(
        "DELETE FROM pg_redis_key_expiry WHERE key = $1 AND expires_at > NOW()",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    Ok(RespValue::Integer(if !removed.is_empty() { 1 } else { 0 }))
}

fn resolve_key_type(client: &SpiClient, key: &str) -> SpiResult<String> {
    let t: Option<String> = client
        .select(
            "SELECT CASE \
                WHEN EXISTS(SELECT 1 FROM pg_redis_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())) THEN 'string' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_key_expiry WHERE key = $1 AND expires_at <= NOW()) THEN 'none' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_hashes WHERE key = $1) THEN 'hash' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_lists WHERE key = $1) THEN 'list' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_zsets WHERE key = $1) THEN 'zset' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_sets WHERE key = $1) THEN 'set' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_streams WHERE key = $1) THEN 'stream' \
                ELSE 'none' \
             END",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<String>()?;

    Ok(t.unwrap_or_else(|| "none".to_string()))
}

pub fn cmd_type(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("type"));
    }
    let key = as_str(&args[1]);
    Ok(RespValue::simple(resolve_key_type(client, &key)?))
}

/// `RENAME key newkey` — atomically moves a key (and its TTL) across all catalog tables, overwriting `newkey`.
pub fn cmd_rename(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
    mutated_keys: &mut Vec<String>,
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("rename"));
    }
    let old_key = as_str(&args[1]);
    let new_key = as_str(&args[2]);

    purge_if_expired_non_string(client, &old_key)?;
    let key_type = resolve_key_type(client, &old_key)?;

    if key_type == "none" {
        return Ok(RespValue::err("ERR no such key"));
    }

    if old_key == new_key {
        return Ok(RespValue::ok());
    }

    // Clear any existing target key across all catalog tables + expiry metadata
    client.update(
        "DELETE FROM pg_redis_strings WHERE key = $1",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum())]),
    )?;
    delete_non_string_key(client, &new_key)?;

    let target_table = match key_type.as_str() {
        "string" => "pg_redis_strings",
        "hash" => "pg_redis_hashes",
        "list" => "pg_redis_lists",
        "zset" => "pg_redis_zsets",
        "set" => "pg_redis_sets",
        "stream" => "pg_redis_streams",
        _ => return Ok(RespValue::err("ERR no such key")),
    };

    let upd_sql = format!("UPDATE {target_table} SET key = $2 WHERE key = $1");
    client.update(
        &upd_sql,
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), old_key.clone().into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum()),
        ]),
    )?;

    if key_type != "string" {
        client.update(
            "UPDATE pg_redis_key_expiry SET key = $2 WHERE key = $1",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), old_key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum()),
            ]),
        )?;
    }

    if let Some(c) = cache {
        c.invalidate(&old_key);
        c.invalidate(&new_key);
    }

    if matches!(key_type.as_str(), "list" | "zset" | "stream") {
        mutated_keys.push(new_key);
    }

    Ok(RespValue::ok())
}

fn collect_all_keys(client: &SpiClient, type_filter: Option<&str>) -> SpiResult<Vec<String>> {
    let sql = match type_filter {
        None => {
            "SELECT key FROM pg_redis_strings WHERE expires_at IS NULL OR expires_at > NOW() \
             UNION \
             SELECT key FROM ( \
                SELECT DISTINCT key FROM pg_redis_hashes \
                UNION SELECT DISTINCT key FROM pg_redis_lists \
                UNION SELECT DISTINCT key FROM pg_redis_zsets \
                UNION SELECT DISTINCT key FROM pg_redis_sets \
                UNION SELECT DISTINCT key FROM pg_redis_streams \
             ) ns \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = ns.key AND e.expires_at <= NOW())"
        }
        Some("string") => {
            "SELECT key FROM pg_redis_strings WHERE expires_at IS NULL OR expires_at > NOW()"
        }
        Some("hash") => {
            "SELECT DISTINCT h.key FROM pg_redis_hashes h \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = h.key AND e.expires_at <= NOW())"
        }
        Some("list") => {
            "SELECT DISTINCT l.key FROM pg_redis_lists l \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = l.key AND e.expires_at <= NOW())"
        }
        Some("zset") => {
            "SELECT DISTINCT z.key FROM pg_redis_zsets z \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = z.key AND e.expires_at <= NOW())"
        }
        Some("set") => {
            "SELECT DISTINCT s.key FROM pg_redis_sets s \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = s.key AND e.expires_at <= NOW())"
        }
        Some("stream") => {
            "SELECT DISTINCT x.key FROM pg_redis_streams x \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.key = x.key AND e.expires_at <= NOW())"
        }
        Some(_) => return Ok(Vec::new()),
    };

    let table = client.select(sql, None, None)?;

    let mut keys = BTreeSet::new();
    for row in table {
        if let Some(k) = row.get::<String>(1)? {
            keys.insert(k);
        }
    }
    Ok(keys.into_iter().collect())
}

pub fn cmd_keys(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("keys"));
    }
    let pattern = as_str(&args[1]);
    let all_keys = collect_all_keys(client, None)?;
    let matched = all_keys
        .into_iter()
        .filter(|k| glob_match(&pattern, k))
        .map(RespValue::bulk)
        .collect();
    Ok(RespValue::Array(matched))
}

pub fn cmd_scan(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 2 {
        return Ok(RespValue::wrong_arity("scan"));
    }
    let Some(cursor) = parse_i64(&args[1]) else {
        return Ok(RespValue::err("ERR invalid cursor"));
    };
    if cursor < 0 {
        return Ok(RespValue::err("ERR invalid cursor"));
    }

    let mut pattern: Option<String> = None;
    let mut type_filter: Option<String> = None;
    let mut count: usize = 10;

    let mut i = 2;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                pattern = Some(as_str(&args[i + 1]));
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(c) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if c <= 0 {
                    return Ok(RespValue::syntax_err());
                }
                count = c as usize;
                i += 2;
            }
            "TYPE" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                type_filter = Some(as_str(&args[i + 1]).to_ascii_lowercase());
                i += 2;
            }
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    let all_keys = collect_all_keys(client, type_filter.as_deref())?;
    let start = cursor as usize;
    let end = (start + count).min(all_keys.len());
    let next_cursor = if end >= all_keys.len() { 0 } else { end };

    let mut batch = Vec::new();
    if start < all_keys.len() {
        for k in &all_keys[start..end] {
            if let Some(ref pat) = pattern {
                if !glob_match(pat, k) {
                    continue;
                }
            }
            batch.push(RespValue::bulk(k.clone()));
        }
    }

    Ok(RespValue::Array(vec![
        RespValue::bulk(next_cursor.to_string()),
        RespValue::Array(batch),
    ]))
}

pub fn cmd_dbsize(client: &SpiClient) -> SpiResult<RespValue> {
    let count = collect_all_keys(client, None)?.len() as i64;
    Ok(RespValue::Integer(count))
}

pub fn cmd_flushdb(client: &mut SpiClient, cache: Option<&L1Cache>) -> SpiResult<RespValue> {
    client.update(
        "DELETE FROM pg_redis_strings; \
         DELETE FROM pg_redis_hashes; \
         DELETE FROM pg_redis_lists; \
         DELETE FROM pg_redis_zsets; \
         DELETE FROM pg_redis_sets; \
         DELETE FROM pg_redis_streams; \
         DELETE FROM pg_redis_key_expiry;",
        None,
        None,
    )?;
    if let Some(c) = cache {
        c.clear();
    }
    Ok(RespValue::ok())
}

/// Redis-style glob pattern matcher supporting `*`, `?`, `[abc]`, `[^a]`, `[a-z]`, and `\x`.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    glob_match_slice(&p, &t)
}

fn glob_match_slice(p: &[char], t: &[char]) -> bool {
    if p.is_empty() {
        return t.is_empty();
    }
    match p[0] {
        '*' => {
            let mut pi = 1;
            while pi < p.len() && p[pi] == '*' {
                pi += 1;
            }
            if pi == p.len() {
                return true;
            }
            for ti in 0..=t.len() {
                if glob_match_slice(&p[pi..], &t[ti..]) {
                    return true;
                }
            }
            false
        }
        '?' => !t.is_empty() && glob_match_slice(&p[1..], &t[1..]),
        '[' => {
            if t.is_empty() {
                return false;
            }
            let mut pi = 1;
            let negated = pi < p.len() && p[pi] == '^';
            if negated {
                pi += 1;
            }
            let mut matched = false;
            let ch = t[0];
            while pi < p.len() && p[pi] != ']' {
                if pi + 2 < p.len() && p[pi + 1] == '-' && p[pi + 2] != ']' {
                    if ch >= p[pi] && ch <= p[pi + 2] {
                        matched = true;
                    }
                    pi += 3;
                } else {
                    if ch == p[pi] {
                        matched = true;
                    }
                    pi += 1;
                }
            }
            if pi >= p.len() || p[pi] != ']' {
                return false;
            }
            if negated {
                matched = !matched;
            }
            matched && glob_match_slice(&p[pi + 1..], &t[1..])
        }
        '\\' if p.len() >= 2 => {
            !t.is_empty() && p[1] == t[0] && glob_match_slice(&p[2..], &t[1..])
        }
        c => !t.is_empty() && c == t[0] && glob_match_slice(&p[1..], &t[1..]),
    }
}
