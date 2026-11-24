use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::L1Cache;
use crate::config;
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

const ALL_TABLES_CANONICAL: [&str; 7] = [
    "pg_redis_strings",
    "pg_redis_key_expiry",
    "pg_redis_hashes",
    "pg_redis_lists",
    "pg_redis_zsets",
    "pg_redis_sets",
    "pg_redis_streams",
];

/// Fast read-only check: returns `true` if `(db, key)` has an expired TTL in `pg_redis_key_expiry`.
pub fn is_non_string_expired(client: &SpiClient, db: i16, key: &str) -> SpiResult<bool> {
    let table = client.select(
        "SELECT 1 FROM pg_redis_key_expiry WHERE db = $1 AND key = $2 AND expires_at <= NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;
    Ok(!table.is_empty())
}

/// Lazily deletes all non-string rows and the expiry metadata if `(db, key)` has expired.
/// Returns `true` if an expired entry was purged.
pub fn purge_if_expired_non_string(client: &mut SpiClient, db: i16, key: &str) -> SpiResult<bool> {
    if !is_non_string_expired(client, db, key)? {
        return Ok(false);
    }
    delete_non_string_key(client, db, key)?;
    Ok(true)
}

/// Remove `(db, key)` across all five non-string tables plus `pg_redis_key_expiry`.
/// Returns `true` if at least one data row existed before deletion.
fn delete_non_string_key(client: &mut SpiClient, db: i16, key: &str) -> SpiResult<bool> {
    let mut existed = false;
    for table in NON_STRING_TABLES {
        let sql = format!("DELETE FROM {table} WHERE db = $1 AND key = $2");
        let res = client.update(
            &sql,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?;
        if !res.is_empty() {
            existed = true;
        }
    }
    client.update(
        "DELETE FROM pg_redis_key_expiry WHERE db = $1 AND key = $2",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;
    Ok(existed)
}

/// If removing members emptied a non-string key, drop any leftover TTL metadata so a
/// future recreation of the same key starts persistent (matching Redis semantics).
pub fn cleanup_expiry_if_empty(
    client: &mut SpiClient,
    table: &str,
    db: i16,
    key: &str,
) -> SpiResult<()> {
    let sql = format!(
        "DELETE FROM pg_redis_key_expiry \
         WHERE db = $1 AND key = $2 \
           AND NOT EXISTS (SELECT 1 FROM {table} WHERE db = $1 AND key = $2)"
    );
    client.update(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;
    Ok(())
}

fn non_string_key_exists(client: &SpiClient, db: i16, key: &str) -> SpiResult<bool> {
    if is_non_string_expired(client, db, key)? {
        return Ok(false);
    }
    let exists = client
        .select(
            "SELECT ( \
                EXISTS(SELECT 1 FROM pg_redis_hashes WHERE db = $1 AND key = $2) \
                OR EXISTS(SELECT 1 FROM pg_redis_lists WHERE db = $1 AND key = $2) \
                OR EXISTS(SELECT 1 FROM pg_redis_zsets WHERE db = $1 AND key = $2) \
                OR EXISTS(SELECT 1 FROM pg_redis_sets WHERE db = $1 AND key = $2) \
                OR EXISTS(SELECT 1 FROM pg_redis_streams WHERE db = $1 AND key = $2) \
            )",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?
        .first()
        .get_one::<bool>()?
        .unwrap_or(false);
    Ok(exists)
}

/// Delete keys in `db` across all six catalog tables plus `pg_redis_key_expiry`.
/// Returns the number of distinct active (non-expired) keys removed.
pub fn cmd_del(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    db: i16,
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
            "DELETE FROM pg_redis_strings WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
        )?;
        if !s_del.is_empty() {
            existed = true;
        } else {
            // Clean up any already-expired tombstone row without counting it
            client.update(
                "DELETE FROM pg_redis_strings WHERE db = $1 AND key = $2",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                ]),
            )?;
        }

        // Purge any already-expired non-string key without counting it as active
        let was_expired_non_string = purge_if_expired_non_string(client, db, &key)?;
        if !was_expired_non_string && delete_non_string_key(client, db, &key)? {
            existed = true;
        }

        if let Some(c) = cache {
            c.invalidate(db, &key);
        }

        if existed {
            deleted_keys += 1;
        }
    }

    Ok(RespValue::Integer(deleted_keys))
}

pub fn cmd_exists(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 2 {
        return Ok(RespValue::wrong_arity("exists"));
    }

    let mut count: i64 = 0;
    for raw_key in &args[1..] {
        let key = as_str(raw_key);
        let found = client
            .select(
                "SELECT ( \
                    EXISTS(SELECT 1 FROM pg_redis_strings WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())) \
                    OR ( \
                        NOT EXISTS(SELECT 1 FROM pg_redis_key_expiry WHERE db = $1 AND key = $2 AND expires_at <= NOW()) \
                        AND ( \
                            EXISTS(SELECT 1 FROM pg_redis_hashes WHERE db = $1 AND key = $2) \
                            OR EXISTS(SELECT 1 FROM pg_redis_lists WHERE db = $1 AND key = $2) \
                            OR EXISTS(SELECT 1 FROM pg_redis_zsets WHERE db = $1 AND key = $2) \
                            OR EXISTS(SELECT 1 FROM pg_redis_sets WHERE db = $1 AND key = $2) \
                            OR EXISTS(SELECT 1 FROM pg_redis_streams WHERE db = $1 AND key = $2) \
                        ) \
                    ) \
                )",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                ]),
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
        match cur_rem_ms {
            Some(cur) if (new_ttl_ms as f64) > cur => {}
            _ => return false,
        }
    }
    if flags.lt {
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
    db: i16,
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
         WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
    )?;

    if !str_table.is_empty() {
        let cur_rem_ms: Option<f64> = str_table.first().get(1)?;
        if !expire_condition_passes(&flags, cur_rem_ms, ttl_ms) {
            return Ok(RespValue::Integer(0));
        }

        if ttl_ms <= 0 {
            client.update(
                "DELETE FROM pg_redis_strings WHERE db = $1 AND key = $2",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                ]),
            )?;
        } else {
            client.update(
                "UPDATE pg_redis_strings \
                 SET expires_at = NOW() + ($3 * INTERVAL '1 millisecond') \
                 WHERE db = $1 AND key = $2",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), ttl_ms.into_datum()),
                ]),
            )?;
        }

        if let Some(c) = cache {
            c.invalidate(db, &key);
        }
        return Ok(RespValue::Integer(1));
    }

    // 2. Check non-string structures (hashes, lists, zsets, sets, streams)
    purge_if_expired_non_string(client, db, &key)?;
    if !non_string_key_exists(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION * 1000.0 \
         FROM pg_redis_key_expiry \
         WHERE db = $1 AND key = $2 AND expires_at > NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
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
        delete_non_string_key(client, db, &key)?;
    } else {
        client.update(
            "INSERT INTO pg_redis_key_expiry (db, key, expires_at) \
             VALUES ($1, $2, NOW() + ($3 * INTERVAL '1 millisecond')) \
             ON CONFLICT (db, key) DO UPDATE SET expires_at = EXCLUDED.expires_at",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), ttl_ms.into_datum()),
            ]),
        )?;
    }

    Ok(RespValue::Integer(1))
}

pub fn cmd_ttl(client: &SpiClient, db: i16, args: &[Bytes], millis: bool) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity(if millis { "pttl" } else { "ttl" }));
    }
    let key = as_str(&args[1]);
    let table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION \
         FROM pg_redis_strings \
         WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
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

    if !non_string_key_exists(client, db, &key)? {
        return Ok(RespValue::Integer(-2));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM (expires_at - NOW()))::DOUBLE PRECISION \
         FROM pg_redis_key_expiry \
         WHERE db = $1 AND key = $2 AND expires_at > NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
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
pub fn cmd_expiretime(
    client: &SpiClient,
    db: i16,
    args: &[Bytes],
    millis: bool,
) -> SpiResult<RespValue> {
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
         WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
    )?;

    if !table.is_empty() {
        let epoch_secs: Option<f64> = table.first().get(1)?;
        return Ok(RespValue::Integer(match epoch_secs {
            None => -1,
            Some(s) if millis => (s * 1000.0).round() as i64,
            Some(s) => s.round() as i64,
        }));
    }

    if !non_string_key_exists(client, db, &key)? {
        return Ok(RespValue::Integer(-2));
    }

    let exp_table = client.select(
        "SELECT EXTRACT(EPOCH FROM expires_at)::DOUBLE PRECISION \
         FROM pg_redis_key_expiry \
         WHERE db = $1 AND key = $2 AND expires_at > NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
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
    db: i16,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("persist"));
    }
    let key = as_str(&args[1]);
    let updated = client.update(
        "UPDATE pg_redis_strings \
         SET expires_at = NULL \
         WHERE db = $1 AND key = $2 AND expires_at IS NOT NULL AND expires_at > NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
    )?;
    if !updated.is_empty() {
        if let Some(c) = cache {
            c.invalidate(db, &key);
        }
        return Ok(RespValue::Integer(1));
    }

    purge_if_expired_non_string(client, db, &key)?;
    if !non_string_key_exists(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let removed = client.update(
        "DELETE FROM pg_redis_key_expiry WHERE db = $1 AND key = $2 AND expires_at > NOW()",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;
    Ok(RespValue::Integer(if !removed.is_empty() { 1 } else { 0 }))
}

fn resolve_key_type(client: &SpiClient, db: i16, key: &str) -> SpiResult<String> {
    let t: Option<String> = client
        .select(
            "SELECT CASE \
                WHEN EXISTS(SELECT 1 FROM pg_redis_strings WHERE db = $1 AND key = $2 AND (expires_at IS NULL OR expires_at > NOW())) THEN 'string' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_key_expiry WHERE db = $1 AND key = $2 AND expires_at <= NOW()) THEN 'none' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_hashes WHERE db = $1 AND key = $2) THEN 'hash' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_lists WHERE db = $1 AND key = $2) THEN 'list' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_zsets WHERE db = $1 AND key = $2) THEN 'zset' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_sets WHERE db = $1 AND key = $2) THEN 'set' \
                WHEN EXISTS(SELECT 1 FROM pg_redis_streams WHERE db = $1 AND key = $2) THEN 'stream' \
                ELSE 'none' \
             END",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?
        .first()
        .get_one::<String>()?;

    Ok(t.unwrap_or_else(|| "none".to_string()))
}

pub fn cmd_type(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("type"));
    }
    let key = as_str(&args[1]);
    Ok(RespValue::simple(resolve_key_type(client, db, &key)?))
}

/// `RENAME key newkey` — atomically moves a key (and its TTL) within `db`, overwriting `newkey`.
pub fn cmd_rename(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    db: i16,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("rename"));
    }
    let old_key = as_str(&args[1]);
    let new_key = as_str(&args[2]);

    purge_if_expired_non_string(client, db, &old_key)?;
    let key_type = resolve_key_type(client, db, &old_key)?;

    if key_type == "none" {
        return Ok(RespValue::err("ERR no such key"));
    }

    if old_key == new_key {
        return Ok(RespValue::ok());
    }

    // Clear any existing target key in `db` across all catalog tables + expiry metadata
    client.update(
        "DELETE FROM pg_redis_strings WHERE db = $1 AND key = $2",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum()),
        ]),
    )?;
    delete_non_string_key(client, db, &new_key)?;

    let target_table = match key_type.as_str() {
        "string" => "pg_redis_strings",
        "hash" => "pg_redis_hashes",
        "list" => "pg_redis_lists",
        "zset" => "pg_redis_zsets",
        "set" => "pg_redis_sets",
        "stream" => "pg_redis_streams",
        _ => return Ok(RespValue::err("ERR no such key")),
    };

    let upd_sql = format!("UPDATE {target_table} SET key = $3 WHERE db = $1 AND key = $2");
    client.update(
        &upd_sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), old_key.clone().into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum()),
        ]),
    )?;

    if key_type != "string" {
        client.update(
            "UPDATE pg_redis_key_expiry SET key = $3 WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), old_key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), new_key.clone().into_datum()),
            ]),
        )?;
    }

    if let Some(c) = cache {
        c.invalidate(db, &old_key);
        c.invalidate(db, &new_key);
    }

    if matches!(key_type.as_str(), "list" | "zset" | "stream") {
        mutated_keys.push((db, new_key));
    }

    Ok(RespValue::ok())
}

/// `MOVE key db` — atomically moves `key` (and its TTL) from current `db` to `target_db` if absent there.
pub fn cmd_move(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    db: i16,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("move"));
    }
    let key = as_str(&args[1]);
    let Some(dst_db_i64) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    if dst_db_i64 < 0 || dst_db_i64 >= config::databases() {
        return Ok(RespValue::err("ERR DB index is out of range"));
    }
    let dst_db = dst_db_i64 as i16;
    if dst_db == db {
        return Ok(RespValue::err("ERR source and destination objects are the same"));
    }

    purge_if_expired_non_string(client, db, &key)?;
    let src_type = resolve_key_type(client, db, &key)?;
    if src_type == "none" {
        return Ok(RespValue::Integer(0));
    }

    purge_if_expired_non_string(client, dst_db, &key)?;
    if resolve_key_type(client, dst_db, &key)? != "none" {
        return Ok(RespValue::Integer(0));
    }

    // Clear any expired string/non-string tombstones in destination DB before moving
    client.update(
        "DELETE FROM pg_redis_strings WHERE db = $1 AND key = $2",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), dst_db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
        ]),
    )?;
    delete_non_string_key(client, dst_db, &key)?;

    let target_table = match src_type.as_str() {
        "string" => "pg_redis_strings",
        "hash" => "pg_redis_hashes",
        "list" => "pg_redis_lists",
        "zset" => "pg_redis_zsets",
        "set" => "pg_redis_sets",
        "stream" => "pg_redis_streams",
        _ => return Ok(RespValue::Integer(0)),
    };

    let upd_sql = format!("UPDATE {target_table} SET db = $3 WHERE db = $1 AND key = $2");
    client.update(
        &upd_sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            (PgBuiltInOids::INT2OID.oid(), dst_db.into_datum()),
        ]),
    )?;

    if src_type != "string" {
        client.update(
            "UPDATE pg_redis_key_expiry SET db = $3 WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::INT2OID.oid(), dst_db.into_datum()),
            ]),
        )?;
    }

    if let Some(c) = cache {
        c.invalidate(db, &key);
        c.invalidate(dst_db, &key);
    }

    if matches!(src_type.as_str(), "list" | "zset" | "stream") {
        mutated_keys.push((dst_db, key));
    }

    Ok(RespValue::Integer(1))
}

/// `SWAPDB index1 index2` — atomically swaps all keys and expiries between two Redis databases.
pub fn cmd_swapdb(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("swapdb"));
    }
    let Some(db1_i64) = parse_i64(&args[1]) else {
        return Ok(RespValue::err("ERR invalid first DB index"));
    };
    let Some(db2_i64) = parse_i64(&args[2]) else {
        return Ok(RespValue::err("ERR invalid second DB index"));
    };
    let max_dbs = config::databases();
    if db1_i64 < 0 || db1_i64 >= max_dbs {
        return Ok(RespValue::err("ERR invalid first DB index"));
    }
    if db2_i64 < 0 || db2_i64 >= max_dbs {
        return Ok(RespValue::err("ERR invalid second DB index"));
    }
    if db1_i64 == db2_i64 {
        return Ok(RespValue::ok());
    }

    let db1 = db1_i64 as i16;
    let db2 = db2_i64 as i16;
    let tmp: i16 = -1;

    for tbl in ALL_TABLES_CANONICAL {
        let s1 = format!("UPDATE {tbl} SET db = $1 WHERE db = $2");
        client.update(
            &s1,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), tmp.into_datum()),
                (PgBuiltInOids::INT2OID.oid(), db1.into_datum()),
            ]),
        )?;
        client.update(
            &s1,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db1.into_datum()),
                (PgBuiltInOids::INT2OID.oid(), db2.into_datum()),
            ]),
        )?;
        client.update(
            &s1,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db2.into_datum()),
                (PgBuiltInOids::INT2OID.oid(), tmp.into_datum()),
            ]),
        )?;
    }

    if let Some(c) = cache {
        c.clear();
    }

    Ok(RespValue::ok())
}

fn collect_all_keys(
    client: &SpiClient,
    db: i16,
    type_filter: Option<&str>,
) -> SpiResult<Vec<String>> {
    let sql = match type_filter {
        None => {
            "SELECT key FROM pg_redis_strings WHERE db = $1 AND (expires_at IS NULL OR expires_at > NOW()) \
             UNION \
             SELECT key FROM ( \
                SELECT DISTINCT key FROM pg_redis_hashes WHERE db = $1 \
                UNION SELECT DISTINCT key FROM pg_redis_lists WHERE db = $1 \
                UNION SELECT DISTINCT key FROM pg_redis_zsets WHERE db = $1 \
                UNION SELECT DISTINCT key FROM pg_redis_sets WHERE db = $1 \
                UNION SELECT DISTINCT key FROM pg_redis_streams WHERE db = $1 \
             ) ns \
             WHERE NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = ns.key AND e.expires_at <= NOW())"
        }
        Some("string") => {
            "SELECT key FROM pg_redis_strings WHERE db = $1 AND (expires_at IS NULL OR expires_at > NOW())"
        }
        Some("hash") => {
            "SELECT DISTINCT h.key FROM pg_redis_hashes h \
             WHERE h.db = $1 \
               AND NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = h.key AND e.expires_at <= NOW())"
        }
        Some("list") => {
            "SELECT DISTINCT l.key FROM pg_redis_lists l \
             WHERE l.db = $1 \
               AND NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = l.key AND e.expires_at <= NOW())"
        }
        Some("zset") => {
            "SELECT DISTINCT z.key FROM pg_redis_zsets z \
             WHERE z.db = $1 \
               AND NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = z.key AND e.expires_at <= NOW())"
        }
        Some("set") => {
            "SELECT DISTINCT s.key FROM pg_redis_sets s \
             WHERE s.db = $1 \
               AND NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = s.key AND e.expires_at <= NOW())"
        }
        Some("stream") => {
            "SELECT DISTINCT x.key FROM pg_redis_streams x \
             WHERE x.db = $1 \
               AND NOT EXISTS (SELECT 1 FROM pg_redis_key_expiry e WHERE e.db = $1 AND e.key = x.key AND e.expires_at <= NOW())"
        }
        Some(_) => return Ok(Vec::new()),
    };

    let table = client.select(
        sql,
        None,
        Some(vec![(PgBuiltInOids::INT2OID.oid(), db.into_datum())]),
    )?;

    let mut keys = BTreeSet::new();
    for row in table {
        if let Some(k) = row.get::<String>(1)? {
            keys.insert(k);
        }
    }
    Ok(keys.into_iter().collect())
}

pub fn cmd_keys(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("keys"));
    }
    let pattern = as_str(&args[1]);
    let all_keys = collect_all_keys(client, db, None)?;
    let matched = all_keys
        .into_iter()
        .filter(|k| glob_match(&pattern, k))
        .map(RespValue::bulk)
        .collect();
    Ok(RespValue::Array(matched))
}

pub fn cmd_scan(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
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

    let all_keys = collect_all_keys(client, db, type_filter.as_deref())?;
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

pub fn cmd_dbsize(client: &SpiClient, db: i16) -> SpiResult<RespValue> {
    let count = collect_all_keys(client, db, None)?.len() as i64;
    Ok(RespValue::Integer(count))
}

fn validate_flush_args(args: &[Bytes]) -> Result<(), RespValue> {
    if args.len() == 1 {
        return Ok(());
    }
    if args.len() == 2 {
        let opt = as_str(&args[1]).to_uppercase();
        if matches!(opt.as_str(), "ASYNC" | "SYNC") {
            return Ok(());
        }
    }
    Err(RespValue::syntax_err())
}

/// `FLUSHDB [ASYNC|SYNC]` — deletes all keys in the currently selected DB only.
pub fn cmd_flushdb(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    db: i16,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if let Err(e) = validate_flush_args(args) {
        return Ok(e);
    }
    for tbl in ALL_TABLES_CANONICAL {
        let sql = format!("DELETE FROM {tbl} WHERE db = $1");
        client.update(
            &sql,
            None,
            Some(vec![(PgBuiltInOids::INT2OID.oid(), db.into_datum())]),
        )?;
    }
    if let Some(c) = cache {
        c.clear();
    }
    Ok(RespValue::ok())
}

/// `FLUSHALL [ASYNC|SYNC]` — deletes all keys across every logical Redis database.
pub fn cmd_flushall(
    client: &mut SpiClient,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if let Err(e) = validate_flush_args(args) {
        return Ok(e);
    }
    for tbl in ALL_TABLES_CANONICAL {
        let sql = format!("DELETE FROM {tbl}");
        client.update(&sql, None, None)?;
    }
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
            let mut next_p = 1;
            while next_p < p.len() && p[next_p] == '*' {
                next_p += 1;
            }
            if next_p == p.len() {
                return true;
            }
            for i in 0..=t.len() {
                if glob_match_slice(&p[next_p..], &t[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => !t.is_empty() && glob_match_slice(&p[1..], &t[1..]),
        '\\' => {
            if p.len() >= 2 {
                !t.is_empty() && t[0] == p[1] && glob_match_slice(&p[2..], &t[1..])
            } else {
                !t.is_empty() && t[0] == '\\' && glob_match_slice(&p[1..], &t[1..])
            }
        }
        '[' => {
            if t.is_empty() {
                return false;
            }
            let mut idx = 1;
            let negate = if idx < p.len() && p[idx] == '^' {
                idx += 1;
                true
            } else {
                false
            };
            let mut matched = false;
            let ch = t[0];
            while idx < p.len() && p[idx] != ']' {
                if p[idx] == '\\' && idx + 1 < p.len() {
                    if p[idx + 1] == ch {
                        matched = true;
                    }
                    idx += 2;
                } else if idx + 2 < p.len() && p[idx + 1] == '-' && p[idx + 2] != ']' {
                    if ch >= p[idx] && ch <= p[idx + 2] {
                        matched = true;
                    }
                    idx += 3;
                } else {
                    if p[idx] == ch {
                        matched = true;
                    }
                    idx += 1;
                }
            }
            if idx < p.len() && p[idx] == ']' {
                idx += 1;
            }
            if negate {
                matched = !matched;
            }
            matched && glob_match_slice(&p[idx..], &t[1..])
        }
        c => !t.is_empty() && t[0] == c && glob_match_slice(&p[1..], &t[1..]),
    }
}
