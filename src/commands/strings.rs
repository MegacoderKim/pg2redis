use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cache::L1Cache;
use crate::protocol::{format_score, RespValue};
use crate::storage::{
    PreparedPlans, SQL_STRING_GET, SQL_STRING_SET_KEEP_TTL, SQL_STRING_SET_NO_TTL,
    SQL_STRING_SET_WITH_MS,
};

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

fn parse_f64(b: &Bytes) -> Option<f64> {
    let v = std::str::from_utf8(b).ok()?.parse::<f64>().ok()?;
    if v.is_nan() || v.is_infinite() {
        None
    } else {
        Some(v)
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy)]
enum SetExpiry {
    None,
    Millis(i64),
    KeepTtl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SetCondition {
    Always,
    Nx,
    Xx,
}

pub fn get_from_db(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    key: &str,
) -> SpiResult<Option<Bytes>> {
    let table = match plans {
        Some(p) => client.select(&p.string_get, None, Some(vec![key.into_datum()]))?,
        None => client.select(
            SQL_STRING_GET,
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?,
    };

    if table.is_empty() {
        return Ok(None);
    }

    let row = table.first();
    let val_bytes: Option<Vec<u8>> = row.get(1)?;
    let ttl_secs: Option<f64> = row.get(2)?;

    match val_bytes {
        Some(raw) => {
            let bytes = Bytes::from(raw);
            if let Some(c) = cache {
                let rem = ttl_secs.and_then(|s| {
                    if s > 0.0 {
                        Some(Duration::from_secs_f64(s))
                    } else {
                        None
                    }
                });
                c.insert(key.to_string(), bytes.clone(), rem);
            }
            Ok(Some(bytes))
        }
        None => Ok(None),
    }
}

pub fn cmd_get(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("get"));
    }
    let key = as_str(&args[1]);
    if let Some(c) = cache {
        if let Some(val) = c.get(&key) {
            return Ok(RespValue::BulkString(val));
        }
    }
    match get_from_db(client, plans, cache, &key)? {
        Some(b) => Ok(RespValue::BulkString(b)),
        None => Ok(RespValue::Null),
    }
}

pub fn cmd_set(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("set"));
    }
    let key = as_str(&args[1]);
    let value = args[2].clone();

    let mut expiry = SetExpiry::None;
    let mut expiry_set = false;
    let mut cond = SetCondition::Always;
    let mut return_old = false;

    let mut i = 3;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => {
                if cond == SetCondition::Xx {
                    return Ok(RespValue::syntax_err());
                }
                cond = SetCondition::Nx;
                i += 1;
            }
            "XX" => {
                if cond == SetCondition::Nx {
                    return Ok(RespValue::syntax_err());
                }
                cond = SetCondition::Xx;
                i += 1;
            }
            "GET" => {
                return_old = true;
                i += 1;
            }
            "KEEPTTL" => {
                if expiry_set {
                    return Ok(RespValue::syntax_err());
                }
                expiry = SetExpiry::KeepTtl;
                expiry_set = true;
                i += 1;
            }
            "EX" => {
                if expiry_set || i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(secs) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if secs <= 0 {
                    return Ok(RespValue::err("ERR invalid expire time in 'set' command"));
                }
                let Some(ms) = secs.checked_mul(1000) else {
                    return Ok(RespValue::err("ERR invalid expire time in 'set' command"));
                };
                expiry = SetExpiry::Millis(ms);
                expiry_set = true;
                i += 2;
            }
            "PX" => {
                if expiry_set || i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(ms) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if ms <= 0 {
                    return Ok(RespValue::err("ERR invalid expire time in 'set' command"));
                }
                expiry = SetExpiry::Millis(ms);
                expiry_set = true;
                i += 2;
            }
            "EXAT" => {
                if expiry_set || i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(ts_secs) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if ts_secs <= 0 {
                    return Ok(RespValue::err("ERR invalid expire time in 'set' command"));
                }
                let target_ms = ts_secs.saturating_mul(1000);
                expiry = SetExpiry::Millis(target_ms - now_unix_ms());
                expiry_set = true;
                i += 2;
            }
            "PXAT" => {
                if expiry_set || i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(ts_ms) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if ts_ms <= 0 {
                    return Ok(RespValue::err("ERR invalid expire time in 'set' command"));
                }
                expiry = SetExpiry::Millis(ts_ms - now_unix_ms());
                expiry_set = true;
                i += 2;
            }
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    let existing = if return_old || cond != SetCondition::Always {
        get_from_db(client, plans, None, &key)?
    } else {
        None
    };

    if cond == SetCondition::Nx && existing.is_some() {
        return Ok(if return_old {
            existing.map(RespValue::BulkString).unwrap_or(RespValue::Null)
        } else {
            RespValue::Null
        });
    }

    if cond == SetCondition::Xx && existing.is_none() {
        return Ok(RespValue::Null);
    }

    // Handle EXAT/PXAT already in the past: delete immediately
    if let SetExpiry::Millis(ms) = expiry {
        if ms <= 0 {
            client.update(
                "DELETE FROM pg_redis_strings WHERE key = $1",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
            )?;
            if let Some(c) = cache {
                c.invalidate(&key);
            }
            return Ok(if return_old {
                existing.map(RespValue::BulkString).unwrap_or(RespValue::Null)
            } else {
                RespValue::ok()
            });
        }
    }

    write_string(client, plans, cache, &key, &value, expiry)?;

    if return_old {
        Ok(existing.map(RespValue::BulkString).unwrap_or(RespValue::Null))
    } else {
        Ok(RespValue::ok())
    }
}

fn write_string(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    key: &str,
    value: &Bytes,
    expiry: SetExpiry,
) -> SpiResult<()> {
    let val_slice: &[u8] = value.as_ref();
    match expiry {
        SetExpiry::None => {
            match plans {
                Some(p) => {
                    client.update(
                        &p.string_set_no_ttl,
                        None,
                        Some(vec![key.into_datum(), val_slice.into_datum()]),
                    )?;
                }
                None => {
                    client.update(
                        SQL_STRING_SET_NO_TTL,
                        None,
                        Some(vec![
                            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                            (PgBuiltInOids::BYTEAOID.oid(), val_slice.into_datum()),
                        ]),
                    )?;
                }
            }
            if let Some(c) = cache {
                c.insert(key.to_string(), value.clone(), None);
            }
        }
        SetExpiry::Millis(ms) => {
            match plans {
                Some(p) => {
                    client.update(
                        &p.string_set_with_ms,
                        None,
                        Some(vec![key.into_datum(), val_slice.into_datum(), ms.into_datum()]),
                    )?;
                }
                None => {
                    client.update(
                        SQL_STRING_SET_WITH_MS,
                        None,
                        Some(vec![
                            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                            (PgBuiltInOids::BYTEAOID.oid(), val_slice.into_datum()),
                            (PgBuiltInOids::INT8OID.oid(), ms.into_datum()),
                        ]),
                    )?;
                }
            }
            if let Some(c) = cache {
                c.insert(
                    key.to_string(),
                    value.clone(),
                    Some(Duration::from_millis(ms as u64)),
                );
            }
        }
        SetExpiry::KeepTtl => {
            match plans {
                Some(p) => {
                    client.update(
                        &p.string_set_keep_ttl,
                        None,
                        Some(vec![key.into_datum(), val_slice.into_datum()]),
                    )?;
                }
                None => {
                    client.update(
                        SQL_STRING_SET_KEEP_TTL,
                        None,
                        Some(vec![
                            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                            (PgBuiltInOids::BYTEAOID.oid(), val_slice.into_datum()),
                        ]),
                    )?;
                }
            }
            if let Some(c) = cache {
                c.invalidate(key);
            }
        }
    }
    Ok(())
}

pub fn cmd_setex(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
    millis: bool,
) -> SpiResult<RespValue> {
    let name = if millis { "psetex" } else { "setex" };
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity(name));
    }
    let key = as_str(&args[1]);
    let Some(ttl_raw) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    if ttl_raw <= 0 {
        return Ok(RespValue::err(format!(
            "ERR invalid expire time in '{name}' command"
        )));
    }
    let ms = if millis {
        ttl_raw
    } else {
        match ttl_raw.checked_mul(1000) {
            Some(v) => v,
            None => {
                return Ok(RespValue::err(format!(
                    "ERR invalid expire time in '{name}' command"
                )))
            }
        }
    };
    write_string(client, plans, cache, &key, &args[3], SetExpiry::Millis(ms))?;
    Ok(RespValue::ok())
}

pub fn cmd_setnx(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("setnx"));
    }
    let key = as_str(&args[1]);
    if get_from_db(client, plans, None, &key)?.is_some() {
        return Ok(RespValue::Integer(0));
    }
    write_string(client, plans, cache, &key, &args[2], SetExpiry::None)?;
    Ok(RespValue::Integer(1))
}

pub fn cmd_getset(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("getset"));
    }
    let key = as_str(&args[1]);
    let old = get_from_db(client, plans, None, &key)?;
    write_string(client, plans, cache, &key, &args[2], SetExpiry::None)?;
    Ok(old.map(RespValue::BulkString).unwrap_or(RespValue::Null))
}

pub fn cmd_getdel(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("getdel"));
    }
    let key = as_str(&args[1]);
    let old = get_from_db(client, plans, None, &key)?;
    if old.is_some() {
        client.update(
            "DELETE FROM pg_redis_strings WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?;
    }
    if let Some(c) = cache {
        c.invalidate(&key);
    }
    Ok(old.map(RespValue::BulkString).unwrap_or(RespValue::Null))
}

pub fn cmd_mget(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 2 {
        return Ok(RespValue::wrong_arity("mget"));
    }
    let mut results = Vec::with_capacity(args.len() - 1);
    for arg in &args[1..] {
        let key = as_str(arg);
        if let Some(c) = cache {
            if let Some(val) = c.get(&key) {
                results.push(RespValue::BulkString(val));
                continue;
            }
        }
        match get_from_db(client, plans, cache, &key)? {
            Some(b) => results.push(RespValue::BulkString(b)),
            None => results.push(RespValue::Null),
        }
    }
    Ok(RespValue::Array(results))
}

pub fn cmd_mset(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Ok(RespValue::wrong_arity("mset"));
    }
    for pair in args[1..].chunks_exact(2) {
        let key = as_str(&pair[0]);
        write_string(client, plans, cache, &key, &pair[1], SetExpiry::None)?;
    }
    Ok(RespValue::ok())
}

pub fn cmd_incrby(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
    delta_override: Option<i64>,
    negate: bool,
) -> SpiResult<RespValue> {
    let delta = match delta_override {
        Some(d) => d,
        None => {
            if args.len() != 3 {
                return Ok(RespValue::wrong_arity(if negate { "decrby" } else { "incrby" }));
            }
            let Some(d) = parse_i64(&args[2]) else {
                return Ok(RespValue::not_integer_err());
            };
            if negate {
                match d.checked_neg() {
                    Some(v) => v,
                    None => return Ok(RespValue::not_integer_err()),
                }
            } else {
                d
            }
        }
    };

    if delta_override.is_some() && args.len() != 2 {
        return Ok(RespValue::wrong_arity(if delta < 0 { "decr" } else { "incr" }));
    }

    let key = as_str(&args[1]);
    let current = match get_from_db(client, plans, None, &key)? {
        Some(raw) => match parse_i64(&raw) {
            Some(n) => n,
            None => return Ok(RespValue::not_integer_err()),
        },
        None => 0,
    };

    let Some(next) = current.checked_add(delta) else {
        return Ok(RespValue::err("ERR increment or decrement would overflow"));
    };

    let next_bytes = Bytes::from(next.to_string());
    write_string(client, plans, cache, &key, &next_bytes, SetExpiry::KeepTtl)?;
    Ok(RespValue::Integer(next))
}

pub fn cmd_incrbyfloat(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("incrbyfloat"));
    }
    let Some(delta) = parse_f64(&args[2]) else {
        return Ok(RespValue::not_float_err());
    };
    let key = as_str(&args[1]);
    let current = match get_from_db(client, plans, None, &key)? {
        Some(raw) => match parse_f64(&raw) {
            Some(v) => v,
            None => return Ok(RespValue::not_float_err()),
        },
        None => 0.0,
    };
    let next = current + delta;
    if next.is_nan() || next.is_infinite() {
        return Ok(RespValue::err(
            "ERR increment would produce NaN or Infinity",
        ));
    }
    let formatted = format_score(next);
    let next_bytes = Bytes::from(formatted);
    write_string(client, plans, cache, &key, &next_bytes, SetExpiry::KeepTtl)?;
    Ok(RespValue::BulkString(next_bytes))
}

pub fn cmd_append(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("append"));
    }
    let key = as_str(&args[1]);
    let mut combined = match get_from_db(client, plans, None, &key)? {
        Some(existing) => existing.to_vec(),
        None => Vec::new(),
    };
    combined.extend_from_slice(&args[2]);
    let new_len = combined.len() as i64;
    write_string(
        client,
        plans,
        cache,
        &key,
        &Bytes::from(combined),
        SetExpiry::KeepTtl,
    )?;
    Ok(RespValue::Integer(new_len))
}

pub fn cmd_strlen(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("strlen"));
    }
    let key = as_str(&args[1]);
    if let Some(c) = cache {
        if let Some(val) = c.get(&key) {
            return Ok(RespValue::Integer(val.len() as i64));
        }
    }
    let len = get_from_db(client, plans, cache, &key)?
        .map(|b| b.len() as i64)
        .unwrap_or(0);
    Ok(RespValue::Integer(len))
}
