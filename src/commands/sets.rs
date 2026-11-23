use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};

use crate::commands::keys;
use crate::protocol::RespValue;

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

pub fn cmd_sadd(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("sadd"));
    }
    let key = as_str(&args[1]);
    keys::purge_if_expired_non_string(client, &key)?;
    let mut added: i64 = 0;

    for raw_member in &args[2..] {
        let member = as_str(raw_member);
        let res = client.update(
            "INSERT INTO pg_redis_sets (key, member) VALUES ($1, $2) ON CONFLICT (key, member) DO NOTHING",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            ]),
        )?;
        added += res.len() as i64;
    }

    Ok(RespValue::Integer(added))
}

pub fn cmd_srem(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("srem"));
    }
    let key = as_str(&args[1]);
    if keys::purge_if_expired_non_string(client, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let mut removed: i64 = 0;

    for raw_member in &args[2..] {
        let member = as_str(raw_member);
        let res = client.update(
            "DELETE FROM pg_redis_sets WHERE key = $1 AND member = $2",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            ]),
        )?;
        removed += res.len() as i64;
    }

    if removed > 0 {
        keys::cleanup_expiry_if_empty(client, "pg_redis_sets", &key)?;
    }

    Ok(RespValue::Integer(removed))
}

pub fn cmd_smembers(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("smembers"));
    }
    let key = as_str(&args[1]);
    if keys::is_non_string_expired(client, &key)? {
        return Ok(RespValue::Array(Vec::new()));
    }
    let table = client.select(
        "SELECT member FROM pg_redis_sets WHERE key = $1 ORDER BY member ASC",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;

    let mut out = Vec::with_capacity(table.len());
    for row in table {
        if let Some(m) = row.get::<String>(1)? {
            out.push(RespValue::bulk(m));
        }
    }
    Ok(RespValue::Array(out))
}

pub fn cmd_sismember(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("sismember"));
    }
    let key = as_str(&args[1]);
    let member = as_str(&args[2]);
    if keys::is_non_string_expired(client, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let exists = client
        .select(
            "SELECT EXISTS(SELECT 1 FROM pg_redis_sets WHERE key = $1 AND member = $2)",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            ]),
        )?
        .first()
        .get_one::<bool>()?
        .unwrap_or(false);

    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

pub fn cmd_scard(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("scard"));
    }
    let key = as_str(&args[1]);
    if keys::is_non_string_expired(client, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let count = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_sets WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    Ok(RespValue::Integer(count))
}

pub fn cmd_spop(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 2 || args.len() > 3 {
        return Ok(RespValue::wrong_arity("spop"));
    }
    let key = as_str(&args[1]);
    let count_arg = if args.len() == 3 {
        let Some(c) = parse_i64(&args[2]) else {
            return Ok(RespValue::not_integer_err());
        };
        if c < 0 {
            return Ok(RespValue::err("ERR value is out of range, must be positive"));
        }
        Some(c)
    } else {
        None
    };

    if keys::purge_if_expired_non_string(client, &key)? {
        return Ok(match count_arg {
            None => RespValue::Null,
            Some(_) => RespValue::Array(Vec::new()),
        });
    }

    let limit = count_arg.unwrap_or(1);
    if limit == 0 {
        return Ok(RespValue::Array(Vec::new()));
    }

    let table = client.update(
        "WITH popped AS ( \
            SELECT key, member FROM pg_redis_sets \
            WHERE key = $1 ORDER BY member ASC LIMIT $2 \
        ) \
        DELETE FROM pg_redis_sets s USING popped p \
        WHERE s.key = p.key AND s.member = p.member \
        RETURNING s.member",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            (PgBuiltInOids::INT8OID.oid(), limit.into_datum()),
        ]),
    )?;

    let mut members = Vec::with_capacity(table.len());
    for row in table {
        if let Some(m) = row.get::<String>(1)? {
            members.push(m);
        }
    }

    if members.is_empty() {
        return Ok(match count_arg {
            None => RespValue::Null,
            Some(_) => RespValue::Array(Vec::new()),
        });
    }

    keys::cleanup_expiry_if_empty(client, "pg_redis_sets", &key)?;

    match count_arg {
        None => Ok(RespValue::bulk(members.into_iter().next().unwrap())),
        Some(_) => Ok(RespValue::Array(
            members.into_iter().map(RespValue::bulk).collect(),
        )),
    }
}

/// `SSCAN key cursor [MATCH pattern] [COUNT count]`
pub fn cmd_sscan(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("sscan"));
    }
    let key = as_str(&args[1]);
    let Some(cursor) = parse_i64(&args[2]) else {
        return Ok(RespValue::err("ERR invalid cursor"));
    };
    if cursor < 0 {
        return Ok(RespValue::err("ERR invalid cursor"));
    }

    let mut pattern: Option<String> = None;
    let mut count: usize = 10;

    let mut i = 3;
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
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    if keys::is_non_string_expired(client, &key)? {
        return Ok(RespValue::Array(vec![
            RespValue::bulk("0"),
            RespValue::Array(Vec::new()),
        ]));
    }

    let table = client.select(
        "SELECT member FROM pg_redis_sets WHERE key = $1 ORDER BY member ASC",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;

    let mut members: Vec<String> = Vec::with_capacity(table.len());
    for row in table {
        if let Some(m) = row.get::<String>(1)? {
            members.push(m);
        }
    }

    let start = cursor as usize;
    let end = (start + count).min(members.len());
    let next_cursor = if end >= members.len() { 0 } else { end };

    let mut batch = Vec::new();
    if start < members.len() {
        for m in &members[start..end] {
            if let Some(ref pat) = pattern {
                if !keys::glob_match(pat, m) {
                    continue;
                }
            }
            batch.push(RespValue::bulk(m.clone()));
        }
    }

    Ok(RespValue::Array(vec![
        RespValue::bulk(next_cursor.to_string()),
        RespValue::Array(batch),
    ]))
}
