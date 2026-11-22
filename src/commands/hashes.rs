use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};

use crate::protocol::{format_score, RespValue};
use crate::storage::{PreparedPlans, SQL_HASH_GET, SQL_HASH_GETALL, SQL_HASH_UPSERT};

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

pub fn cmd_hset(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
    hmset: bool,
) -> SpiResult<RespValue> {
    let name = if hmset { "hmset" } else { "hset" };
    if args.len() < 4 || (args.len() - 2) % 2 != 0 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    let mut added: i64 = 0;

    for pair in args[2..].chunks_exact(2) {
        let field = as_str(&pair[0]);
        let value = as_str(&pair[1]);

        let table = match plans {
            Some(p) => client.update(
                &p.hash_upsert,
                None,
                Some(vec![key.clone().into_datum(), field.into_datum(), value.into_datum()]),
            )?,
            None => client.update(
                SQL_HASH_UPSERT,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
                ]),
            )?,
        };

        let is_new = table.first().get_one::<bool>()?.unwrap_or(false);
        if is_new {
            added += 1;
        }
    }

    if hmset {
        Ok(RespValue::ok())
    } else {
        Ok(RespValue::Integer(added))
    }
}

pub fn cmd_hsetnx(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("hsetnx"));
    }
    let key = as_str(&args[1]);
    let field = as_str(&args[2]);
    let value = as_str(&args[3]);
    let res = client.update(
        "INSERT INTO pg_redis_hashes (key, field, value) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (key, field) DO NOTHING",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
        ]),
    )?;
    Ok(RespValue::Integer(if !res.is_empty() { 1 } else { 0 }))
}

pub fn cmd_hget(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("hget"));
    }
    let key = as_str(&args[1]);
    let field = as_str(&args[2]);

    let table = match plans {
        Some(p) => client.select(
            &p.hash_get,
            None,
            Some(vec![key.into_datum(), field.into_datum()]),
        )?,
        None => client.select(
            SQL_HASH_GET,
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
            ]),
        )?,
    };

    if table.is_empty() {
        return Ok(RespValue::Null);
    }
    match table.first().get_one::<String>()? {
        Some(v) => Ok(RespValue::bulk(v)),
        None => Ok(RespValue::Null),
    }
}

pub fn cmd_hmget(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("hmget"));
    }
    let key = as_str(&args[1]);
    let mut out = Vec::with_capacity(args.len() - 2);

    for raw_field in &args[2..] {
        let field = as_str(raw_field);
        let table = match plans {
            Some(p) => client.select(
                &p.hash_get,
                None,
                Some(vec![key.clone().into_datum(), field.into_datum()]),
            )?,
            None => client.select(
                SQL_HASH_GET,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
                ]),
            )?,
        };
        if table.is_empty() {
            out.push(RespValue::Null);
        } else {
            match table.first().get_one::<String>()? {
                Some(v) => out.push(RespValue::bulk(v)),
                None => out.push(RespValue::Null),
            }
        }
    }

    Ok(RespValue::Array(out))
}

pub fn cmd_hgetall(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("hgetall"));
    }
    let key = as_str(&args[1]);
    let table = match plans {
        Some(p) => client.select(&p.hash_getall, None, Some(vec![key.into_datum()]))?,
        None => client.select(
            SQL_HASH_GETALL,
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?,
    };

    let mut pairs = Vec::with_capacity(table.len());
    for row in table {
        let field: String = row.get(1)?.unwrap_or_default();
        let value: String = row.get(2)?.unwrap_or_default();
        pairs.push((RespValue::bulk(field), RespValue::bulk(value)));
    }
    Ok(RespValue::Map(pairs))
}

pub fn cmd_hdel(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("hdel"));
    }
    let key = as_str(&args[1]);
    let mut deleted: i64 = 0;

    for raw_field in &args[2..] {
        let field = as_str(raw_field);
        let res = client.update(
            "DELETE FROM pg_redis_hashes WHERE key = $1 AND field = $2",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
            ]),
        )?;
        deleted += res.len() as i64;
    }

    Ok(RespValue::Integer(deleted))
}

pub fn cmd_hexists(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("hexists"));
    }
    let key = as_str(&args[1]);
    let field = as_str(&args[2]);
    let exists = client
        .select(
            "SELECT EXISTS(SELECT 1 FROM pg_redis_hashes WHERE key = $1 AND field = $2)",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
            ]),
        )?
        .first()
        .get_one::<bool>()?
        .unwrap_or(false);
    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

pub fn cmd_hlen(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("hlen"));
    }
    let key = as_str(&args[1]);
    let count = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_hashes WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    Ok(RespValue::Integer(count))
}

pub fn cmd_hkeys(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("hkeys"));
    }
    let key = as_str(&args[1]);
    let table = client.select(
        "SELECT field FROM pg_redis_hashes WHERE key = $1 ORDER BY field ASC",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    let mut keys = Vec::with_capacity(table.len());
    for row in table {
        if let Some(f) = row.get::<String>(1)? {
            keys.push(RespValue::bulk(f));
        }
    }
    Ok(RespValue::Array(keys))
}

pub fn cmd_hvals(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("hvals"));
    }
    let key = as_str(&args[1]);
    let table = client.select(
        "SELECT value FROM pg_redis_hashes WHERE key = $1 ORDER BY field ASC",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;
    let mut vals = Vec::with_capacity(table.len());
    for row in table {
        if let Some(v) = row.get::<String>(1)? {
            vals.push(RespValue::bulk(v));
        }
    }
    Ok(RespValue::Array(vals))
}

pub fn cmd_hincrby(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("hincrby"));
    }
    let key = as_str(&args[1]);
    let field = as_str(&args[2]);
    let Some(delta) = parse_i64(&args[3]) else {
        return Ok(RespValue::not_integer_err());
    };

    let current = {
        let table = match plans {
            Some(p) => client.select(
                &p.hash_get,
                None,
                Some(vec![key.clone().into_datum(), field.clone().into_datum()]),
            )?,
            None => client.select(
                SQL_HASH_GET,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.clone().into_datum()),
                ]),
            )?,
        };
        if table.is_empty() {
            0i64
        } else {
            let raw = table.first().get_one::<String>()?.unwrap_or_default();
            match raw.parse::<i64>() {
                Ok(n) => n,
                Err(_) => return Ok(RespValue::err("ERR hash value is not an integer")),
            }
        }
    };

    let Some(next) = current.checked_add(delta) else {
        return Ok(RespValue::err("ERR increment or decrement would overflow"));
    };

    let next_str = next.to_string();
    match plans {
        Some(p) => {
            client.update(
                &p.hash_upsert,
                None,
                Some(vec![key.into_datum(), field.into_datum(), next_str.into_datum()]),
            )?;
        }
        None => {
            client.update(
                SQL_HASH_UPSERT,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), next_str.into_datum()),
                ]),
            )?;
        }
    }

    Ok(RespValue::Integer(next))
}

pub fn cmd_hincrbyfloat(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("hincrbyfloat"));
    }
    let key = as_str(&args[1]);
    let field = as_str(&args[2]);
    let Some(delta) = parse_f64(&args[3]) else {
        return Ok(RespValue::not_float_err());
    };

    let current = {
        let table = match plans {
            Some(p) => client.select(
                &p.hash_get,
                None,
                Some(vec![key.clone().into_datum(), field.clone().into_datum()]),
            )?,
            None => client.select(
                SQL_HASH_GET,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.clone().into_datum()),
                ]),
            )?,
        };
        if table.is_empty() {
            0.0f64
        } else {
            let raw = table.first().get_one::<String>()?.unwrap_or_default();
            match raw.parse::<f64>() {
                Ok(v) if !v.is_nan() && !v.is_infinite() => v,
                _ => return Ok(RespValue::err("ERR hash value is not a float")),
            }
        }
    };

    let next = current + delta;
    if next.is_nan() || next.is_infinite() {
        return Ok(RespValue::err(
            "ERR increment would produce NaN or Infinity",
        ));
    }

    let formatted = format_score(next);
    match plans {
        Some(p) => {
            client.update(
                &p.hash_upsert,
                None,
                Some(vec![
                    key.into_datum(),
                    field.into_datum(),
                    formatted.clone().into_datum(),
                ]),
            )?;
        }
        None => {
            client.update(
                SQL_HASH_UPSERT,
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), formatted.clone().into_datum()),
                ]),
            )?;
        }
    }

    Ok(RespValue::bulk(formatted))
}
