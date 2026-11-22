use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};

use crate::protocol::RespValue;

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

/// LPUSH prepends elements one by one to the head of the list (`seq = MIN(seq) - 1`).
/// RPUSH appends elements one by one to the tail of the list (`seq = MAX(seq) + 1`).
pub fn cmd_push(client: &mut SpiClient, args: &[Bytes], left: bool) -> SpiResult<RespValue> {
    let name = if left { "lpush" } else { "rpush" };
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    let insert_sql = if left {
        "INSERT INTO pg_redis_lists (key, seq, value) \
         SELECT $1, COALESCE(MIN(seq), 1) - 1, $2 FROM pg_redis_lists WHERE key = $1"
    } else {
        "INSERT INTO pg_redis_lists (key, seq, value) \
         SELECT $1, COALESCE(MAX(seq), 0) + 1, $2 FROM pg_redis_lists WHERE key = $1"
    };

    for raw_val in &args[2..] {
        let value = as_str(raw_val);
        client.update(
            insert_sql,
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
            ]),
        )?;
    }

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    Ok(RespValue::Integer(len))
}

pub fn cmd_pop(client: &mut SpiClient, args: &[Bytes], left: bool) -> SpiResult<RespValue> {
    let name = if left { "lpop" } else { "rpop" };
    if args.len() < 2 || args.len() > 3 {
        return Ok(RespValue::wrong_arity(name));
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

    let order = if left { "ASC" } else { "DESC" };
    let limit = count_arg.unwrap_or(1);

    if limit == 0 {
        return Ok(RespValue::Array(Vec::new()));
    }

    let sql = format!(
        "WITH popped AS ( \
            SELECT key, seq FROM pg_redis_lists \
            WHERE key = $1 \
            ORDER BY seq {order} \
            LIMIT $2 \
        ) \
        DELETE FROM pg_redis_lists l \
        USING popped p \
        WHERE l.key = p.key AND l.seq = p.seq \
        RETURNING l.seq, l.value"
    );

    let table = client.update(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), limit.into_datum()),
        ]),
    )?;

    let mut rows: Vec<(i64, String)> = Vec::with_capacity(table.len());
    for row in table {
        let seq: i64 = row.get(1)?.unwrap_or(0);
        let val: String = row.get(2)?.unwrap_or_default();
        rows.push((seq, val));
    }

    if rows.is_empty() {
        return Ok(RespValue::Null);
    }

    if left {
        rows.sort_by_key(|(seq, _)| *seq);
    } else {
        rows.sort_by_key(|(seq, _)| std::cmp::Reverse(*seq));
    }

    match count_arg {
        None => Ok(RespValue::bulk(rows.into_iter().next().unwrap().1)),
        Some(_) => Ok(RespValue::Array(
            rows.into_iter().map(|(_, v)| RespValue::bulk(v)).collect(),
        )),
    }
}

pub fn cmd_lrange(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("lrange"));
    }
    let key = as_str(&args[1]);
    let Some(mut start) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let Some(mut stop) = parse_i64(&args[3]) else {
        return Ok(RespValue::not_integer_err());
    };

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if len == 0 {
        return Ok(RespValue::Array(Vec::new()));
    }

    if start < 0 {
        start = (len + start).max(0);
    }
    if stop < 0 {
        stop += len;
    }
    if start > stop || start >= len || stop < 0 {
        return Ok(RespValue::Array(Vec::new()));
    }
    if stop >= len {
        stop = len - 1;
    }

    let count = stop - start + 1;
    let table = client.select(
        "SELECT value FROM pg_redis_lists \
         WHERE key = $1 \
         ORDER BY seq ASC \
         OFFSET $2 LIMIT $3",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), start.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), count.into_datum()),
        ]),
    )?;

    let mut out = Vec::with_capacity(table.len());
    for row in table {
        let val: String = row.get(1)?.unwrap_or_default();
        out.push(RespValue::bulk(val));
    }
    Ok(RespValue::Array(out))
}

pub fn cmd_llen(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("llen"));
    }
    let key = as_str(&args[1]);
    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    Ok(RespValue::Integer(len))
}

pub fn cmd_lindex(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("lindex"));
    }
    let key = as_str(&args[1]);
    let Some(mut idx) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if idx < 0 {
        idx += len;
    }
    if idx < 0 || idx >= len {
        return Ok(RespValue::Null);
    }

    let table = client.select(
        "SELECT value FROM pg_redis_lists WHERE key = $1 ORDER BY seq ASC OFFSET $2 LIMIT 1",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), idx.into_datum()),
        ]),
    )?;
    if table.is_empty() {
        Ok(RespValue::Null)
    } else {
        match table.first().get_one::<String>()? {
            Some(v) => Ok(RespValue::bulk(v)),
            None => Ok(RespValue::Null),
        }
    }
}

pub fn cmd_lset(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("lset"));
    }
    let key = as_str(&args[1]);
    let Some(mut idx) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let value = as_str(&args[3]);

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if len == 0 {
        return Ok(RespValue::err("ERR no such key"));
    }
    if idx < 0 {
        idx += len;
    }
    if idx < 0 || idx >= len {
        return Ok(RespValue::err("ERR index out of range"));
    }

    client.update(
        "UPDATE pg_redis_lists \
         SET value = $3 \
         WHERE (key, seq) = ( \
             SELECT key, seq FROM pg_redis_lists \
             WHERE key = $1 ORDER BY seq ASC OFFSET $2 LIMIT 1 \
         )",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), idx.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
        ]),
    )?;

    Ok(RespValue::ok())
}

pub fn cmd_lrem(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("lrem"));
    }
    let key = as_str(&args[1]);
    let Some(count) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let element = as_str(&args[3]);

    let res = if count == 0 {
        client.update(
            "DELETE FROM pg_redis_lists WHERE key = $1 AND value = $2",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), element.into_datum()),
            ]),
        )?
    } else {
        let order = if count > 0 { "ASC" } else { "DESC" };
        let limit = count.unsigned_abs() as i64;
        let sql = format!(
            "WITH target AS ( \
                SELECT key, seq FROM pg_redis_lists \
                WHERE key = $1 AND value = $2 \
                ORDER BY seq {order} LIMIT $3 \
             ) \
             DELETE FROM pg_redis_lists l USING target t \
             WHERE l.key = t.key AND l.seq = t.seq"
        );
        client.update(
            &sql,
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), element.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), limit.into_datum()),
            ]),
        )?
    };

    Ok(RespValue::Integer(res.len() as i64))
}
