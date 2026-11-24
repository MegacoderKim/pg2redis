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

/// LPUSH prepends elements one by one to the head of the list (`seq = MIN(seq) - 1`).
/// RPUSH appends elements one by one to the tail of the list (`seq = MAX(seq) + 1`).
pub fn cmd_push(
    client: &mut SpiClient,
    db: i16,
    args: &[Bytes],
    left: bool,
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    let name = if left { "lpush" } else { "rpush" };
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    keys::purge_if_expired_non_string(client, db, &key)?;
    let insert_sql = if left {
        "INSERT INTO pg_redis_lists (db, key, seq, value) \
         SELECT $1, $2, COALESCE(MIN(seq), 1) - 1, $3 FROM pg_redis_lists WHERE db = $1 AND key = $2"
    } else {
        "INSERT INTO pg_redis_lists (db, key, seq, value) \
         SELECT $1, $2, COALESCE(MAX(seq), 0) + 1, $3 FROM pg_redis_lists WHERE db = $1 AND key = $2"
    };

    for raw_val in &args[2..] {
        let value = as_str(raw_val);
        client.update(
            insert_sql,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
            ]),
        )?;
    }

    mutated_keys.push((db, key.clone()));

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    Ok(RespValue::Integer(len))
}

pub fn cmd_pop(
    client: &mut SpiClient,
    db: i16,
    args: &[Bytes],
    left: bool,
) -> SpiResult<RespValue> {
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

    if keys::purge_if_expired_non_string(client, db, &key)? {
        return Ok(RespValue::Null);
    }

    if limit == 0 {
        return Ok(RespValue::Array(Vec::new()));
    }

    let sql = format!(
        "WITH popped AS ( \
            SELECT db, key, seq FROM pg_redis_lists \
            WHERE db = $1 AND key = $2 \
            ORDER BY seq {order} \
            LIMIT $3 \
        ) \
        DELETE FROM pg_redis_lists l \
        USING popped p \
        WHERE l.db = p.db AND l.key = p.key AND l.seq = p.seq \
        RETURNING l.seq, l.value"
    );

    let table = client.update(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
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

    keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, &key)?;

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

pub fn cmd_lrange(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
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
    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(RespValue::Array(Vec::new()));
    }

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
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
         WHERE db = $1 AND key = $2 \
         ORDER BY seq ASC \
         OFFSET $3 LIMIT $4",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
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

pub fn cmd_llen(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("llen"));
    }
    let key = as_str(&args[1]);
    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    Ok(RespValue::Integer(len))
}

pub fn cmd_lindex(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("lindex"));
    }
    let key = as_str(&args[1]);
    let Some(mut idx) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(RespValue::Null);
    }

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
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
        "SELECT value FROM pg_redis_lists WHERE db = $1 AND key = $2 ORDER BY seq ASC OFFSET $3 LIMIT 1",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
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

pub fn cmd_lset(client: &mut SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("lset"));
    }
    let key = as_str(&args[1]);
    let Some(mut idx) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let value = as_str(&args[3]);
    keys::purge_if_expired_non_string(client, db, &key)?;

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
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
         SET value = $4 \
         WHERE (db, key, seq) = ( \
             SELECT db, key, seq FROM pg_redis_lists \
             WHERE db = $1 AND key = $2 ORDER BY seq ASC OFFSET $3 LIMIT 1 \
         )",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), idx.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), value.into_datum()),
        ]),
    )?;

    Ok(RespValue::ok())
}

pub fn cmd_lrem(client: &mut SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("lrem"));
    }
    let key = as_str(&args[1]);
    let Some(count) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let element = as_str(&args[3]);
    if keys::purge_if_expired_non_string(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }

    let res = if count == 0 {
        client.update(
            "DELETE FROM pg_redis_lists WHERE db = $1 AND key = $2 AND value = $3",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), element.into_datum()),
            ]),
        )?
    } else {
        let order = if count > 0 { "ASC" } else { "DESC" };
        let limit = count.unsigned_abs() as i64;
        let sql = format!(
            "WITH target AS ( \
                SELECT db, key, seq FROM pg_redis_lists \
                WHERE db = $1 AND key = $2 AND value = $3 \
                ORDER BY seq {order} LIMIT $4 \
             ) \
             DELETE FROM pg_redis_lists l USING target t \
             WHERE l.db = t.db AND l.key = t.key AND l.seq = t.seq"
        );
        client.update(
            &sql,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), element.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), limit.into_datum()),
            ]),
        )?
    };

    let removed = res.len() as i64;
    if removed > 0 {
        keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, &key)?;
    }

    Ok(RespValue::Integer(removed))
}

/// `LTRIM key start stop`
pub fn cmd_ltrim(client: &mut SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("ltrim"));
    }
    let key = as_str(&args[1]);
    let Some(mut start) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let Some(mut stop) = parse_i64(&args[3]) else {
        return Ok(RespValue::not_integer_err());
    };
    if keys::purge_if_expired_non_string(client, db, &key)? {
        return Ok(RespValue::ok());
    }

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if len == 0 {
        return Ok(RespValue::ok());
    }

    if start < 0 {
        start = (len + start).max(0);
    }
    if stop < 0 {
        stop += len;
    }

    if start > stop || start >= len || stop < 0 {
        client.update(
            "DELETE FROM pg_redis_lists WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
        )?;
        keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, &key)?;
        return Ok(RespValue::ok());
    }

    if stop >= len {
        stop = len - 1;
    }

    let keep_count = stop - start + 1;
    client.update(
        "WITH kept AS ( \
            SELECT db, key, seq FROM pg_redis_lists \
            WHERE db = $1 AND key = $2 ORDER BY seq ASC \
            OFFSET $3 LIMIT $4 \
        ) \
        DELETE FROM pg_redis_lists l \
        WHERE l.db = $1 AND l.key = $2 \
          AND NOT EXISTS (SELECT 1 FROM kept k WHERE k.db = l.db AND k.key = l.key AND k.seq = l.seq)",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            (PgBuiltInOids::INT8OID.oid(), start.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), keep_count.into_datum()),
        ]),
    )?;
    keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, &key)?;

    Ok(RespValue::ok())
}

/// `LPOS key element [RANK rank] [COUNT num-matches] [MAXLEN len]`
pub fn cmd_lpos(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("lpos"));
    }
    let key = as_str(&args[1]);
    let target = as_str(&args[2]);

    let mut rank: i64 = 1;
    let mut count_opt: Option<i64> = None;
    let mut maxlen: i64 = 0;

    let mut i = 3;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "RANK" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(r) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if r == 0 {
                    return Ok(RespValue::err(
                        "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list",
                    ));
                }
                rank = r;
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(c) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if c < 0 {
                    return Ok(RespValue::err("ERR COUNT can't be negative"));
                }
                count_opt = Some(c);
                i += 2;
            }
            "MAXLEN" => {
                if i + 1 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(m) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                if m < 0 {
                    return Ok(RespValue::err("ERR MAXLEN can't be negative"));
                }
                maxlen = m;
                i += 2;
            }
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(match count_opt {
            None => RespValue::Null,
            Some(_) => RespValue::Array(Vec::new()),
        });
    }

    let table = client.select(
        "SELECT value FROM pg_redis_lists WHERE db = $1 AND key = $2 ORDER BY seq ASC",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;

    let mut items = Vec::with_capacity(table.len());
    for row in table {
        items.push(row.get::<String>(1)?.unwrap_or_default());
    }

    let len = items.len();
    let max_matches = match count_opt {
        None => 1usize,
        Some(0) => usize::MAX,
        Some(c) => c as usize,
    };

    let mut skipped = 0i64;
    let skip_needed = rank.abs() - 1;
    let mut comparisons = 0i64;
    let mut matches: Vec<i64> = Vec::new();

    if rank > 0 {
        for (idx, val) in items.iter().enumerate() {
            if maxlen > 0 && comparisons >= maxlen {
                break;
            }
            comparisons += 1;
            if val == &target {
                if skipped < skip_needed {
                    skipped += 1;
                } else {
                    matches.push(idx as i64);
                    if matches.len() >= max_matches {
                        break;
                    }
                }
            }
        }
    } else {
        for rev_offset in 0..len {
            if maxlen > 0 && comparisons >= maxlen {
                break;
            }
            comparisons += 1;
            let idx = len - 1 - rev_offset;
            if items[idx] == target {
                if skipped < skip_needed {
                    skipped += 1;
                } else {
                    matches.push(idx as i64);
                    if matches.len() >= max_matches {
                        break;
                    }
                }
            }
        }
    }

    match count_opt {
        None => Ok(matches
            .into_iter()
            .next()
            .map(RespValue::Integer)
            .unwrap_or(RespValue::Null)),
        Some(_) => Ok(RespValue::Array(
            matches.into_iter().map(RespValue::Integer).collect(),
        )),
    }
}

/// `RPOPLPUSH source destination` / `LMOVE source destination LEFT|RIGHT LEFT|RIGHT`
pub fn cmd_lmove(
    client: &mut SpiClient,
    db: i16,
    src_key: &str,
    dst_key: &str,
    pop_left: bool,
    push_left: bool,
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if keys::purge_if_expired_non_string(client, db, src_key)? {
        return Ok(RespValue::Null);
    }
    keys::purge_if_expired_non_string(client, db, dst_key)?;

    let order = if pop_left { "ASC" } else { "DESC" };
    let pop_sql = format!(
        "WITH popped AS ( \
            SELECT db, key, seq FROM pg_redis_lists \
            WHERE db = $1 AND key = $2 ORDER BY seq {order} LIMIT 1 \
        ) \
        DELETE FROM pg_redis_lists l USING popped p \
        WHERE l.db = p.db AND l.key = p.key AND l.seq = p.seq \
        RETURNING l.value"
    );

    let table = client.update(
        &pop_sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), src_key.into_datum()),
        ]),
    )?;

    if table.is_empty() {
        return Ok(RespValue::Null);
    }

    let val = table.first().get_one::<String>()?.unwrap_or_default();
    if src_key != dst_key {
        keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, src_key)?;
    }

    let push_sql = if push_left {
        "INSERT INTO pg_redis_lists (db, key, seq, value) \
         SELECT $1, $2, COALESCE(MIN(seq), 1) - 1, $3 FROM pg_redis_lists WHERE db = $1 AND key = $2"
    } else {
        "INSERT INTO pg_redis_lists (db, key, seq, value) \
         SELECT $1, $2, COALESCE(MAX(seq), 0) + 1, $3 FROM pg_redis_lists WHERE db = $1 AND key = $2"
    };

    client.update(
        push_sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), dst_key.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), val.clone().into_datum()),
        ]),
    )?;

    mutated_keys.push((db, dst_key.to_string()));
    Ok(RespValue::bulk(val))
}

pub fn cmd_rpoplpush(
    client: &mut SpiClient,
    db: i16,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("rpoplpush"));
    }
    let src = as_str(&args[1]);
    let dst = as_str(&args[2]);
    cmd_lmove(client, db, &src, &dst, false, true, mutated_keys)
}

pub fn cmd_lmove_cmd(
    client: &mut SpiClient,
    db: i16,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.len() != 5 {
        return Ok(RespValue::wrong_arity("lmove"));
    }
    let src = as_str(&args[1]);
    let dst = as_str(&args[2]);
    let from = as_str(&args[3]).to_uppercase();
    let to = as_str(&args[4]).to_uppercase();
    let pop_left = match from.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Ok(RespValue::syntax_err()),
    };
    let push_left = match to.as_str() {
        "LEFT" => true,
        "RIGHT" => false,
        _ => return Ok(RespValue::syntax_err()),
    };
    cmd_lmove(client, db, &src, &dst, pop_left, push_left, mutated_keys)
}

/// Non-blocking single pop across multiple list keys in `db` used by `BLPOP` / `BRPOP`.
pub fn try_blpop_once(
    client: &mut SpiClient,
    db: i16,
    keys: &[String],
    is_right: bool,
) -> SpiResult<Option<RespValue>> {
    let order = if is_right { "DESC" } else { "ASC" };
    let sql = format!(
        "WITH popped AS ( \
            SELECT db, key, seq FROM pg_redis_lists \
            WHERE db = $1 AND key = $2 ORDER BY seq {order} LIMIT 1 \
        ) \
        DELETE FROM pg_redis_lists l USING popped p \
        WHERE l.db = p.db AND l.key = p.key AND l.seq = p.seq \
        RETURNING l.value"
    );

    for key in keys {
        if keys::purge_if_expired_non_string(client, db, key)? {
            continue;
        }
        let table = client.update(
            &sql,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            ]),
        )?;
        if !table.is_empty() {
            let val = table.first().get_one::<String>()?.unwrap_or_default();
            keys::cleanup_expiry_if_empty(client, "pg_redis_lists", db, key)?;
            return Ok(Some(RespValue::Array(vec![
                RespValue::bulk(key.clone()),
                RespValue::bulk(val),
            ])));
        }
    }

    Ok(None)
}
