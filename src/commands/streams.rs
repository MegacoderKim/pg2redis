use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::keys;
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

#[derive(Debug, Clone, Copy)]
pub enum StreamTrimStrategy {
    MaxLen(i64),
    MinId(i64, i64),
}

#[derive(Debug, Clone)]
pub struct XReadRequest {
    pub count: Option<i64>,
    pub block_ms: Option<u64>,
    pub streams: Vec<(String, String)>,
}

fn parse_stream_id_exact(s: &str) -> Option<(i64, i64)> {
    if let Some((ms_s, seq_s)) = s.split_once('-') {
        let ms = ms_s.parse::<i64>().ok()?;
        let seq = seq_s.parse::<i64>().ok()?;
        if ms < 0 || seq < 0 {
            return None;
        }
        Some((ms, seq))
    } else {
        let ms = s.parse::<i64>().ok()?;
        if ms < 0 {
            return None;
        }
        Some((ms, 0))
    }
}

fn parse_range_bound(s: &str, is_end: bool) -> Option<(i64, i64, bool)> {
    if s == "-" {
        return Some((0, 0, true));
    }
    if s == "+" {
        return Some((i64::MAX, i64::MAX, true));
    }
    let (exclusive, rest) = if let Some(r) = s.strip_prefix('(') {
        (true, r)
    } else {
        (false, s)
    };
    if let Some((ms_s, seq_s)) = rest.split_once('-') {
        let ms = ms_s.parse::<i64>().ok()?;
        let seq = seq_s.parse::<i64>().ok()?;
        Some((ms, seq, !exclusive))
    } else {
        let ms = rest.parse::<i64>().ok()?;
        let seq = if is_end && !exclusive { i64::MAX } else { 0 };
        Some((ms, seq, !exclusive))
    }
}

fn get_last_stream_id(client: &SpiClient, db: i16, key: &str) -> SpiResult<Option<(i64, i64)>> {
    let table = client.select(
        "SELECT ms, seq FROM pg_redis_streams WHERE db = $1 AND key = $2 ORDER BY ms DESC, seq DESC LIMIT 1",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
        ]),
    )?;
    if table.is_empty() {
        return Ok(None);
    }
    let row = table.first();
    let ms: i64 = row.get(1)?.unwrap_or(0);
    let seq: i64 = row.get(2)?.unwrap_or(0);
    Ok(Some((ms, seq)))
}

fn encode_entry_fields(pairs: &[Bytes]) -> String {
    let vec_pairs: Vec<[String; 2]> = pairs
        .chunks_exact(2)
        .map(|chunk| [as_str(&chunk[0]), as_str(&chunk[1])])
        .collect();
    serde_json::to_string(&vec_pairs).unwrap_or_else(|_| "[]".to_string())
}

fn decode_entry_fields(json_str: &str) -> Vec<RespValue> {
    let parsed: Vec<[String; 2]> = serde_json::from_str(json_str).unwrap_or_default();
    let mut out = Vec::with_capacity(parsed.len() * 2);
    for [k, v] in parsed {
        out.push(RespValue::bulk(k));
        out.push(RespValue::bulk(v));
    }
    out
}

fn apply_trim(
    client: &mut SpiClient,
    db: i16,
    key: &str,
    strategy: StreamTrimStrategy,
) -> SpiResult<i64> {
    match strategy {
        StreamTrimStrategy::MaxLen(max_len) => {
            let keep = max_len.max(0);
            let res = client.update(
                "WITH to_delete AS ( \
                    SELECT db, key, ms, seq FROM pg_redis_streams \
                    WHERE db = $1 AND key = $2 \
                    ORDER BY ms DESC, seq DESC \
                    OFFSET $3 \
                ) \
                DELETE FROM pg_redis_streams s USING to_delete d \
                WHERE s.db = d.db AND s.key = d.key AND s.ms = d.ms AND s.seq = d.seq",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), keep.into_datum()),
                ]),
            )?;
            Ok(res.len() as i64)
        }
        StreamTrimStrategy::MinId(min_ms, min_seq) => {
            let res = client.update(
                "DELETE FROM pg_redis_streams \
                 WHERE db = $1 AND key = $2 AND (ms < $3 OR (ms = $3 AND seq < $4))",
                None,
                Some(vec![
                    (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), min_ms.into_datum()),
                    (PgBuiltInOids::INT8OID.oid(), min_seq.into_datum()),
                ]),
            )?;
            Ok(res.len() as i64)
        }
    }
}

/// `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] (* | id) field value [field value ...]`
pub fn cmd_xadd(
    client: &mut SpiClient,
    db: i16,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.len() < 5 {
        return Ok(RespValue::wrong_arity("xadd"));
    }

    let key = as_str(&args[1]);
    let mut i = 2;
    let mut nomkstream = false;
    let mut trim: Option<StreamTrimStrategy> = None;

    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                i += 1;
            }
            "MAXLEN" => {
                i += 1;
                if i < args.len() && (args[i].as_ref() == b"~" || args[i].as_ref() == b"=") {
                    i += 1;
                }
                if i >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(max_len) = parse_i64(&args[i]) else {
                    return Ok(RespValue::not_integer_err());
                };
                trim = Some(StreamTrimStrategy::MaxLen(max_len));
                i += 1;
                if i + 1 < args.len() && as_str(&args[i]).eq_ignore_ascii_case("LIMIT") {
                    i += 2;
                }
            }
            "MINID" => {
                i += 1;
                if i < args.len() && (args[i].as_ref() == b"~" || args[i].as_ref() == b"=") {
                    i += 1;
                }
                if i >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let id_str = as_str(&args[i]);
                let Some((min_ms, min_seq)) = parse_stream_id_exact(&id_str) else {
                    return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
                };
                trim = Some(StreamTrimStrategy::MinId(min_ms, min_seq));
                i += 1;
                if i + 1 < args.len() && as_str(&args[i]).eq_ignore_ascii_case("LIMIT") {
                    i += 2;
                }
            }
            _ => break,
        }
    }

    if i >= args.len() {
        return Ok(RespValue::wrong_arity("xadd"));
    }

    let id_spec = as_str(&args[i]);
    i += 1;

    let field_args = &args[i..];
    if field_args.is_empty() || field_args.len() % 2 != 0 {
        return Ok(RespValue::wrong_arity("xadd"));
    }

    keys::purge_if_expired_non_string(client, db, &key)?;
    let last_id = get_last_stream_id(client, db, &key)?;
    if nomkstream && last_id.is_none() {
        return Ok(RespValue::Null);
    }

    let (ms, seq) = if id_spec == "*" {
        let now = now_unix_ms();
        match last_id {
            Some((last_ms, last_seq)) if now <= last_ms => (last_ms, last_seq + 1),
            _ => (now, 0),
        }
    } else if let Some(ms_part) = id_spec.strip_suffix("-*") {
        let Ok(ms_val) = ms_part.parse::<i64>() else {
            return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
        };
        if ms_val < 0 {
            return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
        }
        match last_id {
            Some((last_ms, _last_seq)) if ms_val < last_ms => {
                return Ok(RespValue::err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                ));
            }
            Some((last_ms, last_seq)) if ms_val == last_ms => (ms_val, last_seq + 1),
            _ => (ms_val, if ms_val == 0 { 1 } else { 0 }),
        }
    } else {
        let Some((ms_val, seq_val)) = parse_stream_id_exact(&id_spec) else {
            return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
        };
        if ms_val == 0 && seq_val == 0 {
            return Ok(RespValue::err("ERR The ID specified in XADD must be greater than 0-0"));
        }
        if let Some((last_ms, last_seq)) = last_id {
            if ms_val < last_ms || (ms_val == last_ms && seq_val <= last_seq) {
                return Ok(RespValue::err(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                ));
            }
        }
        (ms_val, seq_val)
    };

    let fields_json = encode_entry_fields(field_args);
    client.update(
        "INSERT INTO pg_redis_streams (db, key, ms, seq, fields) VALUES ($1, $2, $3, $4, $5)",
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
            (PgBuiltInOids::INT8OID.oid(), ms.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), seq.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), fields_json.into_datum()),
        ]),
    )?;

    if let Some(strategy) = trim {
        let _ = apply_trim(client, db, &key, strategy)?;
    }

    mutated_keys.push((db, key));
    Ok(RespValue::bulk(format!("{ms}-{seq}")))
}

/// `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count]`
pub fn cmd_xtrim(client: &mut SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 4 {
        return Ok(RespValue::wrong_arity("xtrim"));
    }
    let key = as_str(&args[1]);
    let strategy_kw = as_str(&args[2]).to_uppercase();
    let mut i = 3;
    if i < args.len() && (args[i].as_ref() == b"~" || args[i].as_ref() == b"=") {
        i += 1;
    }
    if i >= args.len() {
        return Ok(RespValue::syntax_err());
    }

    let strategy = match strategy_kw.as_str() {
        "MAXLEN" => {
            let Some(max_len) = parse_i64(&args[i]) else {
                return Ok(RespValue::not_integer_err());
            };
            StreamTrimStrategy::MaxLen(max_len)
        }
        "MINID" => {
            let id_s = as_str(&args[i]);
            let Some((ms, seq)) = parse_stream_id_exact(&id_s) else {
                return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
            };
            StreamTrimStrategy::MinId(ms, seq)
        }
        _ => return Ok(RespValue::syntax_err()),
    };

    if keys::purge_if_expired_non_string(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }

    let deleted = apply_trim(client, db, &key, strategy)?;
    if deleted > 0 {
        keys::cleanup_expiry_if_empty(client, "pg_redis_streams", db, &key)?;
    }
    Ok(RespValue::Integer(deleted))
}

pub fn cmd_xlen(client: &SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("xlen"));
    }
    let key = as_str(&args[1]);
    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let count = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_streams WHERE db = $1 AND key = $2",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            ]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    Ok(RespValue::Integer(count))
}

pub fn cmd_xdel(client: &mut SpiClient, db: i16, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("xdel"));
    }
    let key = as_str(&args[1]);
    if keys::purge_if_expired_non_string(client, db, &key)? {
        return Ok(RespValue::Integer(0));
    }
    let mut deleted: i64 = 0;

    for raw_id in &args[2..] {
        let id_s = as_str(raw_id);
        let Some((ms, seq)) = parse_stream_id_exact(&id_s) else {
            return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
        };
        let res = client.update(
            "DELETE FROM pg_redis_streams WHERE db = $1 AND key = $2 AND ms = $3 AND seq = $4",
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::INT8OID.oid(), ms.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), seq.into_datum()),
            ]),
        )?;
        deleted += res.len() as i64;
    }

    if deleted > 0 {
        keys::cleanup_expiry_if_empty(client, "pg_redis_streams", db, &key)?;
    }

    Ok(RespValue::Integer(deleted))
}

pub fn cmd_xrange(client: &SpiClient, db: i16, args: &[Bytes], rev: bool) -> SpiResult<RespValue> {
    let name = if rev { "xrevrange" } else { "xrange" };
    if args.len() < 4 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    let (start_s, end_s) = if rev {
        (as_str(&args[3]), as_str(&args[2]))
    } else {
        (as_str(&args[2]), as_str(&args[3]))
    };

    let Some((start_ms, start_seq, start_inc)) = parse_range_bound(&start_s, false) else {
        return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
    };
    let Some((end_ms, end_seq, end_inc)) = parse_range_bound(&end_s, true) else {
        return Ok(RespValue::err("ERR Invalid stream ID specified as stream command argument"));
    };

    let mut count_opt: Option<i64> = None;
    if args.len() == 6 && as_str(&args[4]).eq_ignore_ascii_case("COUNT") {
        let Some(c) = parse_i64(&args[5]) else {
            return Ok(RespValue::not_integer_err());
        };
        count_opt = Some(c.max(0));
    } else if args.len() != 4 {
        return Ok(RespValue::syntax_err());
    }

    if keys::is_non_string_expired(client, db, &key)? {
        return Ok(RespValue::Array(Vec::new()));
    }

    let start_cmp = if start_inc { ">=" } else { ">" };
    let end_cmp = if end_inc { "<=" } else { "<" };
    let order = if rev {
        "ms DESC, seq DESC"
    } else {
        "ms ASC, seq ASC"
    };
    let limit_clause = match count_opt {
        Some(c) => format!("LIMIT {c}"),
        None => String::new(),
    };

    let sql = format!(
        "SELECT ms, seq, fields FROM pg_redis_streams \
         WHERE db = $1 AND key = $2 \
           AND (ms, seq) {start_cmp} ($3, $4) \
           AND (ms, seq) {end_cmp} ($5, $6) \
         ORDER BY {order} {limit_clause}"
    );

    let table = client.select(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), start_ms.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), start_seq.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), end_ms.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), end_seq.into_datum()),
        ]),
    )?;

    let mut entries = Vec::with_capacity(table.len());
    for row in table {
        let ms: i64 = row.get(1)?.unwrap_or(0);
        let seq: i64 = row.get(2)?.unwrap_or(0);
        let fields_json: String = row.get(3)?.unwrap_or_default();
        entries.push(RespValue::Array(vec![
            RespValue::bulk(format!("{ms}-{seq}")),
            RespValue::Array(decode_entry_fields(&fields_json)),
        ]));
    }

    Ok(RespValue::Array(entries))
}

/// Parse `XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]`.
pub fn parse_xread_args(args: &[Bytes]) -> Result<XReadRequest, RespValue> {
    if args.len() < 4 {
        return Err(RespValue::wrong_arity("xread"));
    }

    let mut count: Option<i64> = None;
    let mut block_ms: Option<u64> = None;
    let mut i = 1;

    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(RespValue::syntax_err());
                }
                let Some(c) = parse_i64(&args[i + 1]) else {
                    return Err(RespValue::not_integer_err());
                };
                if c < 0 {
                    return Err(RespValue::err("ERR value is out of range, must be positive"));
                }
                count = Some(c);
                i += 2;
            }
            "BLOCK" => {
                if i + 1 >= args.len() {
                    return Err(RespValue::syntax_err());
                }
                let Some(ms) = parse_i64(&args[i + 1]) else {
                    return Err(RespValue::not_integer_err());
                };
                if ms < 0 {
                    return Err(RespValue::err("ERR timeout is negative"));
                }
                block_ms = Some(ms as u64);
                i += 2;
            }
            "STREAMS" => {
                i += 1;
                break;
            }
            _ => return Err(RespValue::syntax_err()),
        }
    }

    let rem = &args[i..];
    if rem.is_empty() || rem.len() % 2 != 0 {
        return Err(RespValue::err(
            "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' must be specified.",
        ));
    }

    let half = rem.len() / 2;
    let mut streams = Vec::with_capacity(half);
    for idx in 0..half {
        streams.push((as_str(&rem[idx]), as_str(&rem[half + idx])));
    }

    Ok(XReadRequest {
        count,
        block_ms,
        streams,
    })
}

/// Resolve any `$` IDs in an `XReadRequest` against the current top ID in PostgreSQL
/// so subsequent blocking wakeups read entries added strictly AFTER `XREAD` started.
pub fn resolve_xread_ids(
    client: &SpiClient,
    db: i16,
    streams: &[(String, String)],
) -> SpiResult<Result<Vec<(String, i64, i64)>, RespValue>> {
    let mut resolved = Vec::with_capacity(streams.len());
    for (key, id_str) in streams {
        if id_str == "$" {
            let (ms, seq) = if keys::is_non_string_expired(client, db, key)? {
                (0, 0)
            } else {
                get_last_stream_id(client, db, key)?.unwrap_or((0, 0))
            };
            resolved.push((key.clone(), ms, seq));
        } else {
            let Some((ms, seq)) = parse_stream_id_exact(id_str) else {
                return Ok(Err(RespValue::err(
                    "ERR Invalid stream ID specified as stream command argument",
                )));
            };
            resolved.push((key.clone(), ms, seq));
        }
    }
    Ok(Ok(resolved))
}

/// Execute one non-blocking `XREAD` pass over resolved `(key, after_ms, after_seq)` stream cursors.
/// Returns `Ok(Some(RespValue))` when at least one stream has new entries, or `Ok(None)` when empty.
pub fn execute_xread_resolved(
    client: &SpiClient,
    db: i16,
    resolved: &[(String, i64, i64)],
    count: Option<i64>,
) -> SpiResult<Option<RespValue>> {
    let limit_clause = match count {
        Some(c) => format!("LIMIT {c}"),
        None => String::new(),
    };
    let sql = format!(
        "SELECT ms, seq, fields FROM pg_redis_streams \
         WHERE db = $1 AND key = $2 AND (ms > $3 OR (ms = $3 AND seq > $4)) \
         ORDER BY ms ASC, seq ASC {limit_clause}"
    );

    let mut stream_results: Vec<RespValue> = Vec::new();

    for (key, after_ms, after_seq) in resolved {
        if keys::is_non_string_expired(client, db, key)? {
            continue;
        }
        let table = client.select(
            &sql,
            None,
            Some(vec![
                (PgBuiltInOids::INT2OID.oid(), db.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::INT8OID.oid(), (*after_ms).into_datum()),
                (PgBuiltInOids::INT8OID.oid(), (*after_seq).into_datum()),
            ]),
        )?;

        if table.is_empty() {
            continue;
        }

        let mut entries = Vec::with_capacity(table.len());
        for row in table {
            let ms: i64 = row.get(1)?.unwrap_or(0);
            let seq: i64 = row.get(2)?.unwrap_or(0);
            let fields_json: String = row.get(3)?.unwrap_or_default();
            entries.push(RespValue::Array(vec![
                RespValue::bulk(format!("{ms}-{seq}")),
                RespValue::Array(decode_entry_fields(&fields_json)),
            ]));
        }

        stream_results.push(RespValue::Array(vec![
            RespValue::bulk(key.clone()),
            RespValue::Array(entries),
        ]));
    }

    if stream_results.is_empty() {
        Ok(None)
    } else {
        Ok(Some(RespValue::Array(stream_results)))
    }
}

pub fn cmd_xread_nonblocking(
    client: &SpiClient,
    db: i16,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    let req = match parse_xread_args(args) {
        Ok(r) => r,
        Err(e) => return Ok(e),
    };
    let resolved = match resolve_xread_ids(client, db, &req.streams)? {
        Ok(r) => r,
        Err(e) => return Ok(e),
    };
    match execute_xread_resolved(client, db, &resolved, req.count)? {
        Some(v) => Ok(v),
        None => Ok(RespValue::Null),
    }
}
