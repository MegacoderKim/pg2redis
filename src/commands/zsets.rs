use bytes::Bytes;
use pgrx::prelude::*;
use pgrx::spi::{SpiClient, SpiResult};

use crate::protocol::{format_score, RespValue};
use crate::storage::{PreparedPlans, SQL_ZSET_SCORE};

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn parse_i64(b: &Bytes) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

fn parse_score(b: &Bytes) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "+inf" | "inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        _ => {
            let v = s.parse::<f64>().ok()?;
            if v.is_nan() {
                None
            } else {
                Some(v)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ScoreBound {
    NegInf,
    PosInf,
    Inclusive(f64),
    Exclusive(f64),
}

fn parse_score_bound(b: &Bytes) -> Option<ScoreBound> {
    let s = std::str::from_utf8(b).ok()?.trim();
    if let Some(rest) = s.strip_prefix('(') {
        let v = parse_score(&Bytes::copy_from_slice(rest.as_bytes()))?;
        return Some(ScoreBound::Exclusive(v));
    }
    match s.to_ascii_lowercase().as_str() {
        "-inf" => Some(ScoreBound::NegInf),
        "+inf" | "inf" => Some(ScoreBound::PosInf),
        _ => {
            let v = s.parse::<f64>().ok()?;
            if v.is_nan() {
                None
            } else {
                Some(ScoreBound::Inclusive(v))
            }
        }
    }
}

fn score_matches_bounds(score: f64, min: ScoreBound, max: ScoreBound) -> bool {
    let min_ok = match min {
        ScoreBound::NegInf => true,
        ScoreBound::PosInf => score == f64::INFINITY,
        ScoreBound::Inclusive(v) => score >= v,
        ScoreBound::Exclusive(v) => score > v,
    };
    let max_ok = match max {
        ScoreBound::PosInf => true,
        ScoreBound::NegInf => score == f64::NEG_INFINITY,
        ScoreBound::Inclusive(v) => score <= v,
        ScoreBound::Exclusive(v) => score < v,
    };
    min_ok && max_ok
}

pub fn cmd_zadd(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() < 4 {
        return Ok(RespValue::wrong_arity("zadd"));
    }

    let key = as_str(&args[1]);
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut ch = false;
    let mut incr = false;

    let mut i = 2;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "GT" => {
                gt = true;
                i += 1;
            }
            "LT" => {
                lt = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            "INCR" => {
                incr = true;
                i += 1;
            }
            _ => break,
        }
    }

    if nx && xx {
        return Ok(RespValue::err(
            "ERR XX and NX options at the same time are not compatible",
        ));
    }
    if (gt && lt) || (nx && (gt || lt)) {
        return Ok(RespValue::err(
            "ERR GT, LT, and/or NX options at the same time are not compatible",
        ));
    }

    let rem = &args[i..];
    if rem.is_empty() || rem.len() % 2 != 0 {
        return Ok(RespValue::syntax_err());
    }
    if incr && rem.len() != 2 {
        return Ok(RespValue::err(
            "ERR INCR option supports a single increment-element pair",
        ));
    }

    // Validate all scores upfront before mutating
    let mut items = Vec::with_capacity(rem.len() / 2);
    for pair in rem.chunks_exact(2) {
        let Some(score) = parse_score(&pair[0]) else {
            return Ok(RespValue::not_float_err());
        };
        items.push((score, as_str(&pair[1])));
    }

    let mut added_count: i64 = 0;
    let mut changed_count: i64 = 0;

    for (score_arg, member) in items {
        let existing_score = get_member_score(client, plans, &key, &member)?;
        match existing_score {
            None => {
                if xx {
                    if incr {
                        return Ok(RespValue::Null);
                    }
                    continue;
                }
                client.update(
                    "INSERT INTO pg_redis_zsets (key, member, score) VALUES ($1, $2, $3) \
                     ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score",
                    None,
                    Some(vec![
                        (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                        (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
                        (PgBuiltInOids::FLOAT8OID.oid(), score_arg.into_datum()),
                    ]),
                )?;
                added_count += 1;
                changed_count += 1;
                if incr {
                    return Ok(RespValue::Double(score_arg));
                }
            }
            Some(old_score) => {
                if nx {
                    if incr {
                        return Ok(RespValue::Null);
                    }
                    continue;
                }
                let new_score = if incr {
                    let s = old_score + score_arg;
                    if s.is_nan() {
                        return Ok(RespValue::err("ERR resulting score is not a number (NaN)"));
                    }
                    s
                } else {
                    score_arg
                };

                let should_update = if gt {
                    new_score > old_score
                } else if lt {
                    new_score < old_score
                } else {
                    true
                };

                if !should_update {
                    if incr {
                        return Ok(RespValue::Null);
                    }
                    continue;
                }

                if new_score != old_score {
                    client.update(
                        "UPDATE pg_redis_zsets SET score = $3 WHERE key = $1 AND member = $2",
                        None,
                        Some(vec![
                            (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                            (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
                            (PgBuiltInOids::FLOAT8OID.oid(), new_score.into_datum()),
                        ]),
                    )?;
                    changed_count += 1;
                }

                if incr {
                    return Ok(RespValue::Double(new_score));
                }
            }
        }
    }

    Ok(RespValue::Integer(if ch {
        changed_count
    } else {
        added_count
    }))
}

fn get_member_score(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    key: &str,
    member: &str,
) -> SpiResult<Option<f64>> {
    let table = match plans {
        Some(p) => client.select(
            &p.zset_score,
            None,
            Some(vec![key.into_datum(), member.into_datum()]),
        )?,
        None => client.select(
            SQL_ZSET_SCORE,
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            ]),
        )?,
    };
    if table.is_empty() {
        return Ok(None);
    }
    table.first().get_one::<f64>()
}

pub fn cmd_zscore(
    client: &SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 3 {
        return Ok(RespValue::wrong_arity("zscore"));
    }
    let key = as_str(&args[1]);
    let member = as_str(&args[2]);
    match get_member_score(client, plans, &key, &member)? {
        Some(s) => Ok(RespValue::Double(s)),
        None => Ok(RespValue::Null),
    }
}

pub fn cmd_zrem(client: &mut SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() < 3 {
        return Ok(RespValue::wrong_arity("zrem"));
    }
    let key = as_str(&args[1]);
    let mut removed: i64 = 0;

    for raw_member in &args[2..] {
        let member = as_str(raw_member);
        let res = client.update(
            "DELETE FROM pg_redis_zsets WHERE key = $1 AND member = $2",
            None,
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            ]),
        )?;
        removed += res.len() as i64;
    }

    Ok(RespValue::Integer(removed))
}

pub fn cmd_zcard(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 2 {
        return Ok(RespValue::wrong_arity("zcard"));
    }
    let key = as_str(&args[1]);
    let count = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_zsets WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);
    Ok(RespValue::Integer(count))
}

pub fn cmd_zrank(client: &SpiClient, args: &[Bytes], rev: bool) -> SpiResult<RespValue> {
    let name = if rev { "zrevrank" } else { "zrank" };
    if args.len() < 3 || args.len() > 4 {
        return Ok(RespValue::wrong_arity(name));
    }
    let withscore = if args.len() == 4 {
        if as_str(&args[3]).eq_ignore_ascii_case("WITHSCORE") {
            true
        } else {
            return Ok(RespValue::syntax_err());
        }
    } else {
        false
    };

    let key = as_str(&args[1]);
    let member = as_str(&args[2]);
    let order = if rev {
        "score DESC, member DESC"
    } else {
        "score ASC, member ASC"
    };

    let sql = format!(
        "SELECT rn - 1, score FROM ( \
            SELECT member, score, ROW_NUMBER() OVER (ORDER BY {order})::BIGINT AS rn \
            FROM pg_redis_zsets WHERE key = $1 \
         ) ranked WHERE member = $2"
    );

    let table = client.select(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
        ]),
    )?;

    if table.is_empty() {
        return Ok(RespValue::Null);
    }

    let row = table.first();
    let rank: i64 = row.get(1)?.unwrap_or(0);
    let score: f64 = row.get(2)?.unwrap_or(0.0);

    if withscore {
        Ok(RespValue::Array(vec![
            RespValue::Integer(rank),
            RespValue::Double(score),
        ]))
    } else {
        Ok(RespValue::Integer(rank))
    }
}

pub fn cmd_zincrby(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("zincrby"));
    }
    let key = as_str(&args[1]);
    let Some(inc) = parse_score(&args[2]) else {
        return Ok(RespValue::not_float_err());
    };
    let member = as_str(&args[3]);

    let current = get_member_score(client, plans, &key, &member)?.unwrap_or(0.0);
    let next = current + inc;
    if next.is_nan() {
        return Ok(RespValue::err("ERR resulting score is not a number (NaN)"));
    }

    client.update(
        "INSERT INTO pg_redis_zsets (key, member, score) VALUES ($1, $2, $3) \
         ON CONFLICT (key, member) DO UPDATE SET score = EXCLUDED.score",
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), member.into_datum()),
            (PgBuiltInOids::FLOAT8OID.oid(), next.into_datum()),
        ]),
    )?;

    Ok(RespValue::Double(next))
}

pub fn cmd_zrange(client: &SpiClient, args: &[Bytes], force_rev: bool) -> SpiResult<RespValue> {
    let name = if force_rev { "zrevrange" } else { "zrange" };
    if args.len() < 4 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    let mut rev = force_rev;
    let mut byscore = false;
    let mut withscores = false;
    let mut limit_opt: Option<(i64, i64)> = None;

    let mut i = 4;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "REV" if !force_rev => {
                rev = true;
                i += 1;
            }
            "BYSCORE" if !force_rev => {
                byscore = true;
                i += 1;
            }
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" if !force_rev => {
                if i + 2 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(offset) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                let Some(count) = parse_i64(&args[i + 2]) else {
                    return Ok(RespValue::not_integer_err());
                };
                limit_opt = Some((offset, count));
                i += 3;
            }
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    if byscore {
        let (min_raw, max_raw) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        return query_zrange_by_score(client, &key, min_raw, max_raw, rev, withscores, limit_opt);
    }

    let Some(mut start) = parse_i64(&args[2]) else {
        return Ok(RespValue::not_integer_err());
    };
    let Some(mut stop) = parse_i64(&args[3]) else {
        return Ok(RespValue::not_integer_err());
    };

    let len = client
        .select(
            "SELECT COUNT(*)::BIGINT FROM pg_redis_zsets WHERE key = $1",
            None,
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.clone().into_datum())]),
        )?
        .first()
        .get_one::<i64>()?
        .unwrap_or(0);

    if len == 0 {
        return Ok(if withscores {
            RespValue::ScoredArray(Vec::new())
        } else {
            RespValue::Array(Vec::new())
        });
    }

    if start < 0 {
        start = (len + start).max(0);
    }
    if stop < 0 {
        stop += len;
    }
    if start > stop || start >= len || stop < 0 {
        return Ok(if withscores {
            RespValue::ScoredArray(Vec::new())
        } else {
            RespValue::Array(Vec::new())
        });
    }
    if stop >= len {
        stop = len - 1;
    }

    let count = stop - start + 1;
    let order = if rev {
        "score DESC, member DESC"
    } else {
        "score ASC, member ASC"
    };
    let sql = format!(
        "SELECT member, score FROM pg_redis_zsets \
         WHERE key = $1 \
         ORDER BY {order} \
         OFFSET $2 LIMIT $3"
    );

    let table = client.select(
        &sql,
        None,
        Some(vec![
            (PgBuiltInOids::TEXTOID.oid(), key.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), start.into_datum()),
            (PgBuiltInOids::INT8OID.oid(), count.into_datum()),
        ]),
    )?;

    format_zset_rows(table, withscores)
}

pub fn cmd_zrangebyscore(
    client: &SpiClient,
    args: &[Bytes],
    rev: bool,
) -> SpiResult<RespValue> {
    let name = if rev {
        "zrevrangebyscore"
    } else {
        "zrangebyscore"
    };
    if args.len() < 4 {
        return Ok(RespValue::wrong_arity(name));
    }

    let key = as_str(&args[1]);
    let (min_raw, max_raw) = if rev {
        (&args[3], &args[2])
    } else {
        (&args[2], &args[3])
    };

    let mut withscores = false;
    let mut limit_opt: Option<(i64, i64)> = None;

    let mut i = 4;
    while i < args.len() {
        let opt = as_str(&args[i]).to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                withscores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Ok(RespValue::syntax_err());
                }
                let Some(offset) = parse_i64(&args[i + 1]) else {
                    return Ok(RespValue::not_integer_err());
                };
                let Some(count) = parse_i64(&args[i + 2]) else {
                    return Ok(RespValue::not_integer_err());
                };
                limit_opt = Some((offset, count));
                i += 3;
            }
            _ => return Ok(RespValue::syntax_err()),
        }
    }

    query_zrange_by_score(client, &key, min_raw, max_raw, rev, withscores, limit_opt)
}

fn query_zrange_by_score(
    client: &SpiClient,
    key: &str,
    min_raw: &Bytes,
    max_raw: &Bytes,
    rev: bool,
    withscores: bool,
    limit_opt: Option<(i64, i64)>,
) -> SpiResult<RespValue> {
    let Some(min_bound) = parse_score_bound(min_raw) else {
        return Ok(RespValue::err("ERR min or max is not a float"));
    };
    let Some(max_bound) = parse_score_bound(max_raw) else {
        return Ok(RespValue::err("ERR min or max is not a float"));
    };

    let order = if rev {
        "score DESC, member DESC"
    } else {
        "score ASC, member ASC"
    };
    let sql = format!(
        "SELECT member, score FROM pg_redis_zsets WHERE key = $1 ORDER BY {order}"
    );

    let table = client.select(
        &sql,
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;

    let mut matched: Vec<(Bytes, f64)> = Vec::new();
    for row in table {
        let member: String = row.get(1)?.unwrap_or_default();
        let score: f64 = row.get(2)?.unwrap_or(0.0);
        if score_matches_bounds(score, min_bound, max_bound) {
            matched.push((Bytes::from(member), score));
        }
    }

    if let Some((offset, count)) = limit_opt {
        if offset < 0 || offset as usize >= matched.len() || count == 0 {
            matched.clear();
        } else {
            let start = offset as usize;
            let sliced = &matched[start..];
            if count < 0 {
                matched = sliced.to_vec();
            } else {
                let end = (count as usize).min(sliced.len());
                matched = sliced[..end].to_vec();
            }
        }
    }

    if withscores {
        Ok(RespValue::ScoredArray(matched))
    } else {
        Ok(RespValue::Array(
            matched
                .into_iter()
                .map(|(m, _)| RespValue::BulkString(m))
                .collect(),
        ))
    }
}

pub fn cmd_zcount(client: &SpiClient, args: &[Bytes]) -> SpiResult<RespValue> {
    if args.len() != 4 {
        return Ok(RespValue::wrong_arity("zcount"));
    }
    let key = as_str(&args[1]);
    let Some(min_bound) = parse_score_bound(&args[2]) else {
        return Ok(RespValue::err("ERR min or max is not a float"));
    };
    let Some(max_bound) = parse_score_bound(&args[3]) else {
        return Ok(RespValue::err("ERR min or max is not a float"));
    };

    let table = client.select(
        "SELECT score FROM pg_redis_zsets WHERE key = $1",
        None,
        Some(vec![(PgBuiltInOids::TEXTOID.oid(), key.into_datum())]),
    )?;

    let mut count: i64 = 0;
    for row in table {
        let score: f64 = row.get(1)?.unwrap_or(0.0);
        if score_matches_bounds(score, min_bound, max_bound) {
            count += 1;
        }
    }
    Ok(RespValue::Integer(count))
}

fn format_zset_rows(
    table: pgrx::spi::SpiTupleTable<'_>,
    withscores: bool,
) -> SpiResult<RespValue> {
    if withscores {
        let mut pairs = Vec::with_capacity(table.len());
        for row in table {
            let member: String = row.get(1)?.unwrap_or_default();
            let score: f64 = row.get(2)?.unwrap_or(0.0);
            pairs.push((Bytes::from(member), score));
        }
        Ok(RespValue::ScoredArray(pairs))
    } else {
        let mut members = Vec::with_capacity(table.len());
        for row in table {
            let member: String = row.get(1)?.unwrap_or_default();
            members.push(RespValue::bulk(member));
        }
        Ok(RespValue::Array(members))
    }
}

#[allow(dead_code)]
fn _keep_format_score_used() -> String {
    format_score(0.0)
}
