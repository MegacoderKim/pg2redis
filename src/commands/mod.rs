pub mod hashes;
pub mod keys;
pub mod lists;
pub mod server;
pub mod strings;
pub mod zsets;

use bytes::Bytes;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::spi::{Spi, SpiClient, SpiResult};

use crate::cache::L1Cache;
use crate::protocol::{RespValue, RespVersion};
use crate::storage::PreparedPlans;

pub struct ConnectionState {
    pub client_id: u64,
    pub protocol: RespVersion,
    pub client_name: Option<String>,
    pub selected_db: i64,
    pub in_multi: bool,
    pub multi_queue: Vec<Vec<Bytes>>,
    pub should_quit: bool,
}

impl ConnectionState {
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id,
            protocol: RespVersion::Resp2,
            client_name: None,
            selected_db: 0,
            in_multi: false,
            multi_queue: Vec::new(),
            should_quit: false,
        }
    }
}

fn cmd_name_upper(args: &[Bytes]) -> String {
    args.first()
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
        .unwrap_or_default()
}

/// Try to handle a command purely in memory without opening a PostgreSQL SPI transaction.
/// Returns `Some(RespValue)` if satisfied in memory (server command, MULTI queue, or L1 cache hit),
/// or `None` if PostgreSQL SPI execution is required.
fn try_execute_in_memory(
    args: &[Bytes],
    conn: &mut ConnectionState,
    cache: Option<&L1Cache>,
) -> Option<RespValue> {
    let cmd = cmd_name_upper(args);

    if conn.in_multi {
        return match cmd.as_str() {
            "MULTI" => Some(RespValue::err("ERR MULTI calls can not be nested")),
            "DISCARD" => {
                conn.in_multi = false;
                conn.multi_queue.clear();
                Some(RespValue::ok())
            }
            "EXEC" => None, // Needs SPI transaction to execute queued commands atomically
            _ => {
                conn.multi_queue.push(args.to_vec());
                Some(RespValue::queued())
            }
        };
    }

    match cmd.as_str() {
        "PING" => Some(server::cmd_ping(args)),
        "ECHO" => Some(server::cmd_echo(args)),
        "QUIT" => Some(server::cmd_quit(conn)),
        "SELECT" => Some(server::cmd_select(conn, args)),
        "HELLO" => Some(server::cmd_hello(conn, args)),
        "CLIENT" => Some(server::cmd_client(conn, args)),
        "COMMAND" => Some(server::cmd_command(args)),
        "INFO" => Some(server::cmd_info(cache)),
        "CONFIG" => Some(server::cmd_config(args)),
        "TIME" => Some(server::cmd_time()),
        "AUTH" => Some(RespValue::ok()),
        "MULTI" => {
            conn.in_multi = true;
            conn.multi_queue.clear();
            Some(RespValue::ok())
        }
        "EXEC" => Some(RespValue::err("ERR EXEC without MULTI")),
        "DISCARD" => Some(RespValue::err("ERR DISCARD without MULTI")),
        "GET" if args.len() == 2 => {
            let c = cache?;
            let key = String::from_utf8_lossy(&args[1]);
            c.get(&key).map(RespValue::BulkString)
        }
        _ => None,
    }
}

/// Execute a single command inside an active PostgreSQL SPI connection.
pub fn dispatch_with_spi(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    conn: &mut ConnectionState,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    if args.is_empty() {
        return Ok(RespValue::err("ERR empty command"));
    }

    let cmd = cmd_name_upper(args);

    // Handle MULTI/EXEC/DISCARD inside SPI batch too
    if conn.in_multi {
        match cmd.as_str() {
            "MULTI" => return Ok(RespValue::err("ERR MULTI calls can not be nested")),
            "DISCARD" => {
                conn.in_multi = false;
                conn.multi_queue.clear();
                return Ok(RespValue::ok());
            }
            "EXEC" => {
                conn.in_multi = false;
                let queued = std::mem::take(&mut conn.multi_queue);
                let mut results = Vec::with_capacity(queued.len());
                for q_args in &queued {
                    let res = dispatch_single_non_multi(client, plans, cache, conn, q_args)?;
                    results.push(res);
                }
                return Ok(RespValue::Array(results));
            }
            _ => {
                conn.multi_queue.push(args.to_vec());
                return Ok(RespValue::queued());
            }
        }
    }

    match cmd.as_str() {
        "MULTI" => {
            conn.in_multi = true;
            conn.multi_queue.clear();
            Ok(RespValue::ok())
        }
        "EXEC" => Ok(RespValue::err("ERR EXEC without MULTI")),
        "DISCARD" => Ok(RespValue::err("ERR DISCARD without MULTI")),
        _ => dispatch_single_non_multi(client, plans, cache, conn, args),
    }
}

fn dispatch_single_non_multi(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    conn: &mut ConnectionState,
    args: &[Bytes],
) -> SpiResult<RespValue> {
    let cmd = cmd_name_upper(args);

    match cmd.as_str() {
        // Server / Protocol
        "PING" => Ok(server::cmd_ping(args)),
        "ECHO" => Ok(server::cmd_echo(args)),
        "QUIT" => Ok(server::cmd_quit(conn)),
        "SELECT" => Ok(server::cmd_select(conn, args)),
        "HELLO" => Ok(server::cmd_hello(conn, args)),
        "CLIENT" => Ok(server::cmd_client(conn, args)),
        "COMMAND" => Ok(server::cmd_command(args)),
        "INFO" => Ok(server::cmd_info(cache)),
        "CONFIG" => Ok(server::cmd_config(args)),
        "TIME" => Ok(server::cmd_time()),
        "AUTH" => Ok(RespValue::ok()),

        // Strings
        "GET" => strings::cmd_get(client, plans, cache, args),
        "SET" => strings::cmd_set(client, plans, cache, args),
        "SETEX" => strings::cmd_setex(client, plans, cache, args, false),
        "PSETEX" => strings::cmd_setex(client, plans, cache, args, true),
        "SETNX" => strings::cmd_setnx(client, plans, cache, args),
        "GETSET" => strings::cmd_getset(client, plans, cache, args),
        "GETDEL" => strings::cmd_getdel(client, plans, cache, args),
        "MGET" => strings::cmd_mget(client, plans, cache, args),
        "MSET" => strings::cmd_mset(client, plans, cache, args),
        "INCR" => strings::cmd_incrby(client, plans, cache, args, Some(1), false),
        "DECR" => strings::cmd_incrby(client, plans, cache, args, Some(-1), false),
        "INCRBY" => strings::cmd_incrby(client, plans, cache, args, None, false),
        "DECRBY" => strings::cmd_incrby(client, plans, cache, args, None, true),
        "INCRBYFLOAT" => strings::cmd_incrbyfloat(client, plans, cache, args),
        "APPEND" => strings::cmd_append(client, plans, cache, args),
        "STRLEN" => strings::cmd_strlen(client, plans, cache, args),

        // Keys & Expiry
        "DEL" | "UNLINK" => keys::cmd_del(client, cache, args),
        "EXISTS" => keys::cmd_exists(client, args),
        "EXPIRE" => keys::cmd_expire(client, cache, args, false, false),
        "PEXPIRE" => keys::cmd_expire(client, cache, args, true, false),
        "EXPIREAT" => keys::cmd_expire(client, cache, args, false, true),
        "PEXPIREAT" => keys::cmd_expire(client, cache, args, true, true),
        "TTL" => keys::cmd_ttl(client, args, false),
        "PTTL" => keys::cmd_ttl(client, args, true),
        "PERSIST" => keys::cmd_persist(client, cache, args),
        "TYPE" => keys::cmd_type(client, args),
        "KEYS" => keys::cmd_keys(client, args),
        "SCAN" => keys::cmd_scan(client, args),
        "DBSIZE" => keys::cmd_dbsize(client),
        "FLUSHDB" | "FLUSHALL" => keys::cmd_flushdb(client, cache),

        // Hashes
        "HSET" => hashes::cmd_hset(client, plans, args, false),
        "HMSET" => hashes::cmd_hset(client, plans, args, true),
        "HSETNX" => hashes::cmd_hsetnx(client, args),
        "HGET" => hashes::cmd_hget(client, plans, args),
        "HMGET" => hashes::cmd_hmget(client, plans, args),
        "HGETALL" => hashes::cmd_hgetall(client, plans, args),
        "HDEL" => hashes::cmd_hdel(client, args),
        "HEXISTS" => hashes::cmd_hexists(client, args),
        "HLEN" => hashes::cmd_hlen(client, args),
        "HKEYS" => hashes::cmd_hkeys(client, args),
        "HVALS" => hashes::cmd_hvals(client, args),
        "HINCRBY" => hashes::cmd_hincrby(client, plans, args),
        "HINCRBYFLOAT" => hashes::cmd_hincrbyfloat(client, plans, args),

        // Lists
        "LPUSH" => lists::cmd_push(client, args, true),
        "RPUSH" => lists::cmd_push(client, args, false),
        "LPOP" => lists::cmd_pop(client, args, true),
        "RPOP" => lists::cmd_pop(client, args, false),
        "LRANGE" => lists::cmd_lrange(client, args),
        "LLEN" => lists::cmd_llen(client, args),
        "LINDEX" => lists::cmd_lindex(client, args),
        "LSET" => lists::cmd_lset(client, args),
        "LREM" => lists::cmd_lrem(client, args),

        // Sorted Sets
        "ZADD" => zsets::cmd_zadd(client, plans, args),
        "ZRANGE" => zsets::cmd_zrange(client, args, false),
        "ZREVRANGE" => zsets::cmd_zrange(client, args, true),
        "ZRANGEBYSCORE" => zsets::cmd_zrangebyscore(client, args, false),
        "ZREVRANGEBYSCORE" => zsets::cmd_zrangebyscore(client, args, true),
        "ZREM" => zsets::cmd_zrem(client, args),
        "ZSCORE" => zsets::cmd_zscore(client, plans, args),
        "ZCARD" => zsets::cmd_zcard(client, args),
        "ZRANK" => zsets::cmd_zrank(client, args, false),
        "ZREVRANK" => zsets::cmd_zrank(client, args, true),
        "ZINCRBY" => zsets::cmd_zincrby(client, plans, args),
        "ZCOUNT" => zsets::cmd_zcount(client, args),

        _ => Ok(RespValue::err(format!(
            "ERR unknown command '{}'",
            cmd.to_lowercase()
        ))),
    }
}

/// Execute a pipelined batch of commands inside the background worker.
///
/// - Commands that can be satisfied purely in memory (protocol/server commands,
///   L1 cache hits, MULTI queueing) are resolved without starting a PostgreSQL transaction.
/// - As soon as any command requires PostgreSQL SPI, all remaining commands in the
///   batch execute together inside a SINGLE `BackgroundWorker::transaction` block (RFC §5.4).
pub fn execute_batch_in_worker(
    batch: Vec<Vec<Bytes>>,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    conn: &mut ConnectionState,
) -> Vec<(RespValue, RespVersion)> {
    let mut responses = Vec::with_capacity(batch.len());
    let mut idx = 0;

    // Fast path: resolve leading in-memory / L1-cached commands without starting a PG transaction
    while idx < batch.len() {
        if let Some(val) = try_execute_in_memory(&batch[idx], conn, cache) {
            responses.push((val, conn.protocol));
            idx += 1;
            if conn.should_quit {
                return responses;
            }
        } else {
            break;
        }
    }

    if idx >= batch.len() {
        return responses;
    }

    // Execute all remaining commands in the batch inside a single SPI transaction
    let remaining = &batch[idx..];
    let mut conn_guard = std::panic::AssertUnwindSafe(conn);
    let cache_guard = std::panic::AssertUnwindSafe(cache);
    let plans_guard = std::panic::AssertUnwindSafe(plans);
    let rem_guard = std::panic::AssertUnwindSafe(remaining);

    let tx_results: Vec<(RespValue, RespVersion)> = BackgroundWorker::transaction(move || {
        Spi::connect(|mut client| {
            let mut out = Vec::with_capacity(rem_guard.len());
            for args in *rem_guard {
                let val = match dispatch_with_spi(
                    &mut client,
                    *plans_guard,
                    *cache_guard,
                    *conn_guard,
                    args,
                ) {
                    Ok(v) => v,
                    Err(e) => RespValue::err(format!("ERR internal database SPI error: {e}")),
                };
                out.push((val, conn_guard.protocol));
                if conn_guard.should_quit {
                    break;
                }
            }
            out
        })
    });

    responses.extend(tx_results);
    responses
}
