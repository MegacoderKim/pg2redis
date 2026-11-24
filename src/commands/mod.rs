pub mod hashes;
pub mod keys;
pub mod lists;
pub mod server;
pub mod sets;
pub mod streams;
pub mod strings;
pub mod zsets;

use bytes::Bytes;
use pgrx::bgworkers::BackgroundWorker;
use pgrx::spi::{Spi, SpiClient, SpiResult};
use std::panic::AssertUnwindSafe;

use crate::cache::L1Cache;
use crate::clients::ClientRegistry;
use crate::config;
use crate::lua::LuaEngine;
use crate::notify::KeyNotifier;
use crate::protocol::{RespValue, RespVersion};
use crate::pubsub::PubSubHub;
use crate::storage::PreparedPlans;

pub struct ConnectionState {
    pub client_id: u64,
    pub protocol: RespVersion,
    pub client_name: Option<String>,
    pub selected_db: i64,
    pub authenticated: bool,
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
            authenticated: config::requirepass().is_none(),
            in_multi: false,
            multi_queue: Vec::new(),
            should_quit: false,
        }
    }
}

/// Asynchronous blocking commands handled cooperatively on the Tokio `LocalSet`.
#[derive(Debug, Clone)]
pub enum BlockingCommand {
    BZPop {
        keys: Vec<String>,
        timeout_secs: f64,
        is_max: bool,
    },
    BLPop {
        keys: Vec<String>,
        timeout_secs: f64,
        is_right: bool,
    },
    XReadBlock {
        req: streams::XReadRequest,
    },
}

pub fn cmd_name_upper(args: &[Bytes]) -> String {
    args.first()
        .map(|b| String::from_utf8_lossy(b).to_ascii_uppercase())
        .unwrap_or_default()
}

/// Detect whether `args` is a blocking command (`BZPOPMIN`, `BZPOPMAX`, `BLPOP`, `BRPOP`,
/// or `XREAD ... BLOCK ...`) when not inside `MULTI`.
pub fn parse_blocking_command(
    args: &[Bytes],
    in_multi: bool,
) -> Option<Result<BlockingCommand, RespValue>> {
    if in_multi || args.is_empty() {
        return None;
    }
    let cmd = cmd_name_upper(args);
    match cmd.as_str() {
        "BZPOPMIN" | "BZPOPMAX" => {
            if args.len() < 3 {
                return Some(Err(RespValue::wrong_arity(&cmd)));
            }
            let timeout_str = std::str::from_utf8(&args[args.len() - 1]).unwrap_or("");
            let Ok(timeout_secs) = timeout_str.parse::<f64>() else {
                return Some(Err(RespValue::err("ERR timeout is not a float or out of range")));
            };
            if timeout_secs.is_nan() || timeout_secs < 0.0 {
                return Some(Err(RespValue::err("ERR timeout is negative")));
            }
            let keys: Vec<String> = args[1..args.len() - 1]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            Some(Ok(BlockingCommand::BZPop {
                keys,
                timeout_secs,
                is_max: cmd == "BZPOPMAX",
            }))
        }
        "BLPOP" | "BRPOP" => {
            if args.len() < 3 {
                return Some(Err(RespValue::wrong_arity(&cmd)));
            }
            let timeout_str = std::str::from_utf8(&args[args.len() - 1]).unwrap_or("");
            let Ok(timeout_secs) = timeout_str.parse::<f64>() else {
                return Some(Err(RespValue::err("ERR timeout is not a float or out of range")));
            };
            if timeout_secs.is_nan() || timeout_secs < 0.0 {
                return Some(Err(RespValue::err("ERR timeout is negative")));
            }
            let keys: Vec<String> = args[1..args.len() - 1]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            Some(Ok(BlockingCommand::BLPop {
                keys,
                timeout_secs,
                is_right: cmd == "BRPOP",
            }))
        }
        "XREAD" => match streams::parse_xread_args(args) {
            Ok(req) if req.block_ms.is_some() => Some(Ok(BlockingCommand::XReadBlock { req })),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        },
        _ => None,
    }
}

fn handle_subscribe_cmd(
    pubsub: Option<&PubSubHub>,
    conn: &ConnectionState,
    args: &[Bytes],
) -> Vec<RespValue> {
    if args.len() < 2 {
        return vec![RespValue::wrong_arity("subscribe")];
    }
    let Some(hub) = pubsub else {
        return vec![RespValue::err("ERR Pub/Sub is unavailable in this session")];
    };
    let mut acks = Vec::with_capacity(args.len() - 1);
    for ch in &args[1..] {
        let count = hub.subscribe(conn.client_id, ch.clone()) as i64;
        acks.push(RespValue::Push(vec![
            RespValue::bulk("subscribe"),
            RespValue::BulkString(ch.clone()),
            RespValue::Integer(count),
        ]));
    }
    acks
}

fn handle_unsubscribe_cmd(
    pubsub: Option<&PubSubHub>,
    conn: &ConnectionState,
    args: &[Bytes],
) -> Vec<RespValue> {
    let Some(hub) = pubsub else {
        return vec![RespValue::err("ERR Pub/Sub is unavailable in this session")];
    };
    if args.len() == 1 {
        let removed = hub.unsubscribe_all(conn.client_id);
        if removed.is_empty() {
            let remaining = hub.subscription_count(conn.client_id) as i64;
            return vec![RespValue::Push(vec![
                RespValue::bulk("unsubscribe"),
                RespValue::Null,
                RespValue::Integer(remaining),
            ])];
        }
        return removed
            .into_iter()
            .map(|(ch, remaining)| {
                RespValue::Push(vec![
                    RespValue::bulk("unsubscribe"),
                    RespValue::BulkString(ch),
                    RespValue::Integer(remaining as i64),
                ])
            })
            .collect();
    }

    let mut acks = Vec::with_capacity(args.len() - 1);
    for ch in &args[1..] {
        let remaining = hub.unsubscribe(conn.client_id, ch) as i64;
        acks.push(RespValue::Push(vec![
            RespValue::bulk("unsubscribe"),
            RespValue::BulkString(ch.clone()),
            RespValue::Integer(remaining),
        ]));
    }
    acks
}

fn handle_psubscribe_cmd(
    pubsub: Option<&PubSubHub>,
    conn: &ConnectionState,
    args: &[Bytes],
) -> Vec<RespValue> {
    if args.len() < 2 {
        return vec![RespValue::wrong_arity("psubscribe")];
    }
    let Some(hub) = pubsub else {
        return vec![RespValue::err("ERR Pub/Sub is unavailable in this session")];
    };
    let mut acks = Vec::with_capacity(args.len() - 1);
    for pat in &args[1..] {
        let count = hub.psubscribe(conn.client_id, pat.clone()) as i64;
        acks.push(RespValue::Push(vec![
            RespValue::bulk("psubscribe"),
            RespValue::BulkString(pat.clone()),
            RespValue::Integer(count),
        ]));
    }
    acks
}

fn handle_punsubscribe_cmd(
    pubsub: Option<&PubSubHub>,
    conn: &ConnectionState,
    args: &[Bytes],
) -> Vec<RespValue> {
    let Some(hub) = pubsub else {
        return vec![RespValue::err("ERR Pub/Sub is unavailable in this session")];
    };
    if args.len() == 1 {
        let removed = hub.punsubscribe_all(conn.client_id);
        if removed.is_empty() {
            let remaining = hub.subscription_count(conn.client_id) as i64;
            return vec![RespValue::Push(vec![
                RespValue::bulk("punsubscribe"),
                RespValue::Null,
                RespValue::Integer(remaining),
            ])];
        }
        return removed
            .into_iter()
            .map(|(pat, remaining)| {
                RespValue::Push(vec![
                    RespValue::bulk("punsubscribe"),
                    RespValue::BulkString(pat),
                    RespValue::Integer(remaining as i64),
                ])
            })
            .collect();
    }

    let mut acks = Vec::with_capacity(args.len() - 1);
    for pat in &args[1..] {
        let remaining = hub.punsubscribe(conn.client_id, pat) as i64;
        acks.push(RespValue::Push(vec![
            RespValue::bulk("punsubscribe"),
            RespValue::BulkString(pat.clone()),
            RespValue::Integer(remaining),
        ]));
    }
    acks
}

fn handle_publish_cmd(pubsub: Option<&PubSubHub>, args: &[Bytes]) -> RespValue {
    if args.len() != 3 {
        return RespValue::wrong_arity("publish");
    }
    let delivered = pubsub
        .map(|hub| hub.publish(&args[1], &args[2]))
        .unwrap_or(0);
    RespValue::Integer(delivered)
}

fn handle_pubsub_cmd(pubsub: Option<&PubSubHub>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::wrong_arity("pubsub");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "CHANNELS" => {
            if args.len() > 3 {
                return RespValue::wrong_arity("pubsub|channels");
            }
            let pat = args
                .get(2)
                .map(|b| String::from_utf8_lossy(b).into_owned());
            let channels = pubsub
                .map(|hub| hub.active_channels(pat.as_deref()))
                .unwrap_or_default();
            RespValue::Array(channels.into_iter().map(RespValue::BulkString).collect())
        }
        "NUMSUB" => {
            let counts = pubsub
                .map(|hub| hub.numsub(&args[2..]))
                .unwrap_or_else(|| args[2..].iter().map(|ch| (ch.clone(), 0)).collect());
            let mut out = Vec::with_capacity(counts.len() * 2);
            for (ch, cnt) in counts {
                out.push(RespValue::BulkString(ch));
                out.push(RespValue::Integer(cnt));
            }
            RespValue::Array(out)
        }
        "NUMPAT" => RespValue::Integer(pubsub.map(PubSubHub::numpat).unwrap_or(0)),
        _ => RespValue::err(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{sub}'. Try PUBSUB HELP."
        )),
    }
}

fn handle_reset_cmd(
    pubsub: Option<&PubSubHub>,
    clients: Option<&ClientRegistry>,
    conn: &mut ConnectionState,
) -> RespValue {
    if let Some(hub) = pubsub {
        hub.unsubscribe_everything(conn.client_id);
    }
    conn.in_multi = false;
    conn.multi_queue.clear();
    conn.protocol = RespVersion::Resp2;
    conn.client_name = None;
    conn.selected_db = 0;
    conn.authenticated = config::requirepass().is_none();
    if let Some(reg) = clients {
        reg.set_name(conn.client_id, None);
        reg.set_db(conn.client_id, 0);
        reg.set_proto(conn.client_id, RespVersion::Resp2);
    }
    RespValue::SimpleString(Bytes::from_static(b"RESET"))
}

fn handle_subscribed_ping(conn: &ConnectionState, args: &[Bytes]) -> RespValue {
    if args.len() > 2 {
        return RespValue::wrong_arity("ping");
    }
    if conn.protocol == RespVersion::Resp2 {
        let payload = args.get(1).cloned().unwrap_or_default();
        RespValue::Array(vec![RespValue::bulk("pong"), RespValue::BulkString(payload)])
    } else {
        server::cmd_ping(args)
    }
}

/// Try to handle a command purely in memory without opening a PostgreSQL SPI transaction.
/// Returns `Some(Vec<RespValue>)` because `SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE`
/// can emit multiple frames.
fn try_execute_in_memory_multi(
    args: &[Bytes],
    conn: &mut ConnectionState,
    cache: Option<&L1Cache>,
    lua: Option<&LuaEngine>,
    pubsub: Option<&PubSubHub>,
    clients: Option<&ClientRegistry>,
) -> Option<Vec<RespValue>> {
    let cmd = cmd_name_upper(args);

    // Enforce `pg_redis.requirepass` authentication gate before any other command.
    if config::requirepass().is_some() && !conn.authenticated {
        return Some(match cmd.as_str() {
            "AUTH" => vec![server::cmd_auth(conn, args)],
            "HELLO" => vec![server::cmd_hello(conn, clients, args)],
            "QUIT" => {
                if let Some(hub) = pubsub {
                    hub.unsubscribe_everything(conn.client_id);
                }
                vec![server::cmd_quit(conn)]
            }
            "RESET" => vec![handle_reset_cmd(pubsub, clients, conn)],
            _ => vec![RespValue::err("NOAUTH Authentication required.")],
        });
    }

    // Enforce Redis RESP2 subscribed-mode restrictions (ioredis uses RESP2 by default).
    // In RESP3, push frames (`>`) allow normal commands alongside subscriptions.
    let in_subscribed_resp2 = conn.protocol == RespVersion::Resp2
        && pubsub
            .map(|hub| hub.subscription_count(conn.client_id) > 0)
            .unwrap_or(false);

    if in_subscribed_resp2 {
        return Some(match cmd.as_str() {
            "SUBSCRIBE" => handle_subscribe_cmd(pubsub, conn, args),
            "UNSUBSCRIBE" => handle_unsubscribe_cmd(pubsub, conn, args),
            "PSUBSCRIBE" => handle_psubscribe_cmd(pubsub, conn, args),
            "PUNSUBSCRIBE" => handle_punsubscribe_cmd(pubsub, conn, args),
            "PING" => vec![handle_subscribed_ping(conn, args)],
            "QUIT" => {
                if let Some(hub) = pubsub {
                    hub.unsubscribe_everything(conn.client_id);
                }
                vec![server::cmd_quit(conn)]
            }
            "RESET" => vec![handle_reset_cmd(pubsub, clients, conn)],
            _ => vec![RespValue::err(format!(
                "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                cmd.to_lowercase()
            ))],
        });
    }

    if conn.in_multi {
        return match cmd.as_str() {
            "MULTI" => Some(vec![RespValue::err("ERR MULTI calls can not be nested")]),
            "DISCARD" => {
                conn.in_multi = false;
                conn.multi_queue.clear();
                Some(vec![RespValue::ok()])
            }
            "EXEC" => None,
            _ => {
                conn.multi_queue.push(args.to_vec());
                Some(vec![RespValue::queued()])
            }
        };
    }

    match cmd.as_str() {
        "SUBSCRIBE" => Some(handle_subscribe_cmd(pubsub, conn, args)),
        "UNSUBSCRIBE" => Some(handle_unsubscribe_cmd(pubsub, conn, args)),
        "PSUBSCRIBE" => Some(handle_psubscribe_cmd(pubsub, conn, args)),
        "PUNSUBSCRIBE" => Some(handle_punsubscribe_cmd(pubsub, conn, args)),
        "PUBLISH" => Some(vec![handle_publish_cmd(pubsub, args)]),
        "PUBSUB" => Some(vec![handle_pubsub_cmd(pubsub, args)]),
        "RESET" => Some(vec![handle_reset_cmd(pubsub, clients, conn)]),
        "PING" => Some(vec![server::cmd_ping(args)]),
        "ECHO" => Some(vec![server::cmd_echo(args)]),
        "QUIT" => {
            if let Some(hub) = pubsub {
                hub.unsubscribe_everything(conn.client_id);
            }
            Some(vec![server::cmd_quit(conn)])
        }
        "SELECT" => Some(vec![server::cmd_select(conn, clients, args)]),
        "HELLO" => Some(vec![server::cmd_hello(conn, clients, args)]),
        "CLIENT" => Some(vec![server::cmd_client(conn, clients, pubsub, args)]),
        "COMMAND" => Some(vec![server::cmd_command(args)]),
        "INFO" => Some(vec![server::cmd_info(cache, clients)]),
        "CONFIG" => Some(vec![server::cmd_config(args)]),
        "TIME" => Some(vec![server::cmd_time()]),
        "AUTH" => Some(vec![server::cmd_auth(conn, args)]),
        "SCRIPT" => Some(vec![handle_script_cmd(lua, args)]),
        "MULTI" => {
            conn.in_multi = true;
            conn.multi_queue.clear();
            Some(vec![RespValue::ok()])
        }
        "EXEC" => Some(vec![RespValue::err("ERR EXEC without MULTI")]),
        "DISCARD" => Some(vec![RespValue::err("ERR DISCARD without MULTI")]),
        "GET" if args.len() == 2 => {
            let c = cache?;
            let key = String::from_utf8_lossy(&args[1]);
            c.get(conn.selected_db as i16, &key)
                .map(|v| vec![RespValue::BulkString(v)])
        }
        _ => None,
    }
}

fn handle_script_cmd(lua: Option<&LuaEngine>, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::wrong_arity("script");
    }
    let Some(engine) = lua else {
        return RespValue::err("ERR Lua engine is unavailable in this session");
    };
    let sub = String::from_utf8_lossy(&args[1]).to_ascii_uppercase();
    match sub.as_str() {
        "LOAD" => {
            if args.len() != 3 {
                return RespValue::wrong_arity("script|load");
            }
            match engine.script_load(args[2].as_ref()) {
                Ok(sha) => RespValue::bulk(sha),
                Err(e) => RespValue::err(e),
            }
        }
        "EXISTS" => {
            if args.len() < 3 {
                return RespValue::wrong_arity("script|exists");
            }
            let shas: Vec<String> = args[2..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            let flags = engine.script_exists(&shas);
            RespValue::Array(flags.into_iter().map(RespValue::Integer).collect())
        }
        "FLUSH" => {
            engine.script_flush();
            RespValue::ok()
        }
        _ => RespValue::err(format!(
            "ERR Unknown SCRIPT subcommand or wrong number of arguments for '{sub}'"
        )),
    }
}

fn handle_eval_cmd(
    is_sha: bool,
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    lua: Option<&LuaEngine>,
    pubsub: Option<&PubSubHub>,
    conn: &mut ConnectionState,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> RespValue {
    let name = if is_sha { "evalsha" } else { "eval" };
    if args.len() < 3 {
        return RespValue::wrong_arity(name);
    }
    let Some(engine) = lua else {
        return RespValue::err("ERR Lua engine is unavailable in this session");
    };
    let Ok(numkeys) = std::str::from_utf8(&args[2]).unwrap_or("").parse::<i64>() else {
        return RespValue::not_integer_err();
    };
    if numkeys < 0 {
        return RespValue::err("ERR Number of keys can't be negative");
    }
    let numkeys_usize = numkeys as usize;
    if 3 + numkeys_usize > args.len() {
        return RespValue::err("ERR Number of keys can't be greater than number of args");
    }

    let keys = &args[3..3 + numkeys_usize];
    let argv = &args[3 + numkeys_usize..];

    if is_sha {
        let sha = String::from_utf8_lossy(&args[1]);
        engine.evalsha(
            &sha,
            keys,
            argv,
            client,
            plans,
            cache,
            pubsub,
            conn,
            mutated_keys,
        )
    } else {
        engine.eval(
            args[1].as_ref(),
            keys,
            argv,
            client,
            plans,
            cache,
            pubsub,
            conn,
            mutated_keys,
        )
    }
}

/// Execute a single command inside an active PostgreSQL SPI connection.
pub fn dispatch_with_spi(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    lua: Option<&LuaEngine>,
    pubsub: Option<&PubSubHub>,
    clients: Option<&ClientRegistry>,
    conn: &mut ConnectionState,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    if args.is_empty() {
        return Ok(RespValue::err("ERR empty command"));
    }

    let cmd = cmd_name_upper(args);

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
                    let res = dispatch_single_non_multi(
                        client,
                        plans,
                        cache,
                        lua,
                        pubsub,
                        clients,
                        conn,
                        q_args,
                        mutated_keys,
                    )?;
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
        _ => dispatch_single_non_multi(
            client,
            plans,
            cache,
            lua,
            pubsub,
            clients,
            conn,
            args,
            mutated_keys,
        ),
    }
}

/// Entry point for `redis.call(...)` and `redis.pcall(...)` inside Lua scripts.
/// Executes directly in the active PostgreSQL SPI transaction (no nested `EVAL`).
pub fn dispatch_from_lua(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    pubsub: Option<&PubSubHub>,
    conn: &mut ConnectionState,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    dispatch_single_non_multi(
        client,
        plans,
        cache,
        None,
        pubsub,
        None,
        conn,
        args,
        mutated_keys,
    )
}

fn dispatch_single_non_multi(
    client: &mut SpiClient,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    lua: Option<&LuaEngine>,
    pubsub: Option<&PubSubHub>,
    clients: Option<&ClientRegistry>,
    conn: &mut ConnectionState,
    args: &[Bytes],
    mutated_keys: &mut Vec<(i16, String)>,
) -> SpiResult<RespValue> {
    let cmd = cmd_name_upper(args);
    let db = conn.selected_db as i16;

    match cmd.as_str() {
        // Server / Protocol
        "PING" => Ok(server::cmd_ping(args)),
        "ECHO" => Ok(server::cmd_echo(args)),
        "QUIT" => {
            if let Some(hub) = pubsub {
                hub.unsubscribe_everything(conn.client_id);
            }
            Ok(server::cmd_quit(conn))
        }
        "RESET" => Ok(handle_reset_cmd(pubsub, clients, conn)),
        "SELECT" => Ok(server::cmd_select(conn, clients, args)),
        "HELLO" => Ok(server::cmd_hello(conn, clients, args)),
        "CLIENT" => Ok(server::cmd_client(conn, clients, pubsub, args)),
        "COMMAND" => Ok(server::cmd_command(args)),
        "INFO" => Ok(server::cmd_info(cache, clients)),
        "CONFIG" => Ok(server::cmd_config(args)),
        "TIME" => Ok(server::cmd_time()),
        "AUTH" => Ok(server::cmd_auth(conn, args)),

        // Pub/Sub (available inside Lua `redis.call` and `MULTI`/`EXEC`)
        "PUBLISH" => Ok(handle_publish_cmd(pubsub, args)),
        "PUBSUB" => Ok(handle_pubsub_cmd(pubsub, args)),

        // Lua 5.1 Scripting
        "SCRIPT" => Ok(handle_script_cmd(lua, args)),
        "EVAL" | "EVAL_RO" => Ok(handle_eval_cmd(
            false,
            client,
            plans,
            cache,
            lua,
            pubsub,
            conn,
            args,
            mutated_keys,
        )),
        "EVALSHA" | "EVALSHA_RO" => Ok(handle_eval_cmd(
            true,
            client,
            plans,
            cache,
            lua,
            pubsub,
            conn,
            args,
            mutated_keys,
        )),

        // Strings
        "GET" => strings::cmd_get(client, plans, cache, db, args),
        "GETEX" => strings::cmd_getex(client, plans, cache, db, args),
        "SET" => strings::cmd_set(client, plans, cache, db, args),
        "SETEX" => strings::cmd_setex(client, plans, cache, db, args, false),
        "PSETEX" => strings::cmd_setex(client, plans, cache, db, args, true),
        "SETNX" => strings::cmd_setnx(client, plans, cache, db, args),
        "GETSET" => strings::cmd_getset(client, plans, cache, db, args),
        "GETDEL" => strings::cmd_getdel(client, plans, cache, db, args),
        "MGET" => strings::cmd_mget(client, plans, cache, db, args),
        "MSET" => strings::cmd_mset(client, plans, cache, db, args),
        "INCR" => strings::cmd_incrby(client, plans, cache, db, args, Some(1), false),
        "DECR" => strings::cmd_incrby(client, plans, cache, db, args, Some(-1), false),
        "INCRBY" => strings::cmd_incrby(client, plans, cache, db, args, None, false),
        "DECRBY" => strings::cmd_incrby(client, plans, cache, db, args, None, true),
        "INCRBYFLOAT" => strings::cmd_incrbyfloat(client, plans, cache, db, args),
        "APPEND" => strings::cmd_append(client, plans, cache, db, args),
        "STRLEN" => strings::cmd_strlen(client, plans, cache, db, args),

        // Keys & Expiry
        "DEL" | "UNLINK" => keys::cmd_del(client, cache, db, args),
        "EXISTS" => keys::cmd_exists(client, db, args),
        "EXPIRE" => keys::cmd_expire(client, cache, db, args, false, false),
        "PEXPIRE" => keys::cmd_expire(client, cache, db, args, true, false),
        "EXPIREAT" => keys::cmd_expire(client, cache, db, args, false, true),
        "PEXPIREAT" => keys::cmd_expire(client, cache, db, args, true, true),
        "EXPIRETIME" => keys::cmd_expiretime(client, db, args, false),
        "PEXPIRETIME" => keys::cmd_expiretime(client, db, args, true),
        "TTL" => keys::cmd_ttl(client, db, args, false),
        "PTTL" => keys::cmd_ttl(client, db, args, true),
        "PERSIST" => keys::cmd_persist(client, cache, db, args),
        "TYPE" => keys::cmd_type(client, db, args),
        "RENAME" => keys::cmd_rename(client, cache, db, args, mutated_keys),
        "MOVE" => keys::cmd_move(client, cache, db, args, mutated_keys),
        "SWAPDB" => keys::cmd_swapdb(client, cache, args),
        "KEYS" => keys::cmd_keys(client, db, args),
        "SCAN" => keys::cmd_scan(client, db, args),
        "DBSIZE" => keys::cmd_dbsize(client, db),
        "FLUSHDB" => keys::cmd_flushdb(client, cache, db, args),
        "FLUSHALL" => keys::cmd_flushall(client, cache, args),

        // Hashes
        "HSET" => hashes::cmd_hset(client, plans, db, args, false),
        "HMSET" => hashes::cmd_hset(client, plans, db, args, true),
        "HSETNX" => hashes::cmd_hsetnx(client, db, args),
        "HGET" => hashes::cmd_hget(client, plans, db, args),
        "HMGET" => hashes::cmd_hmget(client, plans, db, args),
        "HGETALL" => hashes::cmd_hgetall(client, plans, db, args),
        "HDEL" => hashes::cmd_hdel(client, db, args),
        "HEXISTS" => hashes::cmd_hexists(client, db, args),
        "HLEN" => hashes::cmd_hlen(client, db, args),
        "HKEYS" => hashes::cmd_hkeys(client, db, args),
        "HVALS" => hashes::cmd_hvals(client, db, args),
        "HINCRBY" => hashes::cmd_hincrby(client, plans, db, args),
        "HINCRBYFLOAT" => hashes::cmd_hincrbyfloat(client, plans, db, args),
        "HSCAN" => hashes::cmd_hscan(client, db, args),

        // Lists
        "LPUSH" => lists::cmd_push(client, db, args, true, mutated_keys),
        "RPUSH" => lists::cmd_push(client, db, args, false, mutated_keys),
        "LPOP" => lists::cmd_pop(client, db, args, true),
        "RPOP" => lists::cmd_pop(client, db, args, false),
        "LRANGE" => lists::cmd_lrange(client, db, args),
        "LLEN" => lists::cmd_llen(client, db, args),
        "LINDEX" => lists::cmd_lindex(client, db, args),
        "LSET" => lists::cmd_lset(client, db, args),
        "LREM" => lists::cmd_lrem(client, db, args),
        "LTRIM" => lists::cmd_ltrim(client, db, args),
        "LPOS" => lists::cmd_lpos(client, db, args),
        "RPOPLPUSH" => lists::cmd_rpoplpush(client, db, args, mutated_keys),
        "LMOVE" => lists::cmd_lmove_cmd(client, db, args, mutated_keys),

        // Sorted Sets
        "ZADD" => zsets::cmd_zadd(client, plans, db, args, mutated_keys),
        "ZRANGE" => zsets::cmd_zrange(client, db, args, false),
        "ZREVRANGE" => zsets::cmd_zrange(client, db, args, true),
        "ZRANGEBYSCORE" => zsets::cmd_zrangebyscore(client, db, args, false),
        "ZREVRANGEBYSCORE" => zsets::cmd_zrangebyscore(client, db, args, true),
        "ZREM" => zsets::cmd_zrem(client, db, args),
        "ZREMRANGEBYRANK" => zsets::cmd_zremrangebyrank(client, db, args),
        "ZREMRANGEBYSCORE" => zsets::cmd_zremrangebyscore(client, db, args),
        "ZPOPMIN" => zsets::cmd_zpop(client, db, args, false),
        "ZPOPMAX" => zsets::cmd_zpop(client, db, args, true),
        "ZSCORE" => zsets::cmd_zscore(client, plans, db, args),
        "ZCARD" => zsets::cmd_zcard(client, db, args),
        "ZRANK" => zsets::cmd_zrank(client, db, args, false),
        "ZREVRANK" => zsets::cmd_zrank(client, db, args, true),
        "ZINCRBY" => zsets::cmd_zincrby(client, plans, db, args, mutated_keys),
        "ZCOUNT" => zsets::cmd_zcount(client, db, args),
        "ZSCAN" => zsets::cmd_zscan(client, db, args),

        // Sets
        "SADD" => sets::cmd_sadd(client, db, args),
        "SREM" => sets::cmd_srem(client, db, args),
        "SMEMBERS" => sets::cmd_smembers(client, db, args),
        "SISMEMBER" => sets::cmd_sismember(client, db, args),
        "SCARD" => sets::cmd_scard(client, db, args),
        "SPOP" => sets::cmd_spop(client, db, args),
        "SSCAN" => sets::cmd_sscan(client, db, args),

        // Streams
        "XADD" => streams::cmd_xadd(client, db, args, mutated_keys),
        "XTRIM" => streams::cmd_xtrim(client, db, args),
        "XLEN" => streams::cmd_xlen(client, db, args),
        "XDEL" => streams::cmd_xdel(client, db, args),
        "XRANGE" => streams::cmd_xrange(client, db, args, false),
        "XREVRANGE" => streams::cmd_xrange(client, db, args, true),
        "XREAD" => streams::cmd_xread_nonblocking(client, db, args),

        // Non-blocking fallback if called inside MULTI/Lua
        "BZPOPMIN" | "BZPOPMAX" => {
            if args.len() < 3 {
                return Ok(RespValue::wrong_arity(&cmd));
            }
            let keys: Vec<String> = args[1..args.len() - 1]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            Ok(zsets::try_bzpop_once(client, db, &keys, cmd == "BZPOPMAX")?
                .unwrap_or(RespValue::Null))
        }
        "BLPOP" | "BRPOP" => {
            if args.len() < 3 {
                return Ok(RespValue::wrong_arity(&cmd));
            }
            let keys: Vec<String> = args[1..args.len() - 1]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            Ok(lists::try_blpop_once(client, db, &keys, cmd == "BRPOP")?.unwrap_or(RespValue::Null))
        }

        _ => Ok(RespValue::err(format!(
            "ERR unknown command '{}'",
            cmd.to_lowercase()
        ))),
    }
}

/// Execute a pipelined batch of non-blocking commands inside the background worker.
/// Notifies any blocked clients AFTER the PostgreSQL transaction commits.
pub fn execute_batch_in_worker(
    batch: Vec<Vec<Bytes>>,
    plans: Option<&PreparedPlans>,
    cache: Option<&L1Cache>,
    lua: Option<&LuaEngine>,
    notifier: Option<&KeyNotifier>,
    pubsub: Option<&PubSubHub>,
    clients: Option<&ClientRegistry>,
    conn: &mut ConnectionState,
) -> Vec<(RespValue, RespVersion)> {
    let mut responses = Vec::with_capacity(batch.len());
    let mut idx = 0;

    while idx < batch.len() {
        // Drain any commands that can execute purely in memory (zero SPI overhead)
        while idx < batch.len() {
            if let Some(vals) =
                try_execute_in_memory_multi(&batch[idx], conn, cache, lua, pubsub, clients)
            {
                for val in vals {
                    responses.push((val, conn.protocol));
                }
                idx += 1;
                if conn.should_quit {
                    return responses;
                }
            } else {
                break;
            }
        }

        if idx >= batch.len() {
            break;
        }

        // Collect a contiguous SPI chunk, stopping before multi-frame Pub/Sub mode transitions
        // (`SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `RESET`) when outside `MULTI`
        // so they run via the multi-response in-memory path on the next iteration.
        let chunk_start = idx;
        let mut sim_in_multi = conn.in_multi;
        while idx < batch.len() {
            let cmd = cmd_name_upper(&batch[idx]);
            if !sim_in_multi
                && idx > chunk_start
                && matches!(
                    cmd.as_str(),
                    "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE" | "RESET"
                )
            {
                break;
            }
            match cmd.as_str() {
                "MULTI" => sim_in_multi = true,
                "EXEC" | "DISCARD" => sim_in_multi = false,
                _ => {}
            }
            idx += 1;
        }

        let chunk = &batch[chunk_start..idx];
        let mut conn_guard = AssertUnwindSafe(&mut *conn);
        let cache_guard = AssertUnwindSafe(cache);
        let plans_guard = AssertUnwindSafe(plans);
        let lua_guard = AssertUnwindSafe(lua);
        let pubsub_guard = AssertUnwindSafe(pubsub);
        let clients_guard = AssertUnwindSafe(clients);
        let chunk_guard = AssertUnwindSafe(chunk);

        let (tx_results, mutated_keys): (Vec<(RespValue, RespVersion)>, Vec<(i16, String)>) =
            BackgroundWorker::transaction(move || {
                Spi::connect(|mut client| {
                    let mut out = Vec::with_capacity(chunk_guard.len());
                    let mut mutated = Vec::new();
                    for args in *chunk_guard {
                        let val = match dispatch_with_spi(
                            &mut client,
                            *plans_guard,
                            *cache_guard,
                            *lua_guard,
                            *pubsub_guard,
                            *clients_guard,
                            *conn_guard,
                            args,
                            &mut mutated,
                        ) {
                            Ok(v) => v,
                            Err(e) => RespValue::err(format!("ERR internal database SPI error: {e}")),
                        };
                        out.push((val, conn_guard.protocol));
                        if conn_guard.should_quit {
                            break;
                        }
                    }
                    (out, mutated)
                })
            });

        // Notify blocked waiters AFTER transaction commit so new rows are 100% visible via MVCC
        if let Some(n) = notifier {
            n.notify_many(&mutated_keys);
        }

        responses.extend(tx_results);
        if conn.should_quit {
            return responses;
        }
    }

    responses
}
