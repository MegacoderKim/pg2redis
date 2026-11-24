use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::L1Cache;
use crate::clients::ClientRegistry;
use crate::commands::ConnectionState;
use crate::config;
use crate::protocol::{RespValue, RespVersion};
use crate::pubsub::PubSubHub;

fn as_str(b: &Bytes) -> String {
    String::from_utf8_lossy(b).into_owned()
}

pub fn cmd_ping(args: &[Bytes]) -> RespValue {
    match args.len() {
        1 => RespValue::pong(),
        2 => RespValue::BulkString(args[1].clone()),
        _ => RespValue::wrong_arity("ping"),
    }
}

pub fn cmd_echo(args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return RespValue::wrong_arity("echo");
    }
    RespValue::BulkString(args[1].clone())
}

pub fn cmd_quit(conn: &mut ConnectionState) -> RespValue {
    conn.should_quit = true;
    RespValue::ok()
}

pub fn cmd_auth(conn: &mut ConnectionState, args: &[Bytes]) -> RespValue {
    let (user, pass) = match args.len() {
        2 => (b"default".as_slice(), args[1].as_ref()),
        3 => (args[1].as_ref(), args[2].as_ref()),
        _ => return RespValue::wrong_arity("auth"),
    };

    let Some(expected) = config::requirepass() else {
        return RespValue::err(
            "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
        );
    };

    if user.eq_ignore_ascii_case(b"default")
        && config::constant_time_eq(pass, expected.as_bytes())
    {
        conn.authenticated = true;
        RespValue::ok()
    } else {
        RespValue::err("WRONGPASS invalid username-password pair or user is disabled.")
    }
}

pub fn cmd_select(
    conn: &mut ConnectionState,
    clients: Option<&ClientRegistry>,
    args: &[Bytes],
) -> RespValue {
    if args.len() != 2 {
        return RespValue::wrong_arity("select");
    }
    let Ok(db) = std::str::from_utf8(&args[1]).unwrap_or("").parse::<i64>() else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    if db < 0 || db >= config::databases() {
        return RespValue::err("ERR DB index is out of range");
    }
    conn.selected_db = db;
    if let Some(reg) = clients {
        reg.set_db(conn.client_id, db);
    }
    RespValue::ok()
}

/// Supports `HELLO [protover [AUTH user pass] [SETNAME clientname]]` for RESP2/RESP3 clients
/// such as `go-redis/v9`, `redis-py`, `ioredis`, and `Jedis`.
pub fn cmd_hello(
    conn: &mut ConnectionState,
    clients: Option<&ClientRegistry>,
    args: &[Bytes],
) -> RespValue {
    let mut proto = conn.protocol;
    let mut auth_creds: Option<(&[u8], &[u8])> = None;
    let mut new_name: Option<String> = None;

    if args.len() >= 2 {
        let ver_str = as_str(&args[1]);
        match ver_str.as_str() {
            "2" => proto = RespVersion::Resp2,
            "3" => proto = RespVersion::Resp3,
            _ => {
                return RespValue::err("NOPROTO unsupported protocol version");
            }
        }

        let mut i = 2;
        while i < args.len() {
            let opt = as_str(&args[i]).to_uppercase();
            match opt.as_str() {
                "AUTH" => {
                    if i + 2 >= args.len() {
                        return RespValue::syntax_err();
                    }
                    auth_creds = Some((args[i + 1].as_ref(), args[i + 2].as_ref()));
                    i += 3;
                }
                "SETNAME" => {
                    if i + 1 >= args.len() {
                        return RespValue::syntax_err();
                    }
                    new_name = Some(as_str(&args[i + 1]));
                    i += 2;
                }
                _ => return RespValue::syntax_err(),
            }
        }
    }

    if let Some(expected) = config::requirepass() {
        if let Some((user, pass)) = auth_creds {
            if user.eq_ignore_ascii_case(b"default")
                && config::constant_time_eq(pass, expected.as_bytes())
            {
                conn.authenticated = true;
            } else {
                return RespValue::err(
                    "WRONGPASS invalid username-password pair or user is disabled.",
                );
            }
        } else if !conn.authenticated {
            return RespValue::err(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time.",
            );
        }
    }

    conn.protocol = proto;
    if let Some(name) = new_name {
        let stored = if name.is_empty() { None } else { Some(name) };
        conn.client_name = stored.clone();
        if let Some(reg) = clients {
            reg.set_name(conn.client_id, stored);
        }
    }
    if let Some(reg) = clients {
        reg.set_proto(conn.client_id, proto);
    }

    let proto_num = match proto {
        RespVersion::Resp2 => 2,
        RespVersion::Resp3 => 3,
    };

    RespValue::Map(vec![
        (RespValue::bulk("server"), RespValue::bulk("pg_redis_core")),
        (RespValue::bulk("version"), RespValue::bulk("7.2.0")),
        (RespValue::bulk("proto"), RespValue::Integer(proto_num)),
        (
            RespValue::bulk("id"),
            RespValue::Integer(conn.client_id as i64),
        ),
        (RespValue::bulk("mode"), RespValue::bulk("standalone")),
        (RespValue::bulk("role"), RespValue::bulk("master")),
        (RespValue::bulk("modules"), RespValue::Array(Vec::new())),
    ])
}

pub fn cmd_client(
    conn: &mut ConnectionState,
    clients: Option<&ClientRegistry>,
    pubsub: Option<&PubSubHub>,
    args: &[Bytes],
) -> RespValue {
    if args.len() < 2 {
        return RespValue::wrong_arity("client");
    }
    let sub = as_str(&args[1]).to_uppercase();
    match sub.as_str() {
        "SETNAME" => {
            if args.len() != 3 {
                return RespValue::wrong_arity("client|setname");
            }
            let raw = as_str(&args[2]);
            if raw.chars().any(|c| c.is_whitespace()) {
                return RespValue::err(
                    "ERR Client names cannot contain spaces, newlines or special characters.",
                );
            }
            let stored = if raw.is_empty() { None } else { Some(raw) };
            conn.client_name = stored.clone();
            if let Some(reg) = clients {
                reg.set_name(conn.client_id, stored);
            }
            RespValue::ok()
        }
        "GETNAME" => match &conn.client_name {
            Some(name) => RespValue::bulk(name.clone()),
            None => RespValue::Null,
        },
        "ID" => RespValue::Integer(conn.client_id as i64),
        "SETINFO" => {
            // Accepted from go-redis/v9, redis-py, ioredis, Jedis (LIB-NAME / LIB-VER)
            if args.len() != 4 {
                return RespValue::wrong_arity("client|setinfo");
            }
            RespValue::ok()
        }
        "MAINT_NOTIFICATIONS" => RespValue::ok(),
        "NO-EVICT" | "NO-TOUCH" => RespValue::ok(),
        "INFO" => {
            if let Some(reg) = clients {
                if let Some(line) = reg.format_one(conn.client_id, pubsub) {
                    return RespValue::bulk(line);
                }
            }
            let name = conn.client_name.as_deref().unwrap_or("");
            RespValue::bulk(format!("id={} name={} db=0\n", conn.client_id, name))
        }
        "LIST" => {
            let mut filter_type: Option<String> = None;
            let mut filter_ids: Option<Vec<u64>> = None;

            let mut i = 2;
            while i < args.len() {
                let opt = as_str(&args[i]).to_uppercase();
                match opt.as_str() {
                    "TYPE" => {
                        if i + 1 >= args.len() {
                            return RespValue::syntax_err();
                        }
                        let ty = as_str(&args[i + 1]).to_ascii_lowercase();
                        if !matches!(
                            ty.as_str(),
                            "normal" | "master" | "replica" | "slave" | "pubsub"
                        ) {
                            return RespValue::err(format!("ERR Unknown client type '{ty}'"));
                        }
                        filter_type = Some(ty);
                        i += 2;
                    }
                    "ID" => {
                        if i + 1 >= args.len() {
                            return RespValue::syntax_err();
                        }
                        i += 1;
                        let mut ids = Vec::new();
                        while i < args.len() {
                            let Ok(id) = std::str::from_utf8(&args[i]).unwrap_or("").parse::<u64>()
                            else {
                                return RespValue::not_integer_err();
                            };
                            ids.push(id);
                            i += 1;
                        }
                        filter_ids = Some(ids);
                    }
                    _ => return RespValue::syntax_err(),
                }
            }

            if let Some(reg) = clients {
                RespValue::bulk(reg.format_list(
                    filter_ids.as_deref(),
                    filter_type.as_deref(),
                    pubsub,
                ))
            } else {
                let name = conn.client_name.as_deref().unwrap_or("");
                RespValue::bulk(format!("id={} name={} db=0\n", conn.client_id, name))
            }
        }
        "KILL" => {
            let Some(reg) = clients else {
                return RespValue::Integer(0);
            };
            if args.len() == 3 {
                // Legacy `CLIENT KILL ip:port`
                let addr = as_str(&args[2]);
                return if reg.kill_by_addr(&addr) > 0 {
                    RespValue::ok()
                } else {
                    RespValue::err("ERR No such client")
                };
            }
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return RespValue::syntax_err();
            }
            let mut killed: i64 = 0;
            let mut i = 2;
            while i < args.len() {
                let opt = as_str(&args[i]).to_uppercase();
                match opt.as_str() {
                    "ID" => {
                        let Ok(id) = std::str::from_utf8(&args[i + 1]).unwrap_or("").parse::<u64>()
                        else {
                            return RespValue::not_integer_err();
                        };
                        killed += reg.kill_by_id(id);
                    }
                    "ADDR" => {
                        let addr = as_str(&args[i + 1]);
                        killed += reg.kill_by_addr(&addr);
                    }
                    _ => return RespValue::syntax_err(),
                }
                i += 2;
            }
            RespValue::Integer(killed)
        }
        _ => RespValue::err(format!("ERR unknown subcommand '{sub}'. Try CLIENT HELP.")),
    }
}

pub fn cmd_command(args: &[Bytes]) -> RespValue {
    const CMDS: &[&str] = &[
        "ping", "echo", "quit", "reset", "select", "hello", "client", "command", "info", "config",
        "time", "auth", "multi", "exec", "discard", "publish", "subscribe", "unsubscribe",
        "psubscribe", "punsubscribe", "pubsub", "eval", "eval_ro", "evalsha", "evalsha_ro",
        "script", "get", "getex", "set", "setex", "psetex", "setnx", "getset", "getdel", "mget",
        "mset", "incr", "incrby", "decr", "decrby", "incrbyfloat", "append", "strlen", "del",
        "unlink", "exists", "expire", "pexpire", "expireat", "pexpireat", "expiretime",
        "pexpiretime", "ttl", "pttl", "persist", "type", "rename", "move", "swapdb", "keys", "scan",
        "dbsize", "flushdb", "flushall", "hset", "hmset", "hsetnx", "hget", "hmget", "hgetall",
        "hdel", "hexists", "hlen", "hkeys", "hvals", "hincrby", "hincrbyfloat", "hscan", "lpush",
        "rpush",
        "lpop", "rpop", "lrange", "llen", "lindex", "lset", "lrem", "ltrim", "lpos", "rpoplpush",
        "lmove", "blpop", "brpop", "zadd", "zrange", "zrevrange", "zrangebyscore",
        "zrevrangebyscore", "zrem", "zremrangebyrank", "zremrangebyscore", "zpopmin", "zpopmax",
        "bzpopmin", "bzpopmax", "zscore", "zcard", "zrank", "zrevrank", "zincrby", "zcount",
        "zscan", "sadd", "srem", "smembers", "sismember", "scard", "spop", "sscan", "xadd",
        "xtrim", "xlen", "xdel", "xrange", "xrevrange", "xread",
    ];
    if args.len() >= 2 {
        let sub = as_str(&args[1]).to_uppercase();
        match sub.as_str() {
            "COUNT" => return RespValue::Integer(CMDS.len() as i64),
            "DOCS" => return RespValue::Map(Vec::new()),
            "LIST" => {
                return RespValue::Array(CMDS.iter().copied().map(RespValue::bulk).collect());
            }
            _ => return RespValue::Array(Vec::new()),
        }
    }
    RespValue::Array(Vec::new())
}

pub fn cmd_info(cache: Option<&L1Cache>, clients: Option<&ClientRegistry>) -> RespValue {
    let connected_clients = clients.map(ClientRegistry::count).unwrap_or(0);
    let cache_section = if let Some(c) = cache {
        let st = c.stats();
        format!(
            "l1_cache_enabled:{}\r\nl1_cache_hits:{}\r\nl1_cache_misses:{}\r\nl1_cache_invalidations:{}\r\nl1_cache_entries:{}\r\n",
            if config::cache_enabled() { 1 } else { 0 },
            st.hits,
            st.misses,
            st.invalidations,
            st.entry_count
        )
    } else {
        "l1_cache_enabled:0\r\n".to_string()
    };

    let info = format!(
        "# Server\r\n\
         redis_version:7.2.0\r\n\
         pg_redis_core_version:{}\r\n\
         tcp_port:{}\r\n\
         tls_port:{}\r\n\
         databases:{}\r\n\
         pg_database:{}\r\n\
         pg_unlogged:{}\r\n\
         \r\n\
         # Clients\r\n\
         connected_clients:{}\r\n\
         maxclients:{}\r\n\
         \r\n\
         # Memory\r\n\
         maxmemory_policy:noeviction\r\n\
         \r\n\
         # Stats\r\n\
         {}\r\n\
         # Replication\r\n\
         role:master\r\n",
        env!("CARGO_PKG_VERSION"),
        config::port(),
        config::tls_port(),
        config::databases(),
        config::database(),
        if config::unlogged() { 1 } else { 0 },
        connected_clients,
        config::max_clients(),
        cache_section,
    );

    RespValue::bulk(info)
}

pub fn cmd_config(args: &[Bytes]) -> RespValue {
    if args.len() >= 3 && as_str(&args[1]).eq_ignore_ascii_case("GET") {
        let pat = as_str(&args[2]).to_lowercase();
        let all = [
            ("databases", config::databases().to_string()),
            ("save", "".to_string()),
            ("appendonly", "yes".to_string()),
            ("maxmemory-policy", "noeviction".to_string()),
            ("maxclients", config::max_clients().to_string()),
            ("lua-time-limit", config::lua_time_limit_ms().to_string()),
            ("port", config::port().to_string()),
            ("tls-port", config::tls_port().to_string()),
            ("tls-auth-clients", config::tls_auth_clients()),
        ];
        let mut pairs = Vec::new();
        for (k, v) in all {
            if crate::commands::keys::glob_match(&pat, k) {
                pairs.push((RespValue::bulk(k), RespValue::bulk(v)));
            }
        }
        return RespValue::Map(pairs);
    }
    RespValue::ok()
}

pub fn cmd_time() -> RespValue {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    RespValue::Array(vec![
        RespValue::bulk(dur.as_secs().to_string()),
        RespValue::bulk(dur.subsec_micros().to_string()),
    ])
}
