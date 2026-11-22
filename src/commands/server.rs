use bytes::Bytes;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::cache::L1Cache;
use crate::commands::ConnectionState;
use crate::config;
use crate::protocol::{RespValue, RespVersion};

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

pub fn cmd_select(conn: &mut ConnectionState, args: &[Bytes]) -> RespValue {
    if args.len() != 2 {
        return RespValue::wrong_arity("select");
    }
    let Ok(db) = std::str::from_utf8(&args[1]).unwrap_or("").parse::<i64>() else {
        return RespValue::err("ERR value is not an integer or out of range");
    };
    if db != 0 {
        return RespValue::err("ERR DB index is out of range");
    }
    conn.selected_db = db;
    RespValue::ok()
}

/// Supports `HELLO [protover [AUTH user pass] [SETNAME clientname]]` for RESP2/RESP3 clients
/// such as `go-redis/v9`, `redis-py`, and `Jedis`.
pub fn cmd_hello(conn: &mut ConnectionState, args: &[Bytes]) -> RespValue {
    let mut proto = conn.protocol;

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
                    i += 3;
                }
                "SETNAME" => {
                    if i + 1 >= args.len() {
                        return RespValue::syntax_err();
                    }
                    conn.client_name = Some(as_str(&args[i + 1]));
                    i += 2;
                }
                _ => return RespValue::syntax_err(),
            }
        }
    }

    conn.protocol = proto;
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

pub fn cmd_client(conn: &mut ConnectionState, args: &[Bytes]) -> RespValue {
    if args.len() < 2 {
        return RespValue::wrong_arity("client");
    }
    let sub = as_str(&args[1]).to_uppercase();
    match sub.as_str() {
        "SETNAME" => {
            if args.len() != 3 {
                return RespValue::wrong_arity("client|setname");
            }
            conn.client_name = Some(as_str(&args[2]));
            RespValue::ok()
        }
        "GETNAME" => match &conn.client_name {
            Some(name) => RespValue::bulk(name.clone()),
            None => RespValue::Null,
        },
        "ID" => RespValue::Integer(conn.client_id as i64),
        "SETINFO" => {
            // Accepted from go-redis/v9, redis-py, Jedis (LIB-NAME / LIB-VER)
            if args.len() != 4 {
                return RespValue::wrong_arity("client|setinfo");
            }
            RespValue::ok()
        }
        "MAINT_NOTIFICATIONS" => RespValue::ok(),
        "NO-EVICT" | "NO-TOUCH" => RespValue::ok(),
        "INFO" | "LIST" => {
            let name = conn.client_name.as_deref().unwrap_or("");
            RespValue::bulk(format!("id={} name={} db=0\n", conn.client_id, name))
        }
        _ => RespValue::err(format!("ERR unknown subcommand '{sub}'. Try CLIENT HELP.")),
    }
}

pub fn cmd_command(args: &[Bytes]) -> RespValue {
    if args.len() >= 2 {
        let sub = as_str(&args[1]).to_uppercase();
        match sub.as_str() {
            "COUNT" => return RespValue::Integer(52),
            "DOCS" => return RespValue::Map(Vec::new()),
            "LIST" => {
                let cmds = [
                    "ping", "echo", "quit", "select", "hello", "client", "info", "config", "time",
                    "multi", "exec", "discard", "get", "set", "setex", "psetex", "setnx", "getset",
                    "getdel", "mget", "mset", "incr", "incrby", "decr", "decrby", "incrbyfloat",
                    "append", "strlen", "del", "unlink", "exists", "expire", "pexpire", "expireat",
                    "pexpireat", "ttl", "pttl", "persist", "type", "keys", "scan", "dbsize",
                    "flushdb", "flushall", "hset", "hmset", "hsetnx", "hget", "hmget", "hgetall",
                    "hdel", "hexists", "hlen", "hkeys", "hvals", "hincrby", "hincrbyfloat",
                    "lpush", "rpush", "lpop", "rpop", "lrange", "llen", "lindex", "lset", "lrem",
                    "zadd", "zrange", "zrevrange", "zrangebyscore", "zrevrangebyscore", "zrem",
                    "zscore", "zcard", "zrank", "zrevrank", "zincrby", "zcount",
                ];
                return RespValue::Array(cmds.into_iter().map(RespValue::bulk).collect());
            }
            _ => return RespValue::Array(Vec::new()),
        }
    }
    RespValue::Array(Vec::new())
}

pub fn cmd_info(cache: Option<&L1Cache>) -> RespValue {
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
         pg_database:{}\r\n\
         pg_unlogged:{}\r\n\
         \r\n\
         # Stats\r\n\
         {}\r\n\
         # Replication\r\n\
         role:master\r\n",
        env!("CARGO_PKG_VERSION"),
        config::port(),
        config::database(),
        if config::unlogged() { 1 } else { 0 },
        cache_section,
    );

    RespValue::bulk(info)
}

pub fn cmd_config(args: &[Bytes]) -> RespValue {
    if args.len() >= 3 && as_str(&args[1]).eq_ignore_ascii_case("GET") {
        let pat = as_str(&args[2]).to_lowercase();
        let all = [
            ("databases", "1".to_string()),
            ("save", "".to_string()),
            ("appendonly", "yes".to_string()),
            ("port", config::port().to_string()),
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
