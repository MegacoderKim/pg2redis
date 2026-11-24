use bytes::Bytes;
use mlua::{
    Function, HookTriggers, LightUserData, Lua, LuaOptions, MultiValue, RegistryKey,
    Result as LuaResult, StdLib, Table, Value as LuaValue, VmState,
};
use pgrx::spi::SpiClient;
use rmpv::Value as MsgPackValue;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::io::Cursor;
use std::time::{Duration, Instant};

use crate::cache::L1Cache;
use crate::commands::{dispatch_from_lua, ConnectionState};
use crate::config;
use crate::protocol::{format_score, RespValue};
use crate::pubsub::PubSubHub;
use crate::storage::PreparedPlans;

static CJSON_NULL_SENTINEL: u8 = 0;

fn cjson_null_value() -> LuaValue {
    LuaValue::LightUserData(LightUserData(
        &CJSON_NULL_SENTINEL as *const u8 as *mut c_void,
    ))
}

fn is_cjson_null(v: &LuaValue) -> bool {
    matches!(
        v,
        LuaValue::LightUserData(LightUserData(ptr))
            if *ptr == (&CJSON_NULL_SENTINEL as *const u8 as *mut c_void)
    )
}

pub fn sha1_hex(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(40);
    for b in digest {
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Embedded Redis-compatible Lua 5.1 engine (`mlua` vendored Lua 5.1).
///
/// Provides SHA1-cached compiled script execution (`EVAL`, `EVALSHA`, `SCRIPT LOAD/EXISTS/FLUSH`),
/// exact Redis Lua <-> RESP conversions, `cjson`, `cmsgpack`, and `bit` libraries.
pub struct LuaEngine {
    lua: Lua,
    scripts: RefCell<HashMap<String, RegistryKey>>,
}

impl LuaEngine {
    pub fn new() -> Result<Self, String> {
        let safe_libs = StdLib::TABLE | StdLib::STRING | StdLib::MATH;
        let lua =
            Lua::new_with(safe_libs, LuaOptions::default()).map_err(|e| e.to_string())?;
        sandbox_lua_globals(&lua).map_err(|e| e.to_string())?;
        install_redis_globals(&lua).map_err(|e| e.to_string())?;
        Ok(Self {
            lua,
            scripts: RefCell::new(HashMap::new()),
        })
    }

    pub fn script_load(&self, source: &[u8]) -> Result<String, String> {
        let sha = sha1_hex(source);
        if self.scripts.borrow().contains_key(&sha) {
            return Ok(sha);
        }
        let func = self
            .lua
            .load(source)
            .set_name(format!("@user_script:{sha}"))
            .into_function()
            .map_err(|e| format!("ERR Error compiling script (new function): {e}"))?;
        let key = self
            .lua
            .create_registry_value(func)
            .map_err(|e| format!("ERR Error caching script: {e}"))?;
        self.scripts.borrow_mut().insert(sha.clone(), key);
        Ok(sha)
    }

    pub fn script_exists(&self, shas: &[String]) -> Vec<i64> {
        let cache = self.scripts.borrow();
        shas.iter()
            .map(|sha| {
                if cache.contains_key(&sha.to_ascii_lowercase()) {
                    1
                } else {
                    0
                }
            })
            .collect()
    }

    pub fn script_flush(&self) {
        let mut cache = self.scripts.borrow_mut();
        for (_, reg_key) in cache.drain() {
            let _ = self.lua.remove_registry_value(reg_key);
        }
    }

    pub fn eval(
        &self,
        source: &[u8],
        keys: &[Bytes],
        argv: &[Bytes],
        client: &mut SpiClient,
        plans: Option<&PreparedPlans>,
        cache: Option<&L1Cache>,
        pubsub: Option<&PubSubHub>,
        conn: &mut ConnectionState,
        mutated_keys: &mut Vec<(i16, String)>,
    ) -> RespValue {
        let sha = match self.script_load(source) {
            Ok(s) => s,
            Err(e) => return RespValue::err(e),
        };
        self.evalsha(
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
    }

    pub fn evalsha(
        &self,
        sha: &str,
        keys: &[Bytes],
        argv: &[Bytes],
        client: &mut SpiClient,
        plans: Option<&PreparedPlans>,
        cache: Option<&L1Cache>,
        pubsub: Option<&PubSubHub>,
        conn: &mut ConnectionState,
        mutated_keys: &mut Vec<(i16, String)>,
    ) -> RespValue {
        let sha_lower = sha.to_ascii_lowercase();
        let func: Function = {
            let scripts = self.scripts.borrow();
            let Some(reg_key) = scripts.get(&sha_lower) else {
                return RespValue::err("NOSCRIPT No matching script. Please use EVAL.");
            };
            match self.lua.registry_value::<Function>(reg_key) {
                Ok(f) => f,
                Err(e) => return RespValue::err(format!("ERR Error loading cached script: {e}")),
            }
        };

        // Populate global KEYS and ARGV tables (1-indexed, binary-safe)
        let globals = self.lua.globals();
        if let Err(e) = set_bytes_array(&self.lua, &globals, "KEYS", keys) {
            return RespValue::err(format!("ERR Lua KEYS init error: {e}"));
        }
        if let Err(e) = set_bytes_array(&self.lua, &globals, "ARGV", argv) {
            return RespValue::err(format!("ERR Lua ARGV init error: {e}"));
        }

        struct BridgeCtx<'a, 'conn> {
            client: &'a mut SpiClient<'conn>,
            plans: Option<&'a PreparedPlans>,
            cache: Option<&'a L1Cache>,
            pubsub: Option<&'a PubSubHub>,
            conn: &'a mut ConnectionState,
            mutated_keys: &'a mut Vec<(i16, String)>,
        }

        let ctx_cell = RefCell::new(BridgeCtx {
            client,
            plans,
            cache,
            pubsub,
            conn,
            mutated_keys,
        });

        struct HookGuard<'a>(&'a Lua);
        impl Drop for HookGuard<'_> {
            fn drop(&mut self) {
                self.0.remove_hook();
            }
        }

        let limit_ms = config::lua_time_limit_ms();
        let _hook_guard = if limit_ms > 0 {
            let deadline = Instant::now() + Duration::from_millis(limit_ms);
            self.lua.set_hook(
                HookTriggers::new().every_nth_instruction(10_000),
                move |_lua, _debug| {
                    if Instant::now() >= deadline {
                        Err(mlua::Error::RuntimeError(
                            "ERR Lua script exceeded time limit".to_string(),
                        ))
                    } else {
                        Ok(VmState::Continue)
                    }
                },
            );
            Some(HookGuard(&self.lua))
        } else {
            None
        };

        let exec_result: LuaResult<RespValue> = self.lua.scope(|scope| {
            let redis_tbl: Table = globals.get("redis")?;

            let make_caller = |raise_on_err: bool| {
                let ctx_ref = &ctx_cell;
                scope.create_function(move |lua, args: MultiValue| {
                    let cmd_args = lua_args_to_bytes(args)?;
                    if cmd_args.is_empty() {
                        return Err(mlua::Error::RuntimeError(
                            "Please specify at least one argument for redis.call()".to_string(),
                        ));
                    }

                    let resp = {
                        let mut ctx_guard = ctx_ref.try_borrow_mut().map_err(|_| {
                            mlua::Error::RuntimeError(
                                "ERR recursive redis.call is not supported".to_string(),
                            )
                        })?;
                        let ctx = &mut *ctx_guard;
                        match dispatch_from_lua(
                            ctx.client,
                            ctx.plans,
                            ctx.cache,
                            ctx.pubsub,
                            ctx.conn,
                            &cmd_args,
                            ctx.mutated_keys,
                        ) {
                            Ok(r) => r,
                            Err(e) => RespValue::err(format!("ERR SPI error: {e}")),
                        }
                    };

                    match resp {
                        RespValue::Error(msg) => {
                            if raise_on_err {
                                Err(mlua::Error::RuntimeError(msg))
                            } else {
                                let t = lua.create_table()?;
                                t.raw_set("err", msg)?;
                                Ok(LuaValue::Table(t))
                            }
                        }
                        other => resp_to_lua(lua, other),
                    }
                })
            };

            let call_fn = make_caller(true)?;
            let pcall_fn = make_caller(false)?;
            redis_tbl.raw_set("call", call_fn)?;
            redis_tbl.raw_set("pcall", pcall_fn)?;

            let lua_ret: LuaValue = func.call(())?;
            let resp_ret = lua_to_resp(lua_ret)?;

            let _ = redis_tbl.raw_set("call", LuaValue::Nil);
            let _ = redis_tbl.raw_set("pcall", LuaValue::Nil);

            Ok(resp_ret)
        });

        match exec_result {
            Ok(v) => v,
            Err(e) => {
                let msg = clean_lua_error(&e);
                RespValue::err(msg)
            }
        }
    }
}

fn clean_lua_error(err: &mlua::Error) -> String {
    let raw = match err {
        mlua::Error::CallbackError { cause, .. } => return clean_lua_error(cause),
        mlua::Error::RuntimeError(s) => s.clone(),
        other => other.to_string(),
    };
    if raw.starts_with("ERR ") || raw.starts_with("WRONGTYPE ") || raw.starts_with("NOSCRIPT ") {
        raw
    } else {
        format!("ERR Error running script: {raw}")
    }
}

fn set_bytes_array(lua: &Lua, globals: &Table, name: &str, items: &[Bytes]) -> LuaResult<()> {
    let tbl = lua.create_table_with_capacity(items.len(), 0)?;
    for (idx, item) in items.iter().enumerate() {
        let s = lua.create_string(item.as_ref())?;
        tbl.raw_set((idx + 1) as i64, s)?;
    }
    globals.raw_set(name, tbl)?;
    Ok(())
}

fn lua_args_to_bytes(args: MultiValue) -> LuaResult<Vec<Bytes>> {
    let mut out = Vec::with_capacity(args.len());
    for v in args {
        match v {
            LuaValue::String(s) => out.push(Bytes::copy_from_slice(s.as_bytes().as_ref())),
            LuaValue::Integer(n) => out.push(Bytes::from(n.to_string())),
            LuaValue::Number(f) => {
                if f.fract() == 0.0 && f.abs() < 1e15 {
                    out.push(Bytes::from((f as i64).to_string()));
                } else {
                    out.push(Bytes::from(format_score(f)));
                }
            }
            LuaValue::Boolean(b) => {
                out.push(Bytes::from(if b { "1" } else { "0" }));
            }
            LuaValue::Nil => {
                return Err(mlua::Error::RuntimeError(
                    "Lua redis() command arguments must be strings or integers".to_string(),
                ));
            }
            _ => {
                return Err(mlua::Error::RuntimeError(
                    "Lua redis() command arguments must be strings or integers".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

/// Convert a Redis response (`RespValue`) into its exact Redis-Lua representation.
fn resp_to_lua(lua: &Lua, value: RespValue) -> LuaResult<LuaValue> {
    match value {
        RespValue::Null => Ok(LuaValue::Boolean(false)),
        RespValue::Integer(n) => Ok(LuaValue::Integer(n)),
        RespValue::BulkString(b) => Ok(LuaValue::String(lua.create_string(b.as_ref())?)),
        RespValue::Double(d) => Ok(LuaValue::String(lua.create_string(format_score(d))?)),
        RespValue::SimpleString(s) => {
            let t = lua.create_table()?;
            t.raw_set("ok", lua.create_string(s.as_ref())?)?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Error(e) => {
            let t = lua.create_table()?;
            t.raw_set("err", e)?;
            Ok(LuaValue::Table(t))
        }
        RespValue::Array(items) | RespValue::Push(items) => {
            let t = lua.create_table_with_capacity(items.len(), 0)?;
            for (i, item) in items.into_iter().enumerate() {
                t.raw_set((i + 1) as i64, resp_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::Map(pairs) => {
            let t = lua.create_table_with_capacity(pairs.len() * 2, 0)?;
            let mut idx: i64 = 1;
            for (k, v) in pairs {
                t.raw_set(idx, resp_to_lua(lua, k)?)?;
                idx += 1;
                t.raw_set(idx, resp_to_lua(lua, v)?)?;
                idx += 1;
            }
            Ok(LuaValue::Table(t))
        }
        RespValue::ScoredArray(pairs) => {
            let t = lua.create_table_with_capacity(pairs.len() * 2, 0)?;
            let mut idx: i64 = 1;
            for (member, score) in pairs {
                t.raw_set(idx, lua.create_string(member.as_ref())?)?;
                idx += 1;
                t.raw_set(idx, lua.create_string(format_score(score))?)?;
                idx += 1;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

/// Convert a Lua return value into its exact Redis RESP representation.
fn lua_to_resp(value: LuaValue) -> LuaResult<RespValue> {
    match value {
        LuaValue::Nil | LuaValue::Boolean(false) => Ok(RespValue::Null),
        LuaValue::Boolean(true) => Ok(RespValue::Integer(1)),
        LuaValue::Integer(n) => Ok(RespValue::Integer(n)),
        LuaValue::Number(f) => Ok(RespValue::Integer(f.trunc() as i64)),
        LuaValue::String(s) => Ok(RespValue::BulkString(Bytes::copy_from_slice(
            s.as_bytes().as_ref(),
        ))),
        LuaValue::Table(t) => {
            if let Ok(LuaValue::String(err_s)) = t.raw_get::<LuaValue>("err") {
                return Ok(RespValue::Error(
                    String::from_utf8_lossy(err_s.as_bytes().as_ref()).into_owned(),
                ));
            }
            if let Ok(LuaValue::String(ok_s)) = t.raw_get::<LuaValue>("ok") {
                return Ok(RespValue::SimpleString(Bytes::copy_from_slice(
                    ok_s.as_bytes().as_ref(),
                )));
            }
            let mut arr = Vec::new();
            let mut idx: i64 = 1;
            loop {
                let elem: LuaValue = t.raw_get(idx)?;
                if matches!(elem, LuaValue::Nil) {
                    break;
                }
                arr.push(lua_to_resp(elem)?);
                idx += 1;
            }
            Ok(RespValue::Array(arr))
        }
        _ => Ok(RespValue::Null),
    }
}

fn sandbox_lua_globals(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();
    for forbidden in [
        "os",
        "io",
        "package",
        "debug",
        "dofile",
        "loadfile",
        "collectgarbage",
        "module",
        "require",
    ] {
        globals.raw_set(forbidden, LuaValue::Nil)?;
    }
    Ok(())
}

fn install_redis_globals(lua: &Lua) -> LuaResult<()> {
    let globals = lua.globals();

    // 1. `redis` table
    let redis_tbl = lua.create_table()?;
    redis_tbl.raw_set(
        "status_reply",
        lua.create_function(|lua, msg: mlua::String| {
            let t = lua.create_table()?;
            t.raw_set("ok", msg)?;
            Ok(t)
        })?,
    )?;
    redis_tbl.raw_set(
        "error_reply",
        lua.create_function(|lua, msg: mlua::String| {
            let t = lua.create_table()?;
            t.raw_set("err", msg)?;
            Ok(t)
        })?,
    )?;
    redis_tbl.raw_set(
        "sha1hex",
        lua.create_function(|_, data: mlua::String| Ok(sha1_hex(data.as_bytes().as_ref())))?,
    )?;
    redis_tbl.raw_set("LOG_DEBUG", 0i64)?;
    redis_tbl.raw_set("LOG_VERBOSE", 1i64)?;
    redis_tbl.raw_set("LOG_NOTICE", 2i64)?;
    redis_tbl.raw_set("LOG_WARNING", 3i64)?;
    redis_tbl.raw_set(
        "log",
        lua.create_function(|_, (_level, msg): (i64, String)| {
            pgrx::debug1!("pg_redis_core [lua]: {}", msg);
            Ok(())
        })?,
    )?;
    redis_tbl.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    globals.raw_set("redis", redis_tbl)?;

    // 2. `cjson` library
    let cjson_tbl = lua.create_table()?;
    cjson_tbl.raw_set("null", cjson_null_value())?;
    cjson_tbl.raw_set(
        "encode",
        lua.create_function(|_, val: LuaValue| {
            let jv = lua_to_json(val)?;
            serde_json::to_string(&jv)
                .map_err(|e| mlua::Error::RuntimeError(format!("cjson.encode error: {e}")))
        })?,
    )?;
    cjson_tbl.raw_set(
        "decode",
        lua.create_function(|lua, s: mlua::String| {
            let jv: JsonValue = serde_json::from_slice(s.as_bytes().as_ref())
                .map_err(|e| mlua::Error::RuntimeError(format!("cjson.decode error: {e}")))?;
            json_to_lua(lua, jv)
        })?,
    )?;
    globals.raw_set("cjson", cjson_tbl)?;

    // 3. `cmsgpack` library (required by BullMQ 5.x for job args & options)
    let cmsgpack_tbl = lua.create_table()?;
    cmsgpack_tbl.raw_set(
        "pack",
        lua.create_function(|lua, args: MultiValue| {
            let mut buf = Vec::new();
            for val in args {
                let mp = lua_to_msgpack(val)?;
                rmpv::encode::write_value(&mut buf, &mp).map_err(|e| {
                    mlua::Error::RuntimeError(format!("cmsgpack.pack error: {e}"))
                })?;
            }
            lua.create_string(&buf)
        })?,
    )?;
    cmsgpack_tbl.raw_set(
        "unpack",
        lua.create_function(|lua, data: mlua::String| {
            let raw = data.as_bytes();
            let slice: &[u8] = raw.as_ref();
            let mut cursor = Cursor::new(slice);
            let mut results = MultiValue::new();
            while (cursor.position() as usize) < slice.len() {
                let mp = rmpv::decode::read_value(&mut cursor).map_err(|e| {
                    mlua::Error::RuntimeError(format!("cmsgpack.unpack error: {e}"))
                })?;
                results.push_back(msgpack_to_lua(lua, mp)?);
            }
            if results.is_empty() {
                results.push_back(LuaValue::Nil);
            }
            Ok(results)
        })?,
    )?;
    globals.raw_set("cmsgpack", cmsgpack_tbl)?;

    // 4. `bit` library (LuaBitOp compatible)
    install_bit_lib(lua, &globals)?;

    Ok(())
}

fn lua_to_json(value: LuaValue) -> LuaResult<JsonValue> {
    if is_cjson_null(&value) {
        return Ok(JsonValue::Null);
    }
    match value {
        LuaValue::Nil => Ok(JsonValue::Null),
        LuaValue::Boolean(b) => Ok(JsonValue::Bool(b)),
        LuaValue::Integer(n) => Ok(JsonValue::Number(JsonNumber::from(n))),
        LuaValue::Number(f) => {
            if f.fract() == 0.0 && f.abs() < 1e15 {
                Ok(JsonValue::Number(JsonNumber::from(f as i64)))
            } else {
                JsonNumber::from_f64(f)
                    .map(JsonValue::Number)
                    .ok_or_else(|| {
                        mlua::Error::RuntimeError("Cannot serialise NaN/Inf to JSON".to_string())
                    })
            }
        }
        LuaValue::String(s) => Ok(JsonValue::String(
            String::from_utf8_lossy(s.as_bytes().as_ref()).into_owned(),
        )),
        LuaValue::Table(t) => {
            let mut max_idx: i64 = 0;
            let mut count: i64 = 0;
            let mut all_positive_ints = true;
            for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                let (k, _) = pair?;
                count += 1;
                match k {
                    LuaValue::Integer(i) if i >= 1 => {
                        if i > max_idx {
                            max_idx = i;
                        }
                    }
                    LuaValue::Number(f) if f >= 1.0 && f.fract() == 0.0 => {
                        let i = f as i64;
                        if i > max_idx {
                            max_idx = i;
                        }
                    }
                    _ => {
                        all_positive_ints = false;
                    }
                }
            }

            if count > 0 && all_positive_ints && max_idx == count {
                let mut arr = Vec::with_capacity(count as usize);
                for idx in 1..=count {
                    let elem: LuaValue = t.raw_get(idx)?;
                    arr.push(lua_to_json(elem)?);
                }
                Ok(JsonValue::Array(arr))
            } else {
                let mut map = JsonMap::new();
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (k, v) = pair?;
                    let key_str = match k {
                        LuaValue::String(s) => {
                            String::from_utf8_lossy(s.as_bytes().as_ref()).into_owned()
                        }
                        LuaValue::Integer(i) => i.to_string(),
                        LuaValue::Number(f) => format_score(f),
                        _ => continue,
                    };
                    map.insert(key_str, lua_to_json(v)?);
                }
                Ok(JsonValue::Object(map))
            }
        }
        _ => Ok(JsonValue::Null),
    }
}

fn json_to_lua(lua: &Lua, val: JsonValue) -> LuaResult<LuaValue> {
    match val {
        JsonValue::Null => Ok(cjson_null_value()),
        JsonValue::Bool(b) => Ok(LuaValue::Boolean(b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(LuaValue::Integer(i))
            } else if let Some(u) = n.as_u64() {
                Ok(LuaValue::Number(u as f64))
            } else {
                Ok(LuaValue::Number(n.as_f64().unwrap_or(0.0)))
            }
        }
        JsonValue::String(s) => Ok(LuaValue::String(lua.create_string(&s)?)),
        JsonValue::Array(arr) => {
            let t = lua.create_table_with_capacity(arr.len(), 0)?;
            for (idx, item) in arr.into_iter().enumerate() {
                t.raw_set((idx + 1) as i64, json_to_lua(lua, item)?)?;
            }
            Ok(LuaValue::Table(t))
        }
        JsonValue::Object(map) => {
            let t = lua.create_table_with_capacity(0, map.len())?;
            for (k, v) in map {
                t.raw_set(k, json_to_lua(lua, v)?)?;
            }
            Ok(LuaValue::Table(t))
        }
    }
}

fn lua_to_msgpack(value: LuaValue) -> LuaResult<MsgPackValue> {
    if is_cjson_null(&value) {
        return Ok(MsgPackValue::Nil);
    }
    match value {
        LuaValue::Nil => Ok(MsgPackValue::Nil),
        LuaValue::Boolean(b) => Ok(MsgPackValue::Boolean(b)),
        LuaValue::Integer(n) => Ok(MsgPackValue::from(n)),
        LuaValue::Number(f) => {
            if f.fract() == 0.0 && f.abs() < 1e15 {
                Ok(MsgPackValue::from(f as i64))
            } else {
                Ok(MsgPackValue::F64(f))
            }
        }
        LuaValue::String(s) => {
            let raw = s.as_bytes();
            let slice: &[u8] = raw.as_ref();
            match std::str::from_utf8(slice) {
                Ok(utf8) => Ok(MsgPackValue::from(utf8)),
                Err(_) => Ok(MsgPackValue::Binary(slice.to_vec())),
            }
        }
        LuaValue::Table(t) => {
            let mut max_idx: i64 = 0;
            let mut count: i64 = 0;
            let mut all_positive_ints = true;
            for pair in t.clone().pairs::<LuaValue, LuaValue>() {
                let (k, _) = pair?;
                count += 1;
                match k {
                    LuaValue::Integer(i) if i >= 1 => {
                        if i > max_idx {
                            max_idx = i;
                        }
                    }
                    LuaValue::Number(f) if f >= 1.0 && f.fract() == 0.0 => {
                        let i = f as i64;
                        if i > max_idx {
                            max_idx = i;
                        }
                    }
                    _ => all_positive_ints = false,
                }
            }

            if count > 0 && all_positive_ints && max_idx == count {
                let mut arr = Vec::with_capacity(count as usize);
                for idx in 1..=count {
                    let elem: LuaValue = t.raw_get(idx)?;
                    arr.push(lua_to_msgpack(elem)?);
                }
                Ok(MsgPackValue::Array(arr))
            } else {
                let mut entries = Vec::new();
                for pair in t.pairs::<LuaValue, LuaValue>() {
                    let (k, v) = pair?;
                    entries.push((lua_to_msgpack(k)?, lua_to_msgpack(v)?));
                }
                Ok(MsgPackValue::Map(entries))
            }
        }
        _ => Ok(MsgPackValue::Nil),
    }
}

fn msgpack_to_lua(lua: &Lua, val: MsgPackValue) -> LuaResult<LuaValue> {
    match val {
        MsgPackValue::Nil => Ok(LuaValue::Nil),
        MsgPackValue::Boolean(b) => Ok(LuaValue::Boolean(b)),
        MsgPackValue::Integer(i) => {
            if let Some(n) = i.as_i64() {
                Ok(LuaValue::Integer(n))
            } else if let Some(u) = i.as_u64() {
                Ok(LuaValue::Number(u as f64))
            } else {
                Ok(LuaValue::Number(i.as_f64().unwrap_or(0.0)))
            }
        }
        MsgPackValue::F32(f) => Ok(LuaValue::Number(f as f64)),
        MsgPackValue::F64(f) => Ok(LuaValue::Number(f)),
        MsgPackValue::String(s) => Ok(LuaValue::String(lua.create_string(s.as_bytes())?)),
        MsgPackValue::Binary(b) => Ok(LuaValue::String(lua.create_string(&b)?)),
        MsgPackValue::Array(arr) => {
            let t = lua.create_table_with_capacity(arr.len(), 0)?;
            for (idx, item) in arr.into_iter().enumerate() {
                let lv = msgpack_to_lua(lua, item)?;
                t.raw_set((idx + 1) as i64, lv)?;
            }
            Ok(LuaValue::Table(t))
        }
        MsgPackValue::Map(entries) => {
            let t = lua.create_table_with_capacity(0, entries.len())?;
            for (k, v) in entries {
                let lk = msgpack_to_lua(lua, k)?;
                let lv = msgpack_to_lua(lua, v)?;
                if !matches!(lk, LuaValue::Nil) {
                    t.raw_set(lk, lv)?;
                }
            }
            Ok(LuaValue::Table(t))
        }
        MsgPackValue::Ext(_, data) => Ok(LuaValue::String(lua.create_string(&data)?)),
    }
}

fn install_bit_lib(lua: &Lua, globals: &Table) -> LuaResult<()> {
    let bit = lua.create_table()?;
    bit.raw_set("tobit", lua.create_function(|_, x: f64| Ok(x as i32 as i64))?)?;
    bit.raw_set("bnot", lua.create_function(|_, x: f64| Ok((!(x as i32)) as i64))?)?;
    bit.raw_set(
        "band",
        lua.create_function(|_, args: MultiValue| {
            let mut acc: i32 = -1;
            for v in args {
                if let Some(n) = lua_val_to_i32(&v) {
                    acc &= n;
                }
            }
            Ok(acc as i64)
        })?,
    )?;
    bit.raw_set(
        "bor",
        lua.create_function(|_, args: MultiValue| {
            let mut acc: i32 = 0;
            for v in args {
                if let Some(n) = lua_val_to_i32(&v) {
                    acc |= n;
                }
            }
            Ok(acc as i64)
        })?,
    )?;
    bit.raw_set(
        "bxor",
        lua.create_function(|_, args: MultiValue| {
            let mut acc: i32 = 0;
            for v in args {
                if let Some(n) = lua_val_to_i32(&v) {
                    acc ^= n;
                }
            }
            Ok(acc as i64)
        })?,
    )?;
    bit.raw_set(
        "lshift",
        lua.create_function(|_, (x, n): (f64, i32)| Ok(((x as i32) << (n & 31)) as i64))?,
    )?;
    bit.raw_set(
        "rshift",
        lua.create_function(|_, (x, n): (f64, i32)| {
            Ok((((x as i32 as u32) >> (n & 31)) as i32) as i64)
        })?,
    )?;
    bit.raw_set(
        "arshift",
        lua.create_function(|_, (x, n): (f64, i32)| Ok(((x as i32) >> (n & 31)) as i64))?,
    )?;
    bit.raw_set(
        "rol",
        lua.create_function(|_, (x, n): (f64, u32)| {
            Ok(((x as i32 as u32).rotate_left(n & 31) as i32) as i64)
        })?,
    )?;
    bit.raw_set(
        "ror",
        lua.create_function(|_, (x, n): (f64, u32)| {
            Ok(((x as i32 as u32).rotate_right(n & 31) as i32) as i64)
        })?,
    )?;
    bit.raw_set(
        "bswap",
        lua.create_function(|_, x: f64| Ok(((x as i32 as u32).swap_bytes() as i32) as i64))?,
    )?;
    bit.raw_set(
        "tohex",
        lua.create_function(|_, (x, n): (f64, Option<i32>)| {
            let digits = n.unwrap_or(8);
            let upper = digits < 0;
            let width = digits.unsigned_abs().clamp(1, 8) as usize;
            let val = x as i32 as u32;
            let full = if upper {
                format!("{val:08X}")
            } else {
                format!("{val:08x}")
            };
            Ok(full[8 - width..].to_string())
        })?,
    )?;
    globals.raw_set("bit", bit)?;
    Ok(())
}

fn lua_val_to_i32(v: &LuaValue) -> Option<i32> {
    match v {
        LuaValue::Integer(i) => Some(*i as i32),
        LuaValue::Number(f) => Some(*f as i32),
        _ => None,
    }
}
