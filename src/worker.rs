use bytes::{Bytes, BytesMut};
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::LocalSet;

use crate::cache::L1Cache;
use crate::commands::{
    execute_batch_in_worker, lists, parse_blocking_command, streams, zsets, BlockingCommand,
    ConnectionState,
};
use crate::config;
use crate::lua::LuaEngine;
use crate::notify::KeyNotifier;
use crate::protocol::{decode_command, encode_resp, RespValue};
use crate::storage::{self, PreparedPlans};

static NEXT_CLIENT_ID: AtomicU64 = AtomicU64::new(1);
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn should_shutdown() -> bool {
    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        return true;
    }
    if BackgroundWorker::sigterm_received() {
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
        return true;
    }
    false
}

pub fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_redis_core RESP worker")
        .set_function("pg_redis_bgworker_main")
        .set_library("pg_redis_core")
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(2)))
        .enable_spi_access()
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_redis_bgworker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let dbname = config::database();
    let bind_addr = config::bind_address();
    let port = config::port();

    pgrx::log!(
        "pg_redis_core: starting RESP background worker (db='{}', listen='{}:{}')",
        dbname,
        bind_addr,
        port
    );

    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // Give init scripts (e.g. CREATE EXTENSION pg_redis_core) a brief window to create tables
    // owned by the extension before auto-provisioning them.
    let mut schema_ready = false;
    for _ in 0..10 {
        if should_shutdown() {
            pgrx::log!("pg_redis_core: SIGTERM received during startup, exiting");
            return;
        }
        let exists = BackgroundWorker::transaction(|| {
            Spi::connect(|client| storage::tables_exist(&client).unwrap_or(false))
        });
        if exists {
            schema_ready = true;
            break;
        }
        // NOTE: `wait_latch` consumes `GOT_SIGTERM` and returns `false` on SIGTERM!
        if !BackgroundWorker::wait_latch(Some(Duration::from_millis(100))) {
            SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
            pgrx::log!("pg_redis_core: SIGTERM received during startup wait, exiting");
            return;
        }
    }

    if should_shutdown() {
        pgrx::log!("pg_redis_core: SIGTERM received before schema init, exiting");
        return;
    }

    if !schema_ready {
        let unlogged = config::unlogged();
        let res: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            Spi::connect(|mut client| storage::ensure_schema(&mut client, unlogged))
        });
        if let Err(e) = res {
            pgrx::warning!("pg_redis_core: failed to ensure catalog schema: {}", e);
            return;
        }
        pgrx::log!(
            "pg_redis_core: auto-provisioned catalog tables (unlogged={})",
            unlogged
        );
    }

    if should_shutdown() {
        pgrx::log!("pg_redis_core: SIGTERM received before preparing plans, exiting");
        return;
    }

    // Prepare reusable SPI query plans kept across transactions (RFC §5.3)
    let prep_res: Result<PreparedPlans, pgrx::spi::Error> = BackgroundWorker::transaction(|| {
        Spi::connect(|client| PreparedPlans::prepare_all(&client))
    });
    let plans: Rc<Option<PreparedPlans>> = match prep_res {
        Ok(p) => {
            pgrx::log!("pg_redis_core: prepared SPI query plans initialized");
            Rc::new(Some(p))
        }
        Err(e) => {
            pgrx::warning!(
                "pg_redis_core: unable to pre-prepare SPI plans (falling back to dynamic SQL): {}",
                e
            );
            Rc::new(None)
        }
    };

    // Initialize embedded Lua 5.1 VM (mlua vendored) with cjson, cmsgpack, bit, and SHA1 script cache
    let lua_engine: Rc<Option<LuaEngine>> = match LuaEngine::new() {
        Ok(engine) => {
            pgrx::log!("pg_redis_core: embedded Lua 5.1 scripting engine initialized");
            Rc::new(Some(engine))
        }
        Err(e) => {
            pgrx::warning!("pg_redis_core: failed to initialize Lua 5.1 engine: {}", e);
            Rc::new(None)
        }
    };

    if should_shutdown() {
        pgrx::log!("pg_redis_core: SIGTERM received before starting Tokio runtime, exiting");
        return;
    }

    let cache = Rc::new(L1Cache::new(config::cache_capacity()));
    let notifier = Rc::new(KeyNotifier::new());

    // PostgreSQL SPI is strictly single-threaded per backend process.
    // Tokio `current_thread` + `LocalSet` multiplexes all async TCP connections and
    // blocking command waits (`BZPOPMIN`, `XREAD BLOCK`) on this single OS thread.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            pgrx::warning!("pg_redis_core: failed to create Tokio runtime: {}", e);
            return;
        }
    };

    let local = LocalSet::new();
    let listen_target = format!("{bind_addr}:{port}");

    runtime.block_on(local.run_until(async move {
        let listener = match bind_with_retry(&listen_target).await {
            Some(l) => l,
            None => return,
        };

        pgrx::log!("pg_redis_core: RESP server listening on {}", listen_target);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        // Periodic housekeeping task: SIGTERM/SIGHUP polling + automated TTL eviction (RFC §5.6)
        let plans_evict = Rc::clone(&plans);
        let shutdown_tx_house = shutdown_tx.clone();
        tokio::task::spawn_local(async move {
            let mut signal_tick = tokio::time::interval(Duration::from_millis(50));
            let mut last_eviction = Instant::now();

            loop {
                signal_tick.tick().await;

                if should_shutdown() {
                    pgrx::log!("pg_redis_core: SIGTERM received, shutting down RESP listener");
                    let _ = shutdown_tx_house.send(true);
                    break;
                }

                if BackgroundWorker::sighup_received() {
                    pgrx::log!("pg_redis_core: reloaded GUC configuration on SIGHUP");
                }

                let evict_interval = Duration::from_millis(config::eviction_interval_ms());
                if last_eviction.elapsed() >= evict_interval {
                    last_eviction = Instant::now();
                    let plans_guard = AssertUnwindSafe(plans_evict.as_ref().as_ref());
                    let evicted = BackgroundWorker::transaction(move || {
                        Spi::connect(|mut client| {
                            storage::evict_expired_batch(&mut client, *plans_guard).unwrap_or(0)
                        })
                    });
                    if evicted > 0 {
                        pgrx::debug1!(
                            "pg_redis_core: evicted {} expired string keys",
                            evicted
                        );
                    }
                }
            }
        });

        // Main TCP accept loop
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _peer)) => {
                            let _ = stream.set_nodelay(true);
                            let client_plans = Rc::clone(&plans);
                            let client_cache = Rc::clone(&cache);
                            let client_lua = Rc::clone(&lua_engine);
                            let client_notifier = Rc::clone(&notifier);
                            let client_shutdown = shutdown_rx.clone();
                            tokio::task::spawn_local(async move {
                                handle_connection(
                                    stream,
                                    client_plans,
                                    client_cache,
                                    client_lua,
                                    client_notifier,
                                    client_shutdown,
                                ).await;
                            });
                        }
                        Err(e) => {
                            pgrx::warning!("pg_redis_core: TCP accept error: {}", e);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }

        drop(listener);
        pgrx::log!("pg_redis_core: RESP background worker stopped cleanly");
    }));
}

async fn bind_with_retry(addr: &str) -> Option<TcpListener> {
    for attempt in 1..=20 {
        if should_shutdown() {
            return None;
        }
        match TcpListener::bind(addr).await {
            Ok(listener) => return Some(listener),
            Err(e) => {
                if attempt == 20 {
                    pgrx::warning!(
                        "pg_redis_core: failed to bind {} after {} attempts: {}",
                        addr,
                        attempt,
                        e
                    );
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    None
}

async fn handle_connection(
    mut stream: TcpStream,
    plans: Rc<Option<PreparedPlans>>,
    cache: Rc<L1Cache>,
    lua: Rc<Option<LuaEngine>>,
    notifier: Rc<KeyNotifier>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnectionState::new(client_id);
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(8192);

    loop {
        let bytes_read = tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            res = stream.read_buf(&mut read_buf) => {
                match res {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(_) => break,
                }
            }
        };

        if bytes_read == 0 {
            break;
        }

        // Parse ALL complete RESP frames currently buffered (pipelining & batching, RFC §5.4)
        let mut decoded: Vec<Vec<Bytes>> = Vec::new();
        let mut protocol_err: Option<String> = None;

        loop {
            match decode_command(&mut read_buf) {
                Ok(Some(cmd_args)) => decoded.push(cmd_args),
                Ok(None) => break,
                Err(e) => {
                    protocol_err = Some(e.to_string());
                    break;
                }
            }
        }

        let mut pending_batch: Vec<Vec<Bytes>> = Vec::new();
        let mut write_failed = false;

        for cmd_args in decoded {
            if let Some(blocking_parsed) = parse_blocking_command(&cmd_args, conn.in_multi) {
                // Flush any preceding non-blocking commands first
                if !pending_batch.is_empty() {
                    let batch = std::mem::take(&mut pending_batch);
                    let responses = execute_batch_in_worker(
                        batch,
                        plans.as_ref().as_ref(),
                        Some(cache.as_ref()),
                        lua.as_ref().as_ref(),
                        Some(notifier.as_ref()),
                        &mut conn,
                    );
                    write_buf.clear();
                    for (resp, proto) in &responses {
                        encode_resp(resp, *proto, &mut write_buf);
                    }
                    if !write_buf.is_empty() && stream.write_all(&write_buf).await.is_err() {
                        write_failed = true;
                        break;
                    }
                    if conn.should_quit {
                        break;
                    }
                }

                let resp = match blocking_parsed {
                    Err(err_resp) => err_resp,
                    Ok(bcmd) => {
                        execute_blocking_command(
                            bcmd,
                            &stream,
                            &notifier,
                            &mut shutdown_rx,
                        )
                        .await
                    }
                };

                write_buf.clear();
                encode_resp(&resp, conn.protocol, &mut write_buf);
                if !write_buf.is_empty() && stream.write_all(&write_buf).await.is_err() {
                    write_failed = true;
                    break;
                }
            } else {
                pending_batch.push(cmd_args);
            }
        }

        if write_failed {
            break;
        }

        if !pending_batch.is_empty() {
            let responses = execute_batch_in_worker(
                pending_batch,
                plans.as_ref().as_ref(),
                Some(cache.as_ref()),
                lua.as_ref().as_ref(),
                Some(notifier.as_ref()),
                &mut conn,
            );

            write_buf.clear();
            for (resp, proto) in &responses {
                encode_resp(resp, *proto, &mut write_buf);
            }

            if !write_buf.is_empty() && stream.write_all(&write_buf).await.is_err() {
                break;
            }
        }

        if let Some(err_msg) = protocol_err {
            write_buf.clear();
            encode_resp(&RespValue::err(err_msg), conn.protocol, &mut write_buf);
            let _ = stream.write_all(&write_buf).await;
            break;
        }

        if conn.should_quit {
            break;
        }
    }
}

async fn execute_blocking_command(
    cmd: BlockingCommand,
    stream: &TcpStream,
    notifier: &KeyNotifier,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> RespValue {
    match cmd {
        BlockingCommand::BZPop {
            keys,
            timeout_secs,
            is_max,
        } => {
            let deadline = if timeout_secs == 0.0 {
                None
            } else {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            };
            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, || {
                let keys_guard = AssertUnwindSafe(&keys);
                BackgroundWorker::transaction(move || {
                    Spi::connect(|mut client| {
                        zsets::try_bzpop_once(&mut client, *keys_guard, is_max).unwrap_or(None)
                    })
                })
            })
            .await
        }
        BlockingCommand::BLPop {
            keys,
            timeout_secs,
            is_right,
        } => {
            let deadline = if timeout_secs == 0.0 {
                None
            } else {
                Some(tokio::time::Instant::now() + Duration::from_secs_f64(timeout_secs))
            };
            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, || {
                let keys_guard = AssertUnwindSafe(&keys);
                BackgroundWorker::transaction(move || {
                    Spi::connect(|mut client| {
                        lists::try_blpop_once(&mut client, *keys_guard, is_right).unwrap_or(None)
                    })
                })
            })
            .await
        }
        BlockingCommand::XReadBlock { req } => {
            // Resolve `$` IDs once before blocking so entries added after XREAD starts are captured
            let resolved_res: Result<Vec<(String, i64, i64)>, RespValue> = {
                let req_guard = AssertUnwindSafe(&req.streams);
                BackgroundWorker::transaction(move || {
                    Spi::connect(|client| {
                        streams::resolve_xread_ids(&client, *req_guard)
                            .unwrap_or_else(|e| Err(RespValue::err(format!("ERR SPI error: {e}"))))
                    })
                })
            };

            let resolved = match resolved_res {
                Ok(r) => r,
                Err(e) => return e,
            };

            let block_ms = req.block_ms.unwrap_or(0);
            let deadline = if block_ms == 0 {
                None
            } else {
                Some(tokio::time::Instant::now() + Duration::from_millis(block_ms))
            };

            let keys: Vec<String> = resolved.iter().map(|(k, _, _)| k.clone()).collect();
            let count = req.count;

            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, || {
                let res_guard = AssertUnwindSafe(&resolved);
                BackgroundWorker::transaction(move || {
                    Spi::connect(|client| {
                        streams::execute_xread_resolved(&client, *res_guard, count).unwrap_or(None)
                    })
                })
            })
            .await
        }
    }
}

async fn wait_for_keys<F>(
    keys: &[String],
    deadline: Option<tokio::time::Instant>,
    stream: &TcpStream,
    notifier: &KeyNotifier,
    shutdown_rx: &mut watch::Receiver<bool>,
    mut try_once: F,
) -> RespValue
where
    F: FnMut() -> Option<RespValue>,
{
    // Fast path: check immediately before registering waiters
    if let Some(val) = try_once() {
        return val;
    }

    let handles = notifier.get_notifiers(keys);

    let result = loop {
        // Check deadline
        let now = tokio::time::Instant::now();
        let poll_wait = match deadline {
            Some(dl) if now >= dl => break RespValue::Null,
            Some(dl) => (dl - now).min(Duration::from_millis(250)),
            None => Duration::from_millis(250),
        };

        // Register future BEFORE re-checking to avoid lost wakeups
        let first_notify = handles.first().cloned();

        // Check once more right after registering
        if let Some(val) = try_once() {
            break val;
        }

        // Wait on notification, timer slice, shutdown, or client disconnect (EOF)
        let wait_fut = async {
            if handles.len() == 1 {
                if let Some(ref n) = first_notify {
                    n.notified().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } else if !handles.is_empty() {
                // Race all key notifiers
                let mut futs = Vec::with_capacity(handles.len());
                for h in &handles {
                    futs.push(Box::pin(h.notified()));
                }
                let _ = wait_any_pinned(futs).await;
            } else {
                std::future::pending::<()>().await;
            }
        };

        let mut peek_buf = [0u8; 1];
        tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break RespValue::Null;
                }
            }
            _ = wait_fut => {}
            _ = tokio::time::sleep(poll_wait) => {}
            peek_res = stream.peek(&mut peek_buf) => {
                match peek_res {
                    Ok(0) | Err(_) => break RespValue::Null,
                    Ok(_) => {}
                }
            }
        }

        if let Some(val) = try_once() {
            break val;
        }
    };

    drop(handles);
    notifier.gc_keys(keys);
    result
}

async fn wait_any_pinned<F: std::future::Future>(
    mut futs: Vec<std::pin::Pin<Box<F>>>,
) -> usize {
    std::future::poll_fn(|cx| {
        for (idx, fut) in futs.iter_mut().enumerate() {
            if fut.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(idx);
            }
        }
        std::task::Poll::Pending
    })
    .await
}
