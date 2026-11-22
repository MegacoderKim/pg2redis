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
use crate::commands::{execute_batch_in_worker, ConnectionState};
use crate::config;
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

    if should_shutdown() {
        pgrx::log!("pg_redis_core: SIGTERM received before starting Tokio runtime, exiting");
        return;
    }

    let cache = Rc::new(L1Cache::new(config::cache_capacity()));

    // IMPORTANT: PostgreSQL SPI is strictly single-threaded per backend process.
    // We use Tokio's `current_thread` runtime + `LocalSet` so all async network I/O
    // and SPI transactions execute exclusively on this background worker OS thread.
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
                            let client_shutdown = shutdown_rx.clone();
                            tokio::task::spawn_local(async move {
                                handle_connection(stream, client_plans, client_cache, client_shutdown).await;
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
                    Ok(0) => break, // EOF
                    Ok(n) => n,
                    Err(_) => break,
                }
            }
        };

        if bytes_read == 0 {
            break;
        }

        // Parse ALL complete RESP frames currently buffered (pipelining & batching, RFC §5.4)
        let mut batch: Vec<Vec<Bytes>> = Vec::new();
        let mut protocol_err: Option<String> = None;

        loop {
            match decode_command(&mut read_buf) {
                Ok(Some(cmd_args)) => batch.push(cmd_args),
                Ok(None) => break,
                Err(e) => {
                    protocol_err = Some(e.to_string());
                    break;
                }
            }
        }

        if !batch.is_empty() {
            let responses = execute_batch_in_worker(
                batch,
                plans.as_ref().as_ref(),
                Some(cache.as_ref()),
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
