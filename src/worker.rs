use bytes::{Bytes, BytesMut};
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::LocalSet;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

use crate::cache::L1Cache;
use crate::clients::{ClientRegistry, ClientRegistryGuard};
use crate::commands::{
    execute_batch_in_worker, lists, parse_blocking_command, streams, zsets, BlockingCommand,
    ConnectionState,
};
use crate::config;
use crate::lua::LuaEngine;
use crate::notify::KeyNotifier;
use crate::protocol::{decode_command, encode_resp, RespValue, RespVersion};
use crate::pubsub::{PubSubClientGuard, PubSubHub, PubSubMessage};
use crate::storage::{self, PreparedPlans};
use crate::tls;

const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

enum ClientStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl ClientStream {
    fn tcp_ref(&self) -> &TcpStream {
        match self {
            Self::Plain(s) => s,
            Self::Tls(s) => s.get_ref().0,
        }
    }

    async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read_buf(buf).await,
            Self::Tls(s) => s.read_buf(buf).await,
        }
    }

    async fn write_all_flush(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.write_all(data).await,
            Self::Tls(s) => {
                s.write_all(data).await?;
                s.flush().await
            }
        }
    }
}

async fn accept_optional(listener: &Option<TcpListener>) -> std::io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(l) => l.accept().await,
        None => std::future::pending().await,
    }
}

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
    let tls_port = config::tls_port();

    if port == 0 && tls_port == 0 {
        pgrx::warning!(
            "pg_redis_core: both pg_redis.port and pg_redis.tls_port are 0; no RESP listener enabled"
        );
        return;
    }
    if port > 0 && port == tls_port {
        pgrx::warning!(
            "pg_redis_core: pg_redis.port ({}) and pg_redis.tls_port ({}) cannot be identical",
            port,
            tls_port
        );
        return;
    }

    let initial_tls_acceptor: Option<TlsAcceptor> = if tls_port > 0 {
        match tls::build_tls_acceptor_from_config() {
            Ok(acc) => Some(acc),
            Err(e) => {
                pgrx::warning!("pg_redis_core: TLS initialization failed: {}", e);
                return;
            }
        }
    } else {
        None
    };

    pgrx::log!(
        "pg_redis_core: starting RESP background worker (db='{}', bind='{}', port={}, tls_port={})",
        dbname,
        bind_addr,
        port,
        tls_port
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
    let pubsub = Rc::new(PubSubHub::new());
    let clients = Rc::new(ClientRegistry::new());

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
    let plain_target = (port > 0).then(|| format!("{bind_addr}:{port}"));
    let tls_target = (tls_port > 0).then(|| format!("{bind_addr}:{tls_port}"));
    let tls_acceptor = Rc::new(RefCell::new(initial_tls_acceptor));

    runtime.block_on(local.run_until(async move {
        let plain_listener = match &plain_target {
            Some(target) => match bind_with_retry(target).await {
                Some(l) => {
                    pgrx::log!("pg_redis_core: RESP plain TCP server listening on {}", target);
                    Some(l)
                }
                None => return,
            },
            None => None,
        };

        let tls_listener = match &tls_target {
            Some(target) => match bind_with_retry(target).await {
                Some(l) => {
                    pgrx::log!(
                        "pg_redis_core: RESP TLS (rediss://) server listening on {} (tls_auth_clients={})",
                        target,
                        config::tls_auth_clients()
                    );
                    Some(l)
                }
                None => return,
            },
            None => None,
        };

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        // Periodic housekeeping task: SIGTERM/SIGHUP polling + TLS cert reload + automated TTL eviction
        let plans_evict = Rc::clone(&plans);
        let tls_acceptor_house = Rc::clone(&tls_acceptor);
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
                    unsafe {
                        pg_sys::ProcessConfigFile(pg_sys::GucContext_PGC_SIGHUP);
                    }
                    pgrx::log!("pg_redis_core: reloaded GUC configuration on SIGHUP");
                    if tls_port > 0 {
                        match tls::build_tls_acceptor_from_config() {
                            Ok(new_acc) => {
                                *tls_acceptor_house.borrow_mut() = Some(new_acc);
                                pgrx::log!(
                                    "pg_redis_core: reloaded TLS certificates on SIGHUP (tls_auth_clients={})",
                                    config::tls_auth_clients()
                                );
                            }
                            Err(e) => {
                                pgrx::warning!(
                                    "pg_redis_core: failed to reload TLS certificates on SIGHUP (keeping previous config): {}",
                                    e
                                );
                            }
                        }
                    }
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
                            "pg_redis_core: evicted {} expired keys",
                            evicted
                        );
                    }
                }
            }
        });

        // Main TCP / TLS accept loop
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                accept_res = accept_optional(&plain_listener) => {
                    match accept_res {
                        Ok((mut stream, peer)) => {
                            if clients.count() >= config::max_clients() {
                                tokio::task::spawn_local(async move {
                                    let _ = stream
                                        .write_all(b"-ERR max number of clients reached\r\n")
                                        .await;
                                });
                                continue;
                            }
                            let _ = stream.set_nodelay(true);
                            let peer_addr = peer.to_string();
                            let client_plans = Rc::clone(&plans);
                            let client_cache = Rc::clone(&cache);
                            let client_lua = Rc::clone(&lua_engine);
                            let client_notifier = Rc::clone(&notifier);
                            let client_pubsub = Rc::clone(&pubsub);
                            let client_registry = Rc::clone(&clients);
                            let client_shutdown = shutdown_rx.clone();
                            tokio::task::spawn_local(async move {
                                handle_connection(
                                    ClientStream::Plain(stream),
                                    peer_addr,
                                    client_plans,
                                    client_cache,
                                    client_lua,
                                    client_notifier,
                                    client_pubsub,
                                    client_registry,
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
                accept_res = accept_optional(&tls_listener) => {
                    match accept_res {
                        Ok((stream, peer)) => {
                            if clients.count() >= config::max_clients() {
                                drop(stream);
                                continue;
                            }
                            let _ = stream.set_nodelay(true);
                            let Some(acceptor) = tls_acceptor.borrow().as_ref().cloned() else {
                                drop(stream);
                                continue;
                            };
                            let peer_addr = peer.to_string();
                            let client_plans = Rc::clone(&plans);
                            let client_cache = Rc::clone(&cache);
                            let client_lua = Rc::clone(&lua_engine);
                            let client_notifier = Rc::clone(&notifier);
                            let client_pubsub = Rc::clone(&pubsub);
                            let client_registry = Rc::clone(&clients);
                            let client_shutdown = shutdown_rx.clone();
                            tokio::task::spawn_local(async move {
                                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                                    Ok(Ok(mut tls_stream)) => {
                                        if client_registry.count() >= config::max_clients() {
                                            let _ = tls_stream
                                                .write_all(b"-ERR max number of clients reached\r\n")
                                                .await;
                                            let _ = tls_stream.flush().await;
                                            return;
                                        }
                                        handle_connection(
                                            ClientStream::Tls(Box::new(tls_stream)),
                                            peer_addr,
                                            client_plans,
                                            client_cache,
                                            client_lua,
                                            client_notifier,
                                            client_pubsub,
                                            client_registry,
                                            client_shutdown,
                                        ).await;
                                    }
                                    Ok(Err(e)) => {
                                        pgrx::debug1!(
                                            "pg_redis_core: TLS handshake failed from {}: {}",
                                            peer_addr,
                                            e
                                        );
                                    }
                                    Err(_) => {
                                        pgrx::debug1!(
                                            "pg_redis_core: TLS handshake timed out from {}",
                                            peer_addr
                                        );
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            pgrx::warning!("pg_redis_core: TLS TCP accept error: {}", e);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    }
                }
            }
        }

        drop(plain_listener);
        drop(tls_listener);
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

async fn flush_pubsub_messages(
    pubsub_rx: &mut mpsc::Receiver<PubSubMessage>,
    stream: &mut ClientStream,
    proto: RespVersion,
    write_buf: &mut BytesMut,
    first: Option<PubSubMessage>,
    drain_all: bool,
) -> bool {
    let mut next_msg = first;
    loop {
        write_buf.clear();
        let mut count = 0usize;
        if let Some(msg) = next_msg.take() {
            encode_resp(&msg.into_resp(), proto, write_buf);
            count += 1;
        }
        while count < 256 {
            match pubsub_rx.try_recv() {
                Ok(msg) => {
                    encode_resp(&msg.into_resp(), proto, write_buf);
                    count += 1;
                }
                Err(_) => break,
            }
        }
        if !write_buf.is_empty() && stream.write_all_flush(write_buf).await.is_err() {
            return false;
        }
        if !drain_all || count < 256 {
            return true;
        }
    }
}

enum ConnEvent {
    Shutdown,
    PubSub(PubSubMessage),
    TcpRead(usize),
    TcpClosed,
}

async fn handle_connection(
    mut stream: ClientStream,
    peer_addr: String,
    plans: Rc<Option<PreparedPlans>>,
    cache: Rc<L1Cache>,
    lua: Rc<Option<LuaEngine>>,
    notifier: Rc<KeyNotifier>,
    pubsub: Rc<PubSubHub>,
    clients: Rc<ClientRegistry>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let client_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    let mut conn = ConnectionState::new(client_id);
    let mut read_buf = BytesMut::with_capacity(8192);
    let mut write_buf = BytesMut::with_capacity(8192);

    let (kill_tx, mut kill_rx) = watch::channel(false);
    clients.register(client_id, peer_addr, kill_tx);
    let _client_guard = ClientRegistryGuard::new(client_id, clients.as_ref());

    let (pubsub_tx, mut pubsub_rx) = mpsc::channel::<PubSubMessage>(config::pubsub_queue_size());
    pubsub.register_client(client_id, pubsub_tx);
    let _pubsub_guard = PubSubClientGuard::new(client_id, pubsub.as_ref());

    loop {
        let event = tokio::select! {
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    ConnEvent::Shutdown
                } else {
                    continue;
                }
            }
            _ = kill_rx.changed() => {
                if *kill_rx.borrow() {
                    ConnEvent::Shutdown
                } else {
                    continue;
                }
            }
            maybe_msg = pubsub_rx.recv() => {
                match maybe_msg {
                    Some(msg) => ConnEvent::PubSub(msg),
                    None => ConnEvent::TcpClosed,
                }
            }
            res = stream.read_buf(&mut read_buf) => {
                match res {
                    Ok(0) | Err(_) => ConnEvent::TcpClosed,
                    Ok(n) => ConnEvent::TcpRead(n),
                }
            }
        };

        match event {
            ConnEvent::Shutdown | ConnEvent::TcpClosed => break,
            ConnEvent::PubSub(first_msg) => {
                if !flush_pubsub_messages(
                    &mut pubsub_rx,
                    &mut stream,
                    conn.protocol,
                    &mut write_buf,
                    Some(first_msg),
                    false,
                )
                .await
                {
                    break;
                }
                continue;
            }
            ConnEvent::TcpRead(0) => break,
            ConnEvent::TcpRead(_) => {}
        }

        if read_buf.len() > config::max_frame_bytes() {
            write_buf.clear();
            encode_resp(
                &RespValue::err("ERR Protocol error: max frame size exceeded"),
                conn.protocol,
                &mut write_buf,
            );
            let _ = stream.write_all_flush(&write_buf).await;
            break;
        }

        // Flush any already-queued Pub/Sub messages BEFORE processing client commands
        // (e.g. UNSUBSCRIBE) so messages published prior to UNSUBSCRIBE are strictly delivered first.
        if !flush_pubsub_messages(
            &mut pubsub_rx,
            &mut stream,
            conn.protocol,
            &mut write_buf,
            None,
            true,
        )
        .await
        {
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
            let unauthenticated = config::requirepass().is_some() && !conn.authenticated;
            let in_subscribed_resp2 =
                conn.protocol == RespVersion::Resp2 && pubsub.subscription_count(client_id) > 0;
            if let Some(blocking_parsed) = parse_blocking_command(
                &cmd_args,
                conn.in_multi || unauthenticated || in_subscribed_resp2,
            ) {
                // Flush any preceding non-blocking commands first
                if !pending_batch.is_empty() {
                    let batch = std::mem::take(&mut pending_batch);
                    let responses = execute_batch_in_worker(
                        batch,
                        plans.as_ref().as_ref(),
                        Some(cache.as_ref()),
                        lua.as_ref().as_ref(),
                        Some(notifier.as_ref()),
                        Some(pubsub.as_ref()),
                        Some(clients.as_ref()),
                        &mut conn,
                    );
                    write_buf.clear();
                    for (resp, proto) in &responses {
                        encode_resp(resp, *proto, &mut write_buf);
                    }
                    if !write_buf.is_empty() && stream.write_all_flush(&write_buf).await.is_err() {
                        write_failed = true;
                        break;
                    }
                    if conn.should_quit {
                        break;
                    }
                }

                // Re-check connection mode in case the preceding batch entered MULTI,
                // de-authenticated (RESET), or entered subscribed mode.
                let now_unauth = config::requirepass().is_some() && !conn.authenticated;
                let now_sub_resp2 = conn.protocol == RespVersion::Resp2
                    && pubsub.subscription_count(client_id) > 0;
                if conn.in_multi || now_unauth || now_sub_resp2 {
                    pending_batch.push(cmd_args);
                    continue;
                }

                let resp = match blocking_parsed {
                    Err(err_resp) => err_resp,
                    Ok(bcmd) => {
                        execute_blocking_command(
                            bcmd,
                            stream.tcp_ref(),
                            &notifier,
                            &mut shutdown_rx,
                            &mut kill_rx,
                        )
                        .await
                    }
                };

                if *kill_rx.borrow() {
                    write_failed = true;
                    break;
                }

                write_buf.clear();
                encode_resp(&resp, conn.protocol, &mut write_buf);
                if !write_buf.is_empty() && stream.write_all_flush(&write_buf).await.is_err() {
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
                Some(pubsub.as_ref()),
                Some(clients.as_ref()),
                &mut conn,
            );

            write_buf.clear();
            for (resp, proto) in &responses {
                encode_resp(resp, *proto, &mut write_buf);
            }

            if !write_buf.is_empty() && stream.write_all_flush(&write_buf).await.is_err() {
                break;
            }
        }

        if let Some(err_msg) = protocol_err {
            write_buf.clear();
            encode_resp(&RespValue::err(err_msg), conn.protocol, &mut write_buf);
            let _ = stream.write_all_flush(&write_buf).await;
            break;
        }

        if conn.should_quit || *kill_rx.borrow() {
            break;
        }
    }
}

async fn execute_blocking_command(
    cmd: BlockingCommand,
    stream: &TcpStream,
    notifier: &KeyNotifier,
    shutdown_rx: &mut watch::Receiver<bool>,
    kill_rx: &mut watch::Receiver<bool>,
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
            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, kill_rx, || {
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
            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, kill_rx, || {
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

            wait_for_keys(&keys, deadline, stream, notifier, shutdown_rx, kill_rx, || {
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
    kill_rx: &mut watch::Receiver<bool>,
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
    let mut can_peek = true;

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

        // Wait on notification, timer slice, shutdown, kill, or client disconnect (EOF)
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
            _ = kill_rx.changed() => {
                if *kill_rx.borrow() {
                    break RespValue::Null;
                }
            }
            _ = wait_fut => {}
            _ = tokio::time::sleep(poll_wait) => {}
            peek_res = stream.peek(&mut peek_buf), if can_peek => {
                match peek_res {
                    Ok(0) | Err(_) => break RespValue::Null,
                    Ok(_) => {
                        can_peek = false;
                    }
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
