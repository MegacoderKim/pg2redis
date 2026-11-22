# System Architecture

`pg_redis_core` embeds an asynchronous Redis-compatible TCP server inside a dedicated PostgreSQL background worker process, eliminating extra network hops and dual-database synchronization hazards.

## High-Level Data Flow

```mermaid
flowchart TB
    subgraph Clients["Standard Redis Clients (TCP :6379)"]
        PY["Python (redis-py / FastAPI)"]
        KT["Kotlin (Jedis)"]
        GO["Go (go-redis/v9 RESP3)"]
        CLI["redis-cli / nc"]
    end

    subgraph Worker["PostgreSQL Background Worker (pg_redis_bgworker_main)"]
        RT["Tokio current_thread + LocalSet<br/>(Single OS Thread — 100% SPI Safe)"]
        PARSE["Zero-Copy RESP Parser<br/>(redis-protocol 6.0 + Inline Fallback)"]
        BATCH["Pipeline & MULTI/EXEC Batcher"]
        CACHE["In-Process L1 Hot-Key Cache<br/>(moka::sync::Cache)"]
        EVICT["Periodic Housekeeping Task<br/>(SIGTERM/SIGHUP + ctid TTL Eviction)"]
    end

    subgraph Postgres["PostgreSQL 15+ Storage Engine"]
        SPI["Server Programming Interface (SPI)<br/>Pre-Prepared Plans (SPI_keepplan)"]
        T1[("pg_redis_strings<br/>BYTEA + TIMESTAMPTZ")]
        T2[("pg_redis_hashes<br/>(key, field) -> value")]
        T3[("pg_redis_lists<br/>(key, seq) -> value")]
        T4[("pg_redis_zsets<br/>(key, member) -> score")]
    end

    PY & KT & GO & CLI --> RT
    RT --> PARSE --> BATCH
    BATCH -->|Hot GET hit| CACHE
    BATCH -->|Single Tx per Batch| SPI
    EVICT -->|Every eviction_interval_ms| SPI
    SPI --> T1 & T2 & T3 & T4
```

## Thread-Safety Model: `current_thread` Tokio + `LocalSet`

PostgreSQL backend processes maintain process-global C state (`CurrentMemoryContext`, `TopTransactionContext`, `SPI_tuptable`, `sigjmp_buf`) that is **strictly single-threaded**. Calling SPI from a multi-threaded Tokio worker pool would corrupt PostgreSQL backend memory.

`pg_redis_core` guarantees safety while preserving high socket concurrency:

1. **Single OS Thread:** The background worker builds `tokio::runtime::Builder::new_current_thread().enable_all()` and drives all tasks inside a `tokio::task::LocalSet`.
2. **Multiplexed Async I/O:** Hundreds of concurrent TCP sockets are multiplexed non-blockingly via `epoll`/`kqueue` with `TCP_NODELAY` enabled to eliminate Nagle latency.
3. **Atomic Synchronous SPI Blocks:** No `.await` point ever occurs inside a `BackgroundWorker::transaction(|| Spi::connect(...))` closure, so SPI transactions execute atomically and sequentially with zero interleaving.

## Request Lifecycle & Pipelining

```mermaid
sequenceDiagram
    participant C as Redis Client
    participant W as Tokio Task (handle_connection)
    participant L1 as L1 moka Cache
    participant PG as PostgreSQL SPI

    C->>W: TCP Read (1..N pipelined RESP frames)
    W->>W: Zero-copy decode_command() loop -> batch[1..N]

    alt All commands satisfied in memory (PING / HELLO / L1 GET hit)
        W->>L1: Lookup hot key(s)
        L1-->>W: Cache Hit (Bytes)
        Note over W: Zero PostgreSQL transaction overhead
    else Any command requires SPI
        W->>PG: BackgroundWorker::transaction() [1 Tx for entire batch]
        loop For each remaining command in batch
            PG->>PG: Execute PreparedPlans (SPI_keepplan)
            PG->>L1: Write-through update / invalidate
        end
        PG-->>W: Vec<(RespValue, RespVersion)>
    end

    W->>C: Single write_all() with RESP2 or RESP3 encoded responses
```

## Protocol Negotiation (RESP2 & RESP3)

Each TCP connection tracks its own `ConnectionState`:

- Defaults to **RESP2** (full compatibility with `nc`, `redis-cli`, `redis-py`, `Jedis`).
- Upgrades dynamically to **RESP3** when a client sends `HELLO 3` (used by default by `go-redis/v9`).
- Encodes `Map` responses (`HGETALL`, `HELLO`) as flat arrays in RESP2 and `%N\r\n` maps in RESP3; encodes `ScoredArray` (`ZRANGE ... WITHSCORES`) as flat bulk strings in RESP2 and nested `[member, Double]` tuples in RESP3.
- Supports inline text commands (`PING\r\n`, `SET foo "bar baz"\r\n`) alongside multibulk arrays (`*N\r\n...`).

## Embedded Lua 5.1 Scripting (`mlua`) & Why Not `pllua-ng`

While PostgreSQL has a standalone procedural language extension (**`pllua` / `pllua-ng`**) for writing SQL stored procedures and triggers in Lua 5.3/5.4, Redis scripting (and job frameworks like **BullMQ 5.13.0**) requires:

- **Exact Lua 5.1 semantics** (`unpack()`, `bit` library, Lua 5.1 coercion rules),
- **Redis-specific libraries and globals**: binary-safe `KEYS` / `ARGV`, `redis.call` / `redis.pcall`, `cjson`, `cmsgpack` (used by BullMQ for job arguments and options), and exact Lua↔RESP type conversions (`nil` bulk reply → `false`, `{ok=...}`, `{err=...}`),
- **Zero-hop transactional execution** inside the active PostgreSQL SPI transaction.

`pg_redis_core` therefore embeds **Lua 5.1 directly inside the Rust background worker** via `mlua` (`features = ["lua51", "vendored"]`), caching compiled chunks by 40-char SHA1 (`SCRIPT LOAD` / `EVALSHA`) and executing `redis.call(...)` directly against the active PostgreSQL SPI transaction with full ACID atomicity.

## Cooperative Blocking Wakeups (`BZPOPMIN`, `BLPOP`, `XREAD BLOCK`)

Blocking commands are handled asynchronously on the background worker's `LocalSet` via `KeyNotifier` (`tokio::sync::Notify` per watched key):

- Idle workers (such as BullMQ workers blocked on `BZPOPMIN bull:<queue>:marker <timeout>` or `QueueEvents` blocked on `XREAD BLOCK ... STREAMS ...`) yield without holding a PostgreSQL transaction or burning CPU.
- Whenever any command — direct or inside a Lua script (`ZADD`, `LPUSH`, `RPUSH`, `XADD`) — commits a mutation, `KeyNotifier` wakes waiting tasks **immediately after PostgreSQL transaction commit**, guaranteeing MVCC visibility and sub-millisecond wakeup latency.

## Signal Handling & Clean Shutdown

During PostgreSQL startup/shutdown (including Docker's temporary `initdb` server cycle), the background worker monitors `BackgroundWorker::sigterm_received()` and `wait_latch` return status via an atomic `SHUTDOWN_REQUESTED` latch. On `SIGTERM`, a `tokio::sync::watch` channel terminates the accept loop and active client handlers within 50ms, dropping `TcpListener` immediately so `pg_ctl stop` completes cleanly.
