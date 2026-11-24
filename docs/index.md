---
hide:
  - navigation
  - toc
---

<div class="hero-banner" markdown>

<div class="hero-badges">
  <span class="hero-badge">PostgreSQL 15 &amp; 16</span>
  <span class="hero-badge">Rust + pgrx 0.11</span>
  <span class="hero-badge">RESP2 &amp; RESP3</span>
  <span class="hero-badge">Pub/Sub (Named &amp; Pattern)</span>
  <span class="hero-badge">Sandboxed Lua 5.1</span>
  <span class="hero-badge">16 Logical DBs (SELECT)</span>
  <span class="hero-badge">Optional TLS &amp; mTLS</span>
  <span class="hero-badge">AUTH &amp; Universal TTL</span>
  <span class="hero-badge">BullMQ 5.13 Ready</span>
</div>

# pg_redis_core

Speak native **Redis (RESP2 &amp; RESP3)** on port `6379` directly inside **PostgreSQL 15 &amp; 16**. Keep your existing `ioredis`, `BullMQ`, `redis-py`, `Jedis`, and `go-redis/v9` drivers while unifying cache, queues, Pub/Sub, and relational persistence inside a single ACID-compliant database.

<div class="hero-actions" markdown>
[Get Started in 60s](getting-started/quickstart.md){ .md-button .md-button--primary }
[Client Examples](examples/clients.md){ .md-button }
[Architecture](architecture/overview.md){ .md-button }
</div>

</div>

<div class="metric-grid">
  <div class="metric-card">
    <div class="metric-value">504K req/s</div>
    <div class="metric-label">Hot-key L1-cached <code>GET</code> (<code>p50 = 0.27ms</code>)</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">8.5K req/s</div>
    <div class="metric-label">Pipelined <code>SET</code> throughput (<code>UNLOGGED</code>)</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">RESP2 &amp; 3</div>
    <div class="metric-label">Zero-copy <code>redis-protocol 6.0</code> parser</div>
  </div>
  <div class="metric-card">
    <div class="metric-value">1 Database</div>
    <div class="metric-label">Zero cache-invalidation drift or dual ops</div>
  </div>
</div>

## Why `pg_redis_core`?

<div class="grid cards" markdown>

-   :material-memory:{ .lg .middle } **Break Free from RAM-Bound Costs**

    ---

    Store gigabytes of sessions, queues, leaderboards, and cached payloads on SSD/NVMe-backed PostgreSQL tables instead of expensive memory-only clusters.

-   :material-database-sync:{ .lg .middle } **Single ACID Source of Truth**

    ---

    Query, join, audit, and back up your Redis keys directly using standard PostgreSQL SQL across all seven catalog tables (`pg_redis_strings`, `pg_redis_hashes`, `pg_redis_lists`, `pg_redis_zsets`, `pg_redis_sets`, `pg_redis_streams`, `pg_redis_key_expiry`) with universal TTL support.

-   :material-flash:{ .lg .middle } **Engineered for Speed**

    ---

    Combines an in-process `moka` L1 hot-key cache, `SPI_keepplan` prepared statements, pipelined transaction batching, `fillfactor = 80` HOT updates, and optional `UNLOGGED` WAL bypass.

-   :material-language-python:{ .lg .middle } **Drop-In Driver & BullMQ Compatibility**

    ---

    Verified against unmodified **BullMQ 5.13.0** (Medusa queues: sandboxed Lua 5.1 + `cmsgpack` + `cjson` + `XADD` + `BZPOPMIN` + `HSCAN`/`SSCAN`/`ZSCAN` + `CLIENT LIST`), **`ioredis@5.11.1`**, `redis-py` / `FastAPI`, `Jedis` (Kotlin), `go-redis/v9` (RESP3), and `redis-cli`.

-   :material-broadcast:{ .lg .middle } **In-Process Named & Pattern Pub/Sub**

    ---

    Zero-SPI-overhead `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, and `PUBSUB` (`CHANNELS`/`NUMSUB`/`NUMPAT`) supporting binary-safe payloads far beyond PostgreSQL's 8 KB `NOTIFY` limit with bounded slow-consumer queues.

-   :material-shield-lock:{ .lg .middle } **Optional Native TLS, AUTH & Resource Limits**

    ---

    Opt-in TLS 1.2/1.3 & mTLS (`rediss://` via `tokio-rustls`, plain-only / dual-port / TLS-only modes, live `SIGHUP` cert reload), constant-time `AUTH` & `HELLO ... AUTH` (`pg_redis.requirepass`), sandboxed Lua 5.1 with timeout (`pg_redis.lua_time_limit_ms`), `pg_redis.max_clients`, and `pg_redis.max_frame_bytes`.

</div>

## 30-Second Preview

```bash
git clone https://github.com/MegacoderKim/pg2redis.git
cd pg2redis

# Boot PostgreSQL 15 with pg_redis_core listening on :5432 and :6379
# (or pull prebuilt ghcr.io/megacoderkim/pg2redis:latest-pg16)
docker compose up -d --build --wait pg-redis

# Write via Redis protocol
docker compose exec pg-redis redis-cli SET greeting "hello from postgres"

# Read directly from PostgreSQL SQL
docker compose exec pg-redis psql -U postgres -c \
  "SELECT key, convert_from(value, 'UTF8') AS value FROM pg_redis_strings;"
```

```mermaid
flowchart LR
    C1["Python<br/>redis-py / FastAPI"] -->|RESP2 :6379| BG
    C2["Node.js<br/>ioredis / BullMQ"] -->|RESP2 :6379| BG
    C3["Kotlin / Go<br/>Jedis · go-redis/v9"] -->|RESP2/3 :6379| BG

    subgraph PG["PostgreSQL 15 & 16 Process"]
        BG["pg_redis_core BGWorker<br/>Tokio current_thread + AUTH/Limits"]
        L1["L1 Hot-Key Cache<br/>(moka)"]
        PS["PubSubHub &amp; KeyNotifier<br/>(Named/Pattern + Blocking)"]
        LUA["Sandboxed Lua 5.1<br/>(mlua + Timeout)"]
        SPI["Prepared SPI Plans<br/>(SPI_keepplan)"]
        TBL[("Relational Catalog<br/>strings · hashes · lists · zsets<br/>sets · streams · key_expiry")]

        BG -->|Hot GET| L1
        BG -->|Pub/Sub &amp; Block| PS
        BG -->|EVAL/EVALSHA| LUA
        LUA --> SPI
        BG -->|Batched Tx| SPI
        SPI --> TBL
    end
```
