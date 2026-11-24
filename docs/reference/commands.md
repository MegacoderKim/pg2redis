# Supported Redis Commands

`pg_redis_core` supports **115 Redis commands** over both **RESP2** and **RESP3** (plus inline commands), covering Strings (`GETEX`), Universal Keys/Expiry across all 6 data types (`EXPIRE`/`PEXPIRE` with `NX|XX|GT|LT`, `EXPIRETIME`/`PEXPIRETIME`), Hashes (`HSCAN`), Lists, Sorted Sets (`ZSCAN`), Sets (`SSCAN`), Streams, **Pub/Sub** (`PUBLISH`/`SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBSUB`), Blocking Operations (`BZPOPMIN`, `BLPOP`, `XREAD BLOCK`), Sandboxed Lua 5.1 Scripting (`EVAL`/`EVALSHA`/`SCRIPT`), Transactions, `AUTH` (`requirepass`), and Multi-Client `CLIENT LIST`/`KILL` — verified on **PostgreSQL 15 & 16** for **BullMQ 5.13.0** and **Medusa 2.22.7 (`ioredis 5.11.1`)**.

## Strings

| Command | Syntax | Notes |
| --- | --- | --- |
| `GET` | `GET key` | Binary-safe (`BYTEA`); served from L1 `moka` cache when hot |
| `GETEX` | `GETEX key [EX s \| PX ms \| EXAT ts \| PXAT ts_ms \| PERSIST]` | Atomically reads value and updates or removes TTL (`connect-redis` rolling sessions) |
| `SET` | `SET key value [EX s \| PX ms \| EXAT ts \| PXAT ts_ms \| KEEPTTL] [NX \| XX] [GET]` | Full Redis option matrix; write-through L1 update |
| `SETEX` / `PSETEX` | `SETEX key seconds value` / `PSETEX key ms value` | Set value with relative TTL |
| `SETNX` | `SETNX key value` | Set only if key does not exist (or expired) |
| `GETSET` | `GETSET key value` | Atomically replace value and return previous value |
| `GETDEL` | `GETDEL key` | Atomically fetch and delete key |
| `MGET` | `MGET key [key ...]` | Batch read returning array of values / nils |
| `MSET` | `MSET key value [key value ...]` | Batch write in single SPI transaction |
| `INCR` / `DECR` | `INCR key` / `DECR key` | Atomic 64-bit integer increment/decrement preserving TTL |
| `INCRBY` / `DECRBY` | `INCRBY key delta` / `DECRBY key delta` | Atomic integer delta preserving TTL |
| `INCRBYFLOAT` | `INCRBYFLOAT key delta` | Floating-point increment preserving TTL |
| `APPEND` | `APPEND key value` | Binary-safe append returning new byte length |
| `STRLEN` | `STRLEN key` | Byte length of string value (`0` if missing) |

## Keys & Expiry

| Command | Syntax | Notes |
| --- | --- | --- |
| `DEL` / `UNLINK` | `DEL key [key ...]` | Removes keys across all 6 catalog tables + `pg_redis_key_expiry`; invalidates L1 cache |
| `EXISTS` | `EXISTS key [key ...]` | Checks active (non-expired) keys across all 6 catalog tables |
| `EXPIRE` / `PEXPIRE` | `EXPIRE key seconds [NX\|XX\|GT\|LT]` / `PEXPIRE key ms [...]` | Sets relative expiration on **any** key type (Strings, Hashes, Lists, ZSets, Sets, Streams) |
| `EXPIREAT` / `PEXPIREAT` | `EXPIREAT key unix_ts [NX\|XX\|GT\|LT]` / `PEXPIREAT key unix_ms [...]` | Sets absolute expiration timestamp on any key type |
| `EXPIRETIME` / `PEXPIRETIME` | `EXPIRETIME key` / `PEXPIRETIME key` | Returns absolute Unix expiry timestamp (s/ms), `-1` (persistent), or `-2` (missing) |
| `TTL` / `PTTL` | `TTL key` / `PTTL key` | Returns remaining TTL for any key type, `-1` (no expiry), or `-2` (missing) |
| `PERSIST` | `PERSIST key` | Removes expiration from any key type |
| `TYPE` | `TYPE key` | Returns `string`, `hash`, `list`, `zset`, `set`, `stream`, or `none` |
| `RENAME` | `RENAME key newkey` | Atomically renames key (and its TTL) across all 6 catalog tables |
| `KEYS` | `KEYS pattern` | Supports Redis glob patterns (`*`, `?`, `[abc]`, `[^a]`, `[a-z]`, `\x`) |
| `SCAN` | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | Cursor-based key iteration with glob and type filtering |
| `DBSIZE` | `DBSIZE` | Total count of active keys across all catalog tables |
| `FLUSHDB` / `FLUSHALL` | `FLUSHDB` | Clears all catalog tables, expiry metadata, and L1 cache |

## Hashes

| Command | Syntax | Notes |
| --- | --- | --- |
| `HSET` | `HSET key field value [field value ...]` | Returns count of newly added fields (`RETURNING (xmax = 0)`) |
| `HMSET` | `HMSET key field value [field value ...]` | Multi-field set returning `+OK` |
| `HSETNX` | `HSETNX key field value` | Inserts field only if absent |
| `HGET` | `HGET key field` | Returns field value or nil |
| `HMGET` | `HMGET key field [field ...]` | Returns array of field values / nils |
| `HGETALL` | `HGETALL key` | Flat array in RESP2; native `%N` Map in RESP3 |
| `HDEL` | `HDEL key field [field ...]` | Deletes specified fields, returns count removed |
| `HEXISTS` | `HEXISTS key field` | Returns `1` if field exists, else `0` |
| `HLEN` | `HLEN key` | Number of fields in hash |
| `HKEYS` / `HVALS` | `HKEYS key` / `HVALS key` | Sorted list of field names / values |
| `HINCRBY` / `HINCRBYFLOAT` | `HINCRBY key field delta` | Atomic integer or float hash field increment |
| `HSCAN` | `HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]` | Cursor-based hash field iteration (used by BullMQ pagination & `ioredis`) |

## Lists

| Command | Syntax | Notes |
| --- | --- | --- |
| `LPUSH` / `RPUSH` | `LPUSH key elem [elem ...]` / `RPUSH key elem [elem ...]` | Bidirectional push using signed `BIGINT` `seq` ordering |
| `LPOP` / `RPOP` | `LPOP key [count]` / `RPOP key [count]` | Pops from head (`MIN(seq)`) or tail (`MAX(seq)`) |
| `LRANGE` | `LRANGE key start stop` | Inclusive range with negative index clamping |
| `LLEN` | `LLEN key` | Total number of elements in list |
| `LINDEX` | `LINDEX key index` | Zero-based element lookup (supports negative indices) |
| `LSET` | `LSET key index value` | Updates element at index |
| `LREM` | `LREM key count element` | Removes matching elements from head, tail, or all |
| `LTRIM` | `LTRIM key start stop` | Trims list to the inclusive index range |
| `LPOS` | `LPOS key elem [RANK r] [COUNT c] [MAXLEN m]` | Finds 0-based index(es) of matching element |
| `RPOPLPUSH` / `LMOVE` | `RPOPLPUSH src dst` / `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | Atomically pops from source and pushes to destination |
| `BLPOP` / `BRPOP` | `BLPOP key [key ...] timeout` | Blocking head/tail pop woken asynchronously via `KeyNotifier` |

## Sorted Sets

| Command | Syntax | Notes |
| --- | --- | --- |
| `ZADD` | `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [...]` | Supports `+inf`/`-inf` and all modifier flags; wakes `BZPOPMIN`/`BZPOPMAX` |
| `ZRANGE` | `ZRANGE key start stop [BYSCORE] [REV] [LIMIT off cnt] [WITHSCORES]` | Lexicographical tie-breaking; RESP2 flat array / RESP3 scored tuples |
| `ZREVRANGE` | `ZREVRANGE key start stop [WITHSCORES]` | Descending score + member order |
| `ZRANGEBYSCORE` / `ZREVRANGEBYSCORE` | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT off cnt]` | Supports `-inf`, `+inf`, and exclusive bounds `(x` |
| `ZREM` | `ZREM key member [member ...]` | Removes members, returns deleted count |
| `ZREMRANGEBYRANK` / `ZREMRANGEBYSCORE` | `ZREMRANGEBYRANK key start stop` / `ZREMRANGEBYSCORE key min max` | Bulk range deletion by rank or score |
| `ZPOPMIN` / `ZPOPMAX` | `ZPOPMIN key [count]` / `ZPOPMAX key [count]` | Atomically pops lowest/highest scored members |
| `BZPOPMIN` / `BZPOPMAX` | `BZPOPMIN key [key ...] timeout` | Cooperative async blocking pop (supports float seconds & `0` infinite wait) |
| `ZSCORE` | `ZSCORE key member` | Returns member score (`Double` in RESP3, `BulkString` in RESP2) |
| `ZCARD` | `ZCARD key` | Cardinality of sorted set |
| `ZRANK` / `ZREVRANK` | `ZRANK key member [WITHSCORE]` | 0-based ascending / descending rank |
| `ZINCRBY` | `ZINCRBY key increment member` | Increments member score (inserts if absent) |
| `ZCOUNT` | `ZCOUNT key min max` | Count of members within inclusive/exclusive score bounds |
| `ZSCAN` | `ZSCAN key cursor [MATCH pattern] [COUNT count]` | Cursor-based sorted set iteration returning `[member, score, ...]` |

## Sets

| Command | Syntax | Notes |
| --- | --- | --- |
| `SADD` | `SADD key member [member ...]` | Inserts unique set members into `pg_redis_sets` |
| `SREM` | `SREM key member [member ...]` | Removes members, returns deleted count |
| `SMEMBERS` | `SMEMBERS key` | Returns all members in set |
| `SISMEMBER` | `SISMEMBER key member` | Returns `1` if member exists, else `0` |
| `SCARD` | `SCARD key` | Cardinality of set |
| `SPOP` | `SPOP key [count]` | Atomically removes and returns member(s) |
| `SSCAN` | `SSCAN key cursor [MATCH pattern] [COUNT count]` | Cursor-based set member iteration (used by BullMQ dependency pagination) |

## Streams

| Command | Syntax | Notes |
| --- | --- | --- |
| `XADD` | `XADD key [NOMKSTREAM] [MAXLEN\|MINID [=\|~] n [LIMIT m]] (*\|id) field val [...]` | Monotonic `<ms>-<seq>` IDs, approximate/exact trim, wakes `XREAD BLOCK` |
| `XTRIM` | `XTRIM key MAXLEN\|MINID [=\|~] threshold [LIMIT count]` | Trims oldest stream entries |
| `XREAD` | `XREAD [COUNT n] [BLOCK ms] STREAMS key [...] id [...]` | Supports `$`, `0-0`, explicit IDs, and cooperative async `BLOCK` |
| `XRANGE` / `XREVRANGE` | `XRANGE key start end [COUNT n]` | Range queries with `-`, `+`, and exclusive `(id` bounds |
| `XLEN` | `XLEN key` | Total number of entries in stream |
| `XDEL` | `XDEL key id [id ...]` | Deletes stream entries by ID |

## Lua 5.1 Scripting

| Command | Syntax | Notes |
| --- | --- | --- |
| `EVAL` / `EVAL_RO` | `EVAL script numkeys [key ...] [arg ...]` | Executes Lua 5.1 script atomically inside a single PostgreSQL SPI transaction |
| `EVALSHA` / `EVALSHA_RO` | `EVALSHA sha1 numkeys [key ...] [arg ...]` | Runs cached compiled Lua bytecode chunk (`NOSCRIPT` on cache miss) |
| `SCRIPT LOAD` | `SCRIPT LOAD script` | Compiles and caches script by 40-char lowercase SHA1 hex digest |
| `SCRIPT EXISTS` | `SCRIPT EXISTS sha1 [sha1 ...]` | Checks which SHA1 digests are cached |
| `SCRIPT FLUSH` | `SCRIPT FLUSH` | Clears the compiled Lua script cache |

!!! info "Sandboxed Lua 5.1 Builtins (Exact Redis Sandbox Parity)"
    Powered by vendored **Lua 5.1 (`mlua`)** restricted to safe stdlibs (`table`, `string`, `math` — `os`, `io`, `package`, `debug`, `dofile`, `loadfile`, and `require` are disabled) with configurable `pg_redis.lua_time_limit_ms` timeout protection. Exposes `KEYS`, `ARGV` (binary-safe), `redis.call`, `redis.pcall`, `redis.status_reply`, `redis.error_reply`, `redis.sha1hex`, `redis.log`, **`cjson`**, **`cmsgpack`**, and **`bit`**.

## Pub/Sub (Named Channels & Glob Patterns)

| Command | Syntax | Notes |
| --- | --- | --- |
| `PUBLISH` | `PUBLISH channel message` | Binary-safe, arbitrary payload size (no 8 KB PG `NOTIFY` limit); delivers to matching named & pattern subscribers and returns total count; works inside Lua & `MULTI`/`EXEC` |
| `SUBSCRIBE` | `SUBSCRIBE channel [channel ...]` | Emits `["subscribe", ch, total_count]` per channel (idempotent on duplicates); RESP3 Push (`>`) frames |
| `UNSUBSCRIBE` | `UNSUBSCRIBE [channel ...]` | Unsubscribes specific channels or all channels (0 args) down to remaining pattern count |
| `PSUBSCRIBE` | `PSUBSCRIBE pattern [pattern ...]` | Glob-pattern subscriptions emitting `["pmessage", pattern, channel, payload]` on match |
| `PUNSUBSCRIBE` | `PUNSUBSCRIBE [pattern ...]` | Unsubscribes specific patterns or all patterns (0 args) |
| `PUBSUB` | `PUBSUB CHANNELS [pattern] \| NUMSUB [ch ...] \| NUMPAT` | Inspect active named channels, live subscriber counts, and active pattern subscriptions |

!!! note "Pub/Sub Scope & RESP2 Subscribed Mode"
    Named-channel (`SUBSCRIBE`/`UNSUBSCRIBE`) and pattern (`PSUBSCRIBE`/`PUNSUBSCRIBE`) Pub/Sub run in-process inside the background worker's Tokio `LocalSet` over bounded queues (`pg_redis.pubsub_queue_size`) with zero SPI overhead and automatic cleanup on disconnect or slow-consumer overflow. In **RESP2** subscribed mode, only `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PING` (replying `["pong", msg]`), `QUIT`, and `RESET` are permitted; **RESP3** permits concurrent non-Pub/Sub commands on the same connection. Sharded Pub/Sub (`SSUBSCRIBE`/`SUNSUBSCRIBE`) is not implemented.

## Transactions & Server / Protocol

| Command | Syntax | Notes |
| --- | --- | --- |
| `MULTI` / `EXEC` / `DISCARD` | `MULTI` ... `EXEC` | Atomic command queueing executed in a single SPI transaction |
| `AUTH` | `AUTH [default] password` | Constant-time password verification against `pg_redis.requirepass` (`NOAUTH` / `WRONGPASS`) |
| `HELLO` | `HELLO [2\|3 [AUTH u p] [SETNAME name]]` | Dynamic RESP2 / RESP3 protocol negotiation & inline authentication |
| `PING` / `ECHO` | `PING [msg]` / `ECHO msg` | Resolved in memory without SPI overhead (returns `["pong", msg]` in RESP2 subscribed mode) |
| `RESET` | `RESET` | Unsubscribes all channels/patterns, clears `MULTI` state, resets auth & protocol to RESP2, returns `+RESET` |
| `CLIENT` | `CLIENT SETINFO \| SETNAME \| GETNAME \| ID \| LIST \| INFO \| KILL \| MAINT_NOTIFICATIONS \| NO-EVICT \| NO-TOUCH` | Full multi-connection registry (`LIST [TYPE ...] [ID ...]`, `KILL [ID\|ADDR\|ip:port]`) for BullMQ `getWorkers()`, `go-redis/v9`, `redis-py`, `ioredis`, and `Jedis` |
| `INFO` | `INFO [section]` | Reports `pg_redis_core` version, `tcp_port`, `tls_port`, `# Clients` (`connected_clients`, `maxclients`), `# Memory` (`maxmemory_policy:noeviction`), `# Stats` (L1 cache hit/miss counters), and `# Replication` |
| `COMMAND` | `COMMAND [COUNT \| DOCS \| LIST]` | `redis-cli` interactive startup compatibility (returns all 115 supported commands) |
| `CONFIG GET` | `CONFIG GET pattern` | Glob-matched server config keys (`databases`, `save`, `appendonly`, `maxmemory-policy`, `maxclients`, `lua-time-limit`, `port`, `tls-port`, `tls-auth-clients`) |
| `SELECT` / `TIME` / `QUIT` | Standard Redis syntax | Session lifecycle commands |
