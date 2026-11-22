# Supported Redis Commands

`pg_redis_core` supports **85+ Redis commands** over both **RESP2** and **RESP3** (plus inline commands), covering Strings, Keys/Expiry, Hashes, Lists, Sorted Sets, Sets, Streams, Blocking Operations (`BZPOPMIN`, `XREAD BLOCK`), Embedded Lua 5.1 Scripting (`EVAL`/`EVALSHA`/`SCRIPT`), Transactions, and Driver Handshakes — including full **BullMQ 5.13.0 / Medusa** compatibility.

## Strings

| Command | Syntax | Notes |
| --- | --- | --- |
| `GET` | `GET key` | Binary-safe (`BYTEA`); served from L1 `moka` cache when hot |
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
| `DEL` / `UNLINK` | `DEL key [key ...]` | Removes keys across all 4 catalog tables; invalidates L1 cache |
| `EXISTS` | `EXISTS key [key ...]` | Checks active keys across all 4 catalog tables |
| `EXPIRE` / `PEXPIRE` | `EXPIRE key seconds` / `PEXPIRE key ms` | Sets relative expiration on string keys |
| `EXPIREAT` / `PEXPIREAT` | `EXPIREAT key unix_ts` / `PEXPIREAT key unix_ms` | Sets absolute expiration timestamp |
| `TTL` / `PTTL` | `TTL key` / `PTTL key` | Returns remaining TTL, `-1` (no expiry), or `-2` (missing) |
| `PERSIST` | `PERSIST key` | Removes expiration from key |
| `TYPE` | `TYPE key` | Returns `string`, `hash`, `list`, `zset`, `set`, `stream`, or `none` |
| `RENAME` | `RENAME key newkey` | Atomically renames key across all 6 catalog tables |
| `KEYS` | `KEYS pattern` | Supports Redis glob patterns (`*`, `?`, `[abc]`, `[^a]`, `[a-z]`, `\x`) |
| `SCAN` | `SCAN cursor [MATCH pattern] [COUNT count]` | Cursor-based key iteration |
| `DBSIZE` | `DBSIZE` | Total count of active keys across all catalog tables |
| `FLUSHDB` / `FLUSHALL` | `FLUSHDB` | Clears all 4 catalog tables and resets L1 cache |

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

## Sets

| Command | Syntax | Notes |
| --- | --- | --- |
| `SADD` | `SADD key member [member ...]` | Inserts unique set members into `pg_redis_sets` |
| `SREM` | `SREM key member [member ...]` | Removes members, returns deleted count |
| `SMEMBERS` | `SMEMBERS key` | Returns all members in set |
| `SISMEMBER` | `SISMEMBER key member` | Returns `1` if member exists, else `0` |
| `SCARD` | `SCARD key` | Cardinality of set |
| `SPOP` | `SPOP key [count]` | Atomically removes and returns member(s) |

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

!!! info "Included Lua 5.1 Builtins (Exact Redis Sandbox Parity)"
    Powered by vendored **Lua 5.1 (`mlua`)** inside the background worker with `KEYS`, `ARGV` (binary-safe), `redis.call`, `redis.pcall`, `redis.status_reply`, `redis.error_reply`, `redis.sha1hex`, `redis.log`, **`cjson`**, **`cmsgpack`**, and **`bit`**.

## Transactions & Server / Protocol

| Command | Syntax | Notes |
| --- | --- | --- |
| `MULTI` / `EXEC` / `DISCARD` | `MULTI` ... `EXEC` | Atomic command queueing executed in a single SPI transaction |
| `HELLO` | `HELLO [2\|3 [AUTH u p] [SETNAME name]]` | Dynamic RESP2 / RESP3 protocol negotiation |
| `PING` / `ECHO` | `PING [msg]` / `ECHO msg` | Resolved in memory without SPI overhead |
| `CLIENT` | `CLIENT SETINFO \| SETNAME \| GETNAME \| ID \| LIST \| INFO` | Full `go-redis/v9`, `redis-py`, and `Jedis` handshake support |
| `INFO` | `INFO [section]` | Reports `pg_redis_core` version, PG config, and L1 cache hit/miss stats |
| `COMMAND` | `COMMAND [COUNT \| DOCS \| LIST]` | `redis-cli` interactive startup compatibility |
| `CONFIG GET` | `CONFIG GET pattern` | Returns basic server config keys |
| `SELECT` / `AUTH` / `TIME` / `QUIT` | Standard Redis syntax | Session lifecycle commands |
