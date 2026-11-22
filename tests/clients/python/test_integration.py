import asyncio
import os
import pytest
import psycopg
import redis.asyncio as aioredis
from httpx import ASGITransport, AsyncClient

from app import app, redis_client

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
PG_DSN = (
    f"host={os.getenv('PGHOST', 'localhost')} "
    f"port={os.getenv('PGPORT', '5432')} "
    f"dbname={os.getenv('PGDATABASE', 'postgres')} "
    f"user={os.getenv('PGUSER', 'postgres')} "
    f"password={os.getenv('PGPASSWORD', 'postgres')}"
)


async def make_redis(decode_responses: bool = True):
    return aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=decode_responses,
    )


@pytest.mark.asyncio
async def test_ping_and_echo():
    r = await make_redis()
    try:
        assert await r.ping() is True
        assert await r.echo("pg_redis_core") == "pg_redis_core"
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_fastapi_gateway_endpoints():
    # Exercises the exact FastAPI gateway pattern from RFC §6.1
    try:
        assert await redis_client.ping() is True
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/cache/fastapi:item:1", params={"value": "hello_fastapi"}
            )
            assert resp.status_code == 200
            assert resp.json() == {"status": "success", "key": "fastapi:item:1"}

            resp = await client.get("/cache/fastapi:item:1")
            assert resp.status_code == 200
            assert resp.json() == {"key": "fastapi:item:1", "value": "hello_fastapi"}

            missing = await client.get("/cache/fastapi:does_not_exist")
            assert missing.status_code == 404
    finally:
        await redis_client.aclose()


@pytest.mark.asyncio
async def test_strings_and_ttl_and_binary():
    r = await make_redis(decode_responses=True)
    r_bin = await make_redis(decode_responses=False)
    try:
        await r.flushdb()

        # Basic SET/GET/INCR/DECR/APPEND/STRLEN
        assert await r.set("str:greeting", "hello") is True
        assert await r.get("str:greeting") == "hello"
        assert await r.append("str:greeting", " world") == 11
        assert await r.get("str:greeting") == "hello world"
        assert await r.strlen("str:greeting") == 11

        # NX / XX / GET
        assert await r.set("str:greeting", "blocked", nx=True) is None
        assert await r.get("str:greeting") == "hello world"
        assert await r.set("str:greeting", "updated", xx=True) is True
        assert await r.get("str:greeting") == "updated"
        assert await r.set("str:only_xx", "nope", xx=True) is None

        # MSET / MGET
        assert await r.mset({"m:1": "one", "m:2": "two"}) is True
        assert await r.mget(["m:1", "m:2", "m:missing"]) == ["one", "two", None]

        # Counters
        assert await r.incr("cnt:views") == 1
        assert await r.incrby("cnt:views", 41) == 42
        assert await r.decr("cnt:views") == 41
        assert await r.decrby("cnt:views", 11) == 30

        # Binary safety (non-UTF8 BYTEA)
        payload = b"\x00\x01\x02\xff\xfe\x80binary\r\nbytes"
        assert await r_bin.set(b"bin:payload", payload) is True
        assert await r_bin.get(b"bin:payload") == payload

        # TTL / EXPIRE / PERSIST / expiration
        assert await r.set("ttl:ephemeral", "soon_gone", ex=2) is True
        ttl = await r.ttl("ttl:ephemeral")
        assert 0 < ttl <= 2
        assert await r.get("ttl:ephemeral") == "soon_gone"

        assert await r.set("ttl:persist_me", "stay", ex=5) is True
        assert await r.persist("ttl:persist_me") is True
        assert await r.ttl("ttl:persist_me") == -1

        await asyncio.sleep(2.3)
        assert await r.get("ttl:ephemeral") is None
        assert await r.ttl("ttl:ephemeral") == -2
        assert await r.get("ttl:persist_me") == "stay"

        # Verify SQL eviction function cleans expired rows
        with psycopg.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_redis_evict_expired()")
                evicted = cur.fetchone()[0]
                assert evicted >= 0
                cur.execute(
                    "SELECT COUNT(*) FROM pg_redis_strings WHERE key = 'ttl:ephemeral'"
                )
                assert cur.fetchone()[0] == 0
    finally:
        await r.aclose()
        await r_bin.aclose()


@pytest.mark.asyncio
async def test_hashes_full_lifecycle():
    r = await make_redis()
    try:
        await r.delete("user:42")
        added = await r.hset(
            "user:42",
            mapping={"name": "Ada", "role": "engineer", "score": "10"},
        )
        assert added == 3
        assert await r.hget("user:42", "name") == "Ada"
        assert await r.hget("user:42", "missing") is None
        assert await r.hmget("user:42", ["name", "role", "missing"]) == [
            "Ada",
            "engineer",
            None,
        ]
        assert await r.hgetall("user:42") == {
            "name": "Ada",
            "role": "engineer",
            "score": "10",
        }
        assert await r.hexists("user:42", "role") is True
        assert await r.hlen("user:42") == 3
        assert await r.hincrby("user:42", "score", 5) == 15
        assert await r.hset("user:42", "name", "Ada Lovelace") == 0  # updated, not new
        assert await r.hdel("user:42", "role", "nonexistent") == 1
        assert await r.hlen("user:42") == 2
        assert await r.type("user:42") == "hash"
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_lists_full_lifecycle():
    r = await make_redis()
    try:
        await r.delete("queue:jobs")
        assert await r.lpush("queue:jobs", "job1", "job2") == 2
        assert await r.rpush("queue:jobs", "job3", "job4") == 4
        # Expected order: job2, job1, job3, job4
        assert await r.lrange("queue:jobs", 0, -1) == ["job2", "job1", "job3", "job4"]
        assert await r.lrange("queue:jobs", 1, 2) == ["job1", "job3"]
        assert await r.lrange("queue:jobs", -2, -1) == ["job3", "job4"]
        assert await r.llen("queue:jobs") == 4
        assert await r.lindex("queue:jobs", 0) == "job2"
        assert await r.lindex("queue:jobs", -1) == "job4"
        assert await r.lset("queue:jobs", 1, "job1_edited") is True
        assert await r.lpop("queue:jobs") == "job2"
        assert await r.rpop("queue:jobs") == "job4"
        assert await r.lrange("queue:jobs", 0, -1) == ["job1_edited", "job3"]
        assert await r.type("queue:jobs") == "list"
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_zsets_full_lifecycle():
    r = await make_redis()
    try:
        await r.delete("leaderboard")
        added = await r.zadd(
            "leaderboard", {"alice": 100.0, "bob": 250.5, "carol": 100.0, "dave": 50.0}
        )
        assert added == 4
        # Ties broken lexicographically: dave(50), alice(100), carol(100), bob(250.5)
        assert await r.zrange("leaderboard", 0, -1) == ["dave", "alice", "carol", "bob"]
        assert await r.zrange("leaderboard", 0, -1, withscores=True) == [
            ("dave", 50.0),
            ("alice", 100.0),
            ("carol", 100.0),
            ("bob", 250.5),
        ]
        assert await r.zrevrange("leaderboard", 0, 1) == ["bob", "carol"]
        assert await r.zscore("leaderboard", "bob") == 250.5
        assert await r.zscore("leaderboard", "nobody") is None
        assert await r.zcard("leaderboard") == 4
        assert await r.zrank("leaderboard", "alice") == 1
        assert await r.zrevrank("leaderboard", "bob") == 0
        assert await r.zincrby("leaderboard", 200.0, "alice") == 300.0
        assert await r.zrevrange("leaderboard", 0, 0) == ["alice"]
        assert await r.zcount("leaderboard", 100, 300) == 3
        assert await r.zrangebyscore("leaderboard", 100, 260) == ["carol", "bob"]
        assert await r.zrem("leaderboard", "dave", "missing") == 1
        assert await r.zcard("leaderboard") == 3
        assert await r.type("leaderboard") == "zset"
    finally:
        await r.aclose()


@pytest.mark.asyncio
async def test_pipelining_and_l1_cache_and_postgres_sql_parity():
    r = await make_redis()
    try:
        await r.flushdb()

        # 1. Non-transactional pipeline batch (RFC §5.4)
        async with r.pipeline(transaction=False) as pipe:
            for i in range(25):
                pipe.set(f"pipe:k:{i}", f"val:{i}")
            results = await pipe.execute()
            assert results == [True] * 25

        # 2. Transactional MULTI/EXEC pipeline
        async with r.pipeline(transaction=True) as pipe:
            pipe.set("tx:a", "10")
            pipe.incr("tx:a")
            pipe.get("tx:a")
            tx_res = await pipe.execute()
            assert tx_res == [True, 11, "11"]

        # 3. Hot-key L1 cache hit verification (RFC §5.5)
        info_before = await r.info()
        hits_before = int(info_before.get("l1_cache_hits", 0))
        for _ in range(5):
            assert await r.get("pipe:k:0") == "val:0"
        info_after = await r.info()
        hits_after = int(info_after.get("l1_cache_hits", 0))
        assert hits_after > hits_before, "Expected L1 cache hits to increase on hot GETs"

        # Write-through invalidation check
        await r.set("pipe:k:0", "updated_val")
        assert await r.get("pipe:k:0") == "updated_val"

        # 4. Direct PostgreSQL relational table parity check (RFC §3 & §4.2)
        await r.hset("sql:hash", mapping={"f1": "v1"})
        await r.rpush("sql:list", "elem1", "elem2")
        await r.zadd("sql:zset", {"m1": 42.5})

        with psycopg.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT convert_from(value, 'UTF8') FROM pg_redis_strings WHERE key = 'pipe:k:0'"
                )
                assert cur.fetchone()[0] == "updated_val"

                cur.execute(
                    "SELECT value FROM pg_redis_hashes WHERE key = 'sql:hash' AND field = 'f1'"
                )
                assert cur.fetchone()[0] == "v1"

                cur.execute(
                    "SELECT value FROM pg_redis_lists WHERE key = 'sql:list' ORDER BY seq ASC"
                )
                assert [row[0] for row in cur.fetchall()] == ["elem1", "elem2"]

                cur.execute(
                    "SELECT member, score FROM pg_redis_zsets WHERE key = 'sql:zset'"
                )
                assert cur.fetchone() == ("m1", 42.5)

                # Direct SQL redis_dispatcher check (RFC §4.2)
                cur.execute("SELECT redis_dispatcher('PING', NULL, NULL, NULL)")
                assert cur.fetchone()[0] == "+PONG\r\n"
    finally:
        await r.aclose()
