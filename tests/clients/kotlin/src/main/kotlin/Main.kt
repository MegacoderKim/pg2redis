import redis.clients.jedis.Jedis

object RedisPostgresClient {
    private val host = System.getenv("REDIS_HOST") ?: "localhost"
    private val port = (System.getenv("REDIS_PORT") ?: "6379").toInt()
    private val jedis = Jedis(host, port)

    fun setKey(key: String, value: String): String? = jedis.set(key, value)
    fun getKey(key: String): String? = jedis.get(key)
    fun client(): Jedis = jedis
}

fun assertEq(actual: Any?, expected: Any?, label: String) {
    if (actual != expected) {
        System.err.println("FAIL [$label]: actual=$actual expected=$expected")
        kotlin.system.exitProcess(1)
    }
}

fun main() {
    // 1. Exact RFC §6.2 verification
    assertEq(
        RedisPostgresClient.setKey("user:1001:session", "active_token_xyz"),
        "OK",
        "SET user:1001:session"
    )
    val session = RedisPostgresClient.getKey("user:1001:session")
    assertEq(session, "active_token_xyz", "GET user:1001:session")
    println("Retrieved from Postgres-Redis: $session")

    val jedis = RedisPostgresClient.client()

    // 2. PING & Strings
    assertEq(jedis.ping(), "PONG", "PING")
    jedis.del("kt:counter")
    assertEq(jedis.incr("kt:counter"), 1L, "INCR")
    assertEq(jedis.incrBy("kt:counter", 4L), 5L, "INCRBY")

    // 3. Hashes
    jedis.del("kt:hash")
    assertEq(
        jedis.hset("kt:hash", mapOf("lang" to "kotlin", "driver" to "jedis")),
        2L,
        "HSET"
    )
    assertEq(
        jedis.hgetAll("kt:hash"),
        mapOf("lang" to "kotlin", "driver" to "jedis"),
        "HGETALL"
    )

    // 4. Lists
    jedis.del("kt:list")
    assertEq(jedis.lpush("kt:list", "second", "first"), 2L, "LPUSH")
    assertEq(jedis.rpush("kt:list", "third"), 3L, "RPUSH")
    assertEq(jedis.lrange("kt:list", 0, -1), listOf("first", "second", "third"), "LRANGE")

    // 5. Sorted Sets
    jedis.del("kt:zset")
    assertEq(
        jedis.zadd("kt:zset", mapOf("alice" to 10.0, "bob" to 30.0, "carol" to 20.0)),
        3L,
        "ZADD"
    )
    assertEq(jedis.zrange("kt:zset", 0, -1), listOf("alice", "carol", "bob"), "ZRANGE")
    assertEq(jedis.zscore("kt:zset", "carol"), 20.0, "ZSCORE")

    // 6. Pipelining
    val pipe = jedis.pipelined()
    pipe.set("kt:pipe:1", "one")
    val pResp = pipe.get("kt:pipe:1")
    pipe.sync()
    assertEq(pResp.get(), "one", "Pipeline GET")

    jedis.close()
    println("PASS: All Kotlin/Jedis integration tests succeeded!")
}
