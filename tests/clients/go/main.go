package main

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

func must(err error, msg string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL [%s]: %v\n", msg, err)
		os.Exit(1)
	}
}

func assertEqual(got, want any, label string) {
	if !reflect.DeepEqual(got, want) {
		fmt.Fprintf(os.Stderr, "FAIL [%s]: got=%#v want=%#v\n", label, got, want)
		os.Exit(1)
	}
}

func main() {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Default go-redis/v9 client (uses RESP3 HELLO 3 + CLIENT SETINFO handshake)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	// 1. Exact RFC §6.3 verification
	must(rdb.Set(ctx, "go_key", "hello from golang", 0).Err(), "SET go_key")
	val, err := rdb.Get(ctx, "go_key").Result()
	must(err, "GET go_key")
	assertEqual(val, "hello from golang", "go_key value")
	fmt.Printf("go_key: %s\n", val)

	// 2. Strings + TTL + INCR
	must(rdb.Set(ctx, "go:ttl_key", "expiring", 2*time.Second).Err(), "SET with TTL")
	ttl, err := rdb.TTL(ctx, "go:ttl_key").Result()
	must(err, "TTL go:ttl_key")
	if ttl <= 0 {
		fmt.Fprintf(os.Stderr, "FAIL [TTL]: expected positive TTL, got %v\n", ttl)
		os.Exit(1)
	}
	must(rdb.Del(ctx, "go:counter").Err(), "DEL go:counter")
	cnt, err := rdb.IncrBy(ctx, "go:counter", 7).Result()
	must(err, "INCRBY go:counter")
	assertEqual(cnt, int64(7), "INCRBY result")

	// 3. Hashes (HSET / HGETALL / HGET / HDEL over RESP3 Map)
	must(rdb.Del(ctx, "go:hash").Err(), "DEL go:hash")
	added, err := rdb.HSet(ctx, "go:hash", "lang", "go", "driver", "go-redis-v9").Result()
	must(err, "HSET go:hash")
	assertEqual(added, int64(2), "HSET count")

	hmap, err := rdb.HGetAll(ctx, "go:hash").Result()
	must(err, "HGETALL go:hash")
	assertEqual(hmap, map[string]string{"lang": "go", "driver": "go-redis-v9"}, "HGETALL map")

	// 4. Lists (LPUSH / RPUSH / LRANGE / LPOP)
	must(rdb.Del(ctx, "go:list").Err(), "DEL go:list")
	must(rdb.LPush(ctx, "go:list", "b", "a").Err(), "LPUSH go:list")
	must(rdb.RPush(ctx, "go:list", "c").Err(), "RPUSH go:list")
	items, err := rdb.LRange(ctx, "go:list", 0, -1).Result()
	must(err, "LRANGE go:list")
	assertEqual(items, []string{"a", "b", "c"}, "LRANGE items")

	// 5. Sorted Sets (ZADD / ZRANGE / ZRANGE WITHSCORES / ZSCORE)
	must(rdb.Del(ctx, "go:zset").Err(), "DEL go:zset")
	zAdded, err := rdb.ZAdd(ctx, "go:zset",
		redis.Z{Score: 10.5, Member: "alpha"},
		redis.Z{Score: 5.0, Member: "beta"},
		redis.Z{Score: 20.0, Member: "gamma"},
	).Result()
	must(err, "ZADD go:zset")
	assertEqual(zAdded, int64(3), "ZADD count")

	zMembers, err := rdb.ZRange(ctx, "go:zset", 0, -1).Result()
	must(err, "ZRANGE go:zset")
	assertEqual(zMembers, []string{"beta", "alpha", "gamma"}, "ZRANGE members")

	zScored, err := rdb.ZRangeWithScores(ctx, "go:zset", 0, -1).Result()
	must(err, "ZRANGE WITHSCORES go:zset")
	assertEqual(zScored, []redis.Z{
		{Score: 5.0, Member: "beta"},
		{Score: 10.5, Member: "alpha"},
		{Score: 20.0, Member: "gamma"},
	}, "ZRANGE WITHSCORES")

	// 6. Pipelining & TxPipeline
	pipe := rdb.Pipeline()
	pipe.Set(ctx, "go:pipe:1", "v1", 0)
	pipe.Set(ctx, "go:pipe:2", "v2", 0)
	pGet := pipe.Get(ctx, "go:pipe:2")
	_, err = pipe.Exec(ctx)
	must(err, "Pipeline Exec")
	assertEqual(pGet.Val(), "v2", "Pipeline GET")

	tx := rdb.TxPipeline()
	tx.Set(ctx, "go:tx:1", "100", 0)
	txIncr := tx.Incr(ctx, "go:tx:1")
	_, err = tx.Exec(ctx)
	must(err, "TxPipeline Exec")
	assertEqual(txIncr.Val(), int64(101), "TxPipeline INCR")

	fmt.Println("PASS: All go-redis/v9 (RESP3) integration tests succeeded!")
}
