#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cleanup() {
    echo "==> Cleaning up docker compose test stack..."
    docker compose --profile test --profile gateway down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "============================================================"
echo " 1. Building pg_redis_core runtime image & starting service"
echo "============================================================"
docker compose up -d --build --wait pg-redis

echo "============================================================"
echo " 2. Running Raw TCP RESP compliance test (RFC §7.2)"
echo "============================================================"
bash ./tests/raw_resp_test.sh

echo "============================================================"
echo " 3. Running Python (FastAPI + redis-py + psycopg) test suite"
echo "============================================================"
docker compose --profile test run --rm --build test-python

echo "============================================================"
echo " 4. Running Go (go-redis/v9 RESP3) test suite"
echo "============================================================"
docker compose --profile test run --rm --build test-go

echo "============================================================"
echo " 5. Running Kotlin (Jedis) test suite"
echo "============================================================"
docker compose --profile test run --rm --build test-kotlin

echo "============================================================"
echo " 6. Running Node.js BullMQ 5.13.0 (Medusa / Lua 5.1 / XADD / BZPOPMIN)"
echo "============================================================"
docker compose --profile test run --rm --build test-bullmq

echo "============================================================"
echo " 7. Running Node.js ioredis 5.11.1 Pub/Sub & v0.4.0 suite"
echo "============================================================"
docker compose --profile test run --rm --build test-pubsub

echo "============================================================"
echo " 8. Running v0.4.0 Security & Limits suite (AUTH, Lua, limits)"
echo "============================================================"
bash ./tests/test_v040_security.sh 15

echo "============================================================"
echo " 9. Running pgrx in-PostgreSQL tests (cargo pgrx test & run pg15)"
echo "============================================================"
docker build --target dev --build-arg PG_MAJOR=15 -f docker/Dockerfile -t pg_redis_core:dev-pg15 .
docker run --rm pg_redis_core:dev-pg15 sh -c "
set -e
cargo pgrx test pg15 --no-default-features --features pg15
printf \"CREATE EXTENSION IF NOT EXISTS pg_redis_core;\nSELECT redis_dispatcher('PING', NULL, NULL, NULL);\n\" | cargo pgrx run pg15 | grep -q '+PONG'
"

echo "============================================================"
echo " ALL ACCEPTANCE CRITERIA PASSED!"
echo "============================================================"
