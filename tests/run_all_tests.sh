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
echo " 6. Running pgrx in-PostgreSQL tests (cargo pgrx test & run pg15)"
echo "============================================================"
docker build --target dev -f docker/Dockerfile -t pg_redis_core:dev .
docker run --rm pg_redis_core:dev sh -c "
set -e
cargo pgrx test pg15
printf \"CREATE EXTENSION IF NOT EXISTS pg_redis_core;\nSELECT redis_dispatcher('PING', NULL, NULL, NULL);\n\" | cargo pgrx run pg15 | grep -q '+PONG'
"

echo "============================================================"
echo " ALL ACCEPTANCE CRITERIA PASSED!"
echo "============================================================"
