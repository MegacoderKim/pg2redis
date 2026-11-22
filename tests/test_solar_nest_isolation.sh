#!/usr/bin/env bash
# End-to-end verification for Solar Nest's deployment topology across BOTH
# PostgreSQL 15 (Fly staging) and PostgreSQL 16 (local development):
#   - Application database: solar_nest (kept clean, zero pg_redis_* tables)
#   - Redis / queue database: solar_nest_redis (pg_redis.database=solar_nest_redis)
#   - Runs both ioredis 5.11.1 Pub/Sub suite and BullMQ 5.13.0 regression suite
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_VERSIONS=("$@")
if [ ${#PG_VERSIONS[@]} -eq 0 ]; then
    PG_VERSIONS=(15 16)
fi

NET_NAME="pg2redis-solarnest-test-net"
INIT_SQL="$(mktemp "${TMPDIR:-/tmp}/solar-nest-init.XXXXXX.sql")"

cat <<'EOF' > "$INIT_SQL"
-- Application database (solar_nest) has normal application tables only (NO pg_redis_core)
CREATE TABLE IF NOT EXISTS solar_nest_app_check (
    id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT now()
);
INSERT INTO solar_nest_app_check (id) VALUES ('app-db-isolated') ON CONFLICT DO NOTHING;

-- Dedicated Redis / queue database (solar_nest_redis) hosts pg_redis_core
CREATE DATABASE solar_nest_redis;
\connect solar_nest_redis
CREATE EXTENSION IF NOT EXISTS pg_redis_core;
EOF
chmod 644 "$INIT_SQL"

cleanup() {
    for pg in "${PG_VERSIONS[@]}"; do
        docker rm -f "pg2redis-solarnest-pg${pg}" >/dev/null 2>&1 || true
    done
    docker network rm "$NET_NAME" >/dev/null 2>&1 || true
    rm -f "$INIT_SQL"
}
trap cleanup EXIT

docker network rm "$NET_NAME" >/dev/null 2>&1 || true
docker network create "$NET_NAME" >/dev/null

echo "==> Building Node.js test images (ioredis 5.11.1 Pub/Sub + BullMQ 5.13.0)..."
docker build -t pg2redis-test-pubsub:local ./tests/clients/pubsub
docker build -t pg2redis-test-bullmq:local ./tests/clients/bullmq

for pg in "${PG_VERSIONS[@]}"; do
    CONTAINER="pg2redis-solarnest-pg${pg}"
    IMAGE="pg2redis:pg${pg}"

    echo "============================================================"
    echo " Testing PostgreSQL ${pg} with pg_redis.database=solar_nest_redis"
    echo "============================================================"

    docker build --build-arg "PG_MAJOR=${pg}" -f docker/Dockerfile --target runtime -t "$IMAGE" .

    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker run -d \
        --name "$CONTAINER" \
        --network "$NET_NAME" \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=solar_nest \
        -v "${INIT_SQL}:/docker-entrypoint-initdb.d/01-init-pg-redis.sql:ro" \
        "$IMAGE" \
        postgres \
        -c shared_preload_libraries=pg_redis_core \
        -c max_worker_processes=16 \
        -c pg_redis.bind_address=0.0.0.0 \
        -c pg_redis.port=6379 \
        -c pg_redis.database=solar_nest_redis >/dev/null

    echo "--> Waiting for PostgreSQL ${pg} + pg_redis_core RESP listener..."
    ready=0
    for _ in $(seq 1 40); do
        if docker exec "$CONTAINER" sh -c "pg_isready -U postgres -d solar_nest_redis >/dev/null 2>&1 && redis-cli -h 127.0.0.1 -p 6379 PING 2>/dev/null | grep -q PONG"; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "$ready" -ne 1 ]; then
        echo "ERROR: PostgreSQL ${pg} container failed to become ready. Logs:"
        docker logs "$CONTAINER"
        exit 1
    fi

    echo "--> Verifying PostgreSQL major version & GUC..."
    PG_VER_OUT=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc "SHOW server_version;")
    GUC_DB_OUT=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc "SHOW pg_redis.database;")
    echo "    server_version=${PG_VER_OUT} | pg_redis.database=${GUC_DB_OUT}"
    if [[ "$PG_VER_OUT" != "${pg}."* ]]; then
        echo "ERROR: Expected PostgreSQL ${pg}.x, got ${PG_VER_OUT}"
        exit 1
    fi
    if [[ "$GUC_DB_OUT" != "solar_nest_redis" ]]; then
        echo "ERROR: Expected pg_redis.database=solar_nest_redis, got ${GUC_DB_OUT}"
        exit 1
    fi

    echo "--> Running ioredis 5.11.1 Pub/Sub suite against PostgreSQL ${pg}..."
    docker run --rm --network "$NET_NAME" \
        -e "REDIS_HOST=${CONTAINER}" \
        -e "REDIS_PORT=6379" \
        pg2redis-test-pubsub:local

    echo "--> Running BullMQ 5.13.0 regression suite against PostgreSQL ${pg}..."
    docker run --rm --network "$NET_NAME" \
        -e "REDIS_HOST=${CONTAINER}" \
        -e "REDIS_PORT=6379" \
        pg2redis-test-bullmq:local

    echo "--> Verifying database isolation (solar_nest_redis vs solar_nest)..."
    REDIS_DB_TABLES=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'pg_redis_%';")
    REDIS_DB_HASH_ROWS=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc \
        "SELECT count(*) FROM pg_redis_hashes;")
    REDIS_DB_STREAM_ROWS=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc \
        "SELECT count(*) FROM pg_redis_streams;")
    REDIS_DB_STRING_ROWS=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest_redis -tAc \
        "SELECT count(*) FROM pg_redis_strings;")

    APP_DB_REDIS_TABLES=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest -tAc \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'pg_redis_%';")
    APP_DB_EXT=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest -tAc \
        "SELECT count(*) FROM pg_extension WHERE extname='pg_redis_core';")
    APP_DB_CHECK=$(docker exec "$CONTAINER" psql -U postgres -d solar_nest -tAc \
        "SELECT count(*) FROM solar_nest_app_check;")

    echo "    solar_nest_redis: pg_redis_* tables=${REDIS_DB_TABLES}, hashes=${REDIS_DB_HASH_ROWS}, streams=${REDIS_DB_STREAM_ROWS}, strings=${REDIS_DB_STRING_ROWS}"
    echo "    solar_nest (app): pg_redis_* tables=${APP_DB_REDIS_TABLES}, pg_redis_core ext=${APP_DB_EXT}, app_check rows=${APP_DB_CHECK}"

    if [ "$REDIS_DB_TABLES" -lt 6 ]; then
        echo "ERROR: Expected >= 6 pg_redis_* tables in solar_nest_redis, got ${REDIS_DB_TABLES}"
        exit 1
    fi
    if [ "$REDIS_DB_HASH_ROWS" -lt 1 ] || [ "$REDIS_DB_STREAM_ROWS" -lt 1 ]; then
        echo "ERROR: Expected BullMQ hash & stream rows in solar_nest_redis"
        exit 1
    fi
    if [ "$APP_DB_REDIS_TABLES" -ne 0 ] || [ "$APP_DB_EXT" -ne 0 ] || [ "$APP_DB_CHECK" -ne 1 ]; then
        echo "ERROR: Database isolation violated in solar_nest!"
        exit 1
    fi

    echo "    PASS: PostgreSQL ${pg} isolation, Pub/Sub (ioredis 5.11.1), and BullMQ 5.13.0 verified!"
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
done

echo "============================================================"
echo " ALL SOLAR NEST PG15 & PG16 ISOLATION + PUBSUB + BULLMQ TESTS PASSED!"
echo "============================================================"
