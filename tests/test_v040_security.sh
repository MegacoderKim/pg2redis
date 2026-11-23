#!/usr/bin/env bash
# End-to-end verification for v0.4.0 security & resource-limit GUCs:
#   - pg_redis.requirepass (NOAUTH / WRONGPASS / AUTH / HELLO AUTH / RESET)
#   - pg_redis.lua_time_limit_ms (infinite loop termination & worker recovery)
#   - pg_redis.max_clients (excess connection rejection)
#   - pg_redis.max_frame_bytes (oversized request rejection)
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_MAJOR="${1:-15}"
NET_NAME="pg2redis-v040-sec-net"
CONTAINER="pg2redis-v040-sec-pg${PG_MAJOR}"
IMAGE="pg2redis:pg${PG_MAJOR}"
REDIS_PASS="s3cr3t_v040"

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NET_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
docker network rm "$NET_NAME" >/dev/null 2>&1 || true
docker network create "$NET_NAME" >/dev/null

docker build --build-arg "PG_MAJOR=${PG_MAJOR}" -f docker/Dockerfile --target runtime -t "$IMAGE" .
docker build -t pg2redis-test-pubsub:local ./tests/clients/pubsub

docker run -d \
    --name "$CONTAINER" \
    --network "$NET_NAME" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    "$IMAGE" \
    postgres \
    -c shared_preload_libraries=pg_redis_core \
    -c max_worker_processes=16 \
    -c pg_redis.bind_address=0.0.0.0 \
    -c pg_redis.port=6379 \
    -c pg_redis.database=postgres \
    -c "pg_redis.requirepass=${REDIS_PASS}" \
    -c pg_redis.lua_time_limit_ms=300 \
    -c pg_redis.max_clients=4 \
    -c pg_redis.max_frame_bytes=16384 >/dev/null

ready=0
for _ in $(seq 1 40); do
    if docker exec "$CONTAINER" sh -c "pg_isready -U postgres -d postgres >/dev/null 2>&1 && redis-cli -a '${REDIS_PASS}' --no-auth-warning PING 2>/dev/null | grep -q PONG"; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo "ERROR: Security test container failed to start. Logs:"
    docker logs "$CONTAINER"
    exit 1
fi

docker run --rm --network "$NET_NAME" \
    -e "REDIS_HOST=${CONTAINER}" \
    -e "REDIS_PORT=6379" \
    -e "REDIS_PASS=${REDIS_PASS}" \
    pg2redis-test-pubsub:local \
    node test_security.mjs
