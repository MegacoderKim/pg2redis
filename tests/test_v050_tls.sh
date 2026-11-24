#!/usr/bin/env bash
# End-to-end verification for v0.5.0 optional native TLS (rediss://) & mTLS:
#   1. Dual-port mode (pg_redis.port=6379 + pg_redis.tls_port=6380 + requirepass)
#      - Plain TCP & TLS clients share state, cross-transport Pub/Sub, BZPOPMIN over TLS
#      - redis-cli --tls --cacert verification
#      - Plaintext garbage on TLS port cleanly rejected
#   2. TLS-only + mTLS mode (pg_redis.port=0 + pg_redis.tls_port=6380 + tls_auth_clients=yes)
#      - Plain TCP port 6379 refused (ECONNREFUSED)
#      - Client without cert rejected
#      - Client with CA-signed cert succeeds (PING, SET/GET, Lua EVAL)
#   3. Live SIGHUP certificate hot-reload (SELECT pg_reload_conf()) without restart
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PG_MAJOR="${1:-15}"
NET_NAME="pg2redis-v050-tls-net"
CONTAINER_DUAL="pg2redis-tls-dual-pg${PG_MAJOR}"
CONTAINER_MTLS="pg2redis-tls-mtls-pg${PG_MAJOR}"
IMAGE="pg2redis:pg${PG_MAJOR}"
REDIS_PASS="s3cr3t_tls_v050"
CERT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pg2redis-certs.XXXXXX")"
chmod 755 "$CERT_DIR"

cleanup() {
    docker rm -f "$CONTAINER_DUAL" "$CONTAINER_MTLS" >/dev/null 2>&1 || true
    docker network rm "$NET_NAME" >/dev/null 2>&1 || true
    rm -rf "$CERT_DIR"
}
trap cleanup EXIT

echo "==> Generating ephemeral CA, server (v1 & v2), and client mTLS certificates..."
# CA
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -subj "/CN=pg2redis-test-ca" \
    -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" 2>/dev/null

gen_server_cert() {
    local prefix="$1"
    local serial="$2"
    local cn="$3"
    cat > "$CERT_DIR/${prefix}.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:${CONTAINER_DUAL},DNS:${CONTAINER_MTLS},DNS:localhost,IP:127.0.0.1
EOF
    openssl req -newkey rsa:2048 -nodes \
        -subj "/CN=${cn}" \
        -keyout "$CERT_DIR/${prefix}.key" -out "$CERT_DIR/${prefix}.csr" 2>/dev/null
    openssl x509 -req -in "$CERT_DIR/${prefix}.csr" \
        -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -set_serial "$serial" \
        -days 2 -extfile "$CERT_DIR/${prefix}.ext" \
        -out "$CERT_DIR/${prefix}.crt" 2>/dev/null
}

# Server cert v1 (serial 0x1001) and v2 (serial 0x2002) for SIGHUP hot-reload test
gen_server_cert "server" "0x1001" "pg2redis-server-v1"
gen_server_cert "server_v2" "0x2002" "pg2redis-server-v2"

# Client cert for mTLS
cat > "$CERT_DIR/client.ext" <<EOF
basicConstraints=CA:FALSE
keyUsage=digitalSignature
extendedKeyUsage=clientAuth
EOF
openssl req -newkey rsa:2048 -nodes \
    -subj "/CN=pg2redis-mtls-client" \
    -keyout "$CERT_DIR/client.key" -out "$CERT_DIR/client.csr" 2>/dev/null
openssl x509 -req -in "$CERT_DIR/client.csr" \
    -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -set_serial "0x3003" \
    -days 2 -extfile "$CERT_DIR/client.ext" \
    -out "$CERT_DIR/client.crt" 2>/dev/null

# Ensure postgres user inside container can read mounted certs
chmod 644 "$CERT_DIR"/*.crt "$CERT_DIR"/*.key

docker rm -f "$CONTAINER_DUAL" "$CONTAINER_MTLS" >/dev/null 2>&1 || true
docker network rm "$NET_NAME" >/dev/null 2>&1 || true
docker network create "$NET_NAME" >/dev/null

docker build --build-arg "PG_MAJOR=${PG_MAJOR}" -f docker/Dockerfile --target runtime -t "$IMAGE" .
docker build -t pg2redis-test-pubsub:local ./tests/clients/pubsub

echo "==> Starting Mode 1: Dual-Port container (plain :6379 + TLS :6380 + requirepass)..."
docker run -d \
    --name "$CONTAINER_DUAL" \
    --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    "$IMAGE" \
    postgres \
    -c shared_preload_libraries=pg_redis_core \
    -c max_worker_processes=16 \
    -c pg_redis.bind_address=0.0.0.0 \
    -c pg_redis.port=6379 \
    -c pg_redis.tls_port=6380 \
    -c pg_redis.tls_cert_file=/certs/server.crt \
    -c pg_redis.tls_key_file=/certs/server.key \
    -c pg_redis.tls_auth_clients=no \
    -c pg_redis.database=postgres \
    -c "pg_redis.requirepass=${REDIS_PASS}" >/dev/null

ready=0
for _ in $(seq 1 40); do
    if docker exec "$CONTAINER_DUAL" sh -c "pg_isready -U postgres -d postgres >/dev/null 2>&1 && redis-cli --tls --cacert /certs/ca.crt -h localhost -p 6380 -a '${REDIS_PASS}' --no-auth-warning PING 2>/dev/null | grep -q PONG"; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo "ERROR: Dual-port TLS container failed to start. Logs:"
    docker logs "$CONTAINER_DUAL"
    exit 1
fi
echo "  [PASS] redis-cli --tls --cacert PING -> PONG succeeded!"

docker run --rm --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e "TLS_TEST_MODE=dual" \
    -e "REDIS_HOST=${CONTAINER_DUAL}" \
    -e "REDIS_PLAIN_PORT=6379" \
    -e "REDIS_TLS_PORT=6380" \
    -e "REDIS_PASS=${REDIS_PASS}" \
    -e "CERT_DIR=/certs" \
    pg2redis-test-pubsub:local \
    node test_tls.mjs

docker rm -f "$CONTAINER_DUAL" >/dev/null 2>&1 || true

echo "==> Starting Mode 2: TLS-Only + mTLS container (port=0, tls_port=6380, tls_auth_clients=yes)..."
docker run -d \
    --name "$CONTAINER_MTLS" \
    --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    "$IMAGE" \
    postgres \
    -c shared_preload_libraries=pg_redis_core \
    -c max_worker_processes=16 \
    -c pg_redis.bind_address=0.0.0.0 \
    -c pg_redis.port=0 \
    -c pg_redis.tls_port=6380 \
    -c pg_redis.tls_cert_file=/certs/server.crt \
    -c pg_redis.tls_key_file=/certs/server.key \
    -c pg_redis.tls_ca_cert_file=/certs/ca.crt \
    -c pg_redis.tls_auth_clients=yes \
    -c pg_redis.database=postgres \
    -c "pg_redis.requirepass=${REDIS_PASS}" >/dev/null

ready=0
for _ in $(seq 1 40); do
    if docker exec "$CONTAINER_MTLS" sh -c "pg_isready -U postgres -d postgres >/dev/null 2>&1 && redis-cli --tls --cacert /certs/ca.crt --cert /certs/client.crt --key /certs/client.key -h localhost -p 6380 -a '${REDIS_PASS}' --no-auth-warning PING 2>/dev/null | grep -q PONG"; then
        ready=1
        break
    fi
    sleep 1
done
if [ "$ready" -ne 1 ]; then
    echo "ERROR: TLS-only mTLS container failed to start. Logs:"
    docker logs "$CONTAINER_MTLS"
    exit 1
fi
echo "  [PASS] redis-cli --tls (mTLS with client cert) PING -> PONG succeeded!"

docker run --rm --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e "TLS_TEST_MODE=mtls" \
    -e "REDIS_HOST=${CONTAINER_MTLS}" \
    -e "REDIS_PLAIN_PORT=6379" \
    -e "REDIS_TLS_PORT=6380" \
    -e "REDIS_PASS=${REDIS_PASS}" \
    -e "CERT_DIR=/certs" \
    pg2redis-test-pubsub:local \
    node test_tls.mjs

echo "==> Testing Mode 3: SIGHUP live TLS certificate hot-reload..."
SERIAL_BEFORE="$(docker run --rm --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e "TLS_TEST_MODE=serial" \
    -e "REDIS_HOST=${CONTAINER_MTLS}" \
    -e "REDIS_TLS_PORT=6380" \
    -e "CERT_DIR=/certs" \
    pg2redis-test-pubsub:local \
    node test_tls.mjs | awk -F= '/^SERVER_CERT_SERIAL=/{print $2}')"

cp "$CERT_DIR/server_v2.crt" "$CERT_DIR/server.crt"
cp "$CERT_DIR/server_v2.key" "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt" "$CERT_DIR/server.key"

docker exec "$CONTAINER_MTLS" psql -U postgres -d postgres -c "SELECT pg_reload_conf();" >/dev/null
sleep 1

SERIAL_AFTER="$(docker run --rm --network "$NET_NAME" \
    -v "${CERT_DIR}:/certs:ro" \
    -e "TLS_TEST_MODE=serial" \
    -e "REDIS_HOST=${CONTAINER_MTLS}" \
    -e "REDIS_TLS_PORT=6380" \
    -e "CERT_DIR=/certs" \
    pg2redis-test-pubsub:local \
    node test_tls.mjs | awk -F= '/^SERVER_CERT_SERIAL=/{print $2}')"

echo "  Serial before SIGHUP: ${SERIAL_BEFORE}"
echo "  Serial after  SIGHUP: ${SERIAL_AFTER}"
if [ -z "$SERIAL_BEFORE" ] || [ -z "$SERIAL_AFTER" ] || [ "$SERIAL_BEFORE" = "$SERIAL_AFTER" ]; then
    echo "ERROR: Expected server certificate serial to change after SIGHUP reload!"
    docker logs "$CONTAINER_MTLS" | tail -n 40
    exit 1
fi
echo "  [PASS] SIGHUP live TLS certificate hot-reload verified!"
echo "ALL v0.5.0 TLS & mTLS TESTS PASSED!"
