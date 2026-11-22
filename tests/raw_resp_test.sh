#!/usr/bin/env bash
set -euo pipefail

HOST="${REDIS_HOST:-127.0.0.1}"
PORT="${REDIS_PORT:-6379}"

echo "==> [1/4] Raw TCP RESP2 PING test (RFC §7.2: *1\\r\\n\$4\\r\\nPING\\r\\n -> +PONG\\r\\n)"
RESP=$(printf '*1\r\n$4\r\nPING\r\n' | nc -w 2 "$HOST" "$PORT" | tr -d '\r')
if [ "$RESP" != "+PONG" ]; then
    echo "FAIL: expected '+PONG', got '$RESP'"
    exit 1
fi
echo "    OK: got +PONG\\r\\n"

echo "==> [2/4] Raw TCP inline PING test"
RESP_INLINE=$(printf 'PING\r\n' | nc -w 2 "$HOST" "$PORT" | tr -d '\r')
if [ "$RESP_INLINE" != "+PONG" ]; then
    echo "FAIL: expected '+PONG' for inline PING, got '$RESP_INLINE'"
    exit 1
fi
echo "    OK: got +PONG\\r\\n"

echo "==> [3/4] Raw TCP pipelined burst (SET + GET + INCR + DEL in one write)"
PIPE_OUT=$(printf '*3\r\n$3\r\nSET\r\n$8\r\nraw_pipe\r\n$2\r\n10\r\n*2\r\n$3\r\nGET\r\n$8\r\nraw_pipe\r\n*2\r\n$4\r\nINCR\r\n$8\r\nraw_pipe\r\n*2\r\n$3\r\nDEL\r\n$8\r\nraw_pipe\r\n' \
    | nc -w 2 "$HOST" "$PORT" | tr -d '\r')
EXPECTED_PIPE=$(printf '+OK\n$2\n10\n:11\n:1')
if [ "$PIPE_OUT" != "$EXPECTED_PIPE" ]; then
    echo "FAIL: pipeline mismatch!"
    echo "Got:"
    echo "$PIPE_OUT"
    echo "Expected:"
    echo "$EXPECTED_PIPE"
    exit 1
fi
echo "    OK: pipelined batch returned exact RESP2 responses"

echo "==> [4/4] Raw RESP3 HELLO 3 negotiation"
HELLO_OUT=$(printf '*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n' | nc -w 2 "$HOST" "$PORT" | head -n 1 | tr -d '\r')
if [ "$HELLO_OUT" != "%7" ]; then
    echo "FAIL: expected RESP3 map header '%7', got '$HELLO_OUT'"
    exit 1
fi
echo "    OK: HELLO 3 returned RESP3 map (%7)"

echo "PASS: All raw TCP RESP protocol tests succeeded!"
