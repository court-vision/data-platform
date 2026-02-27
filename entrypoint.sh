#!/bin/bash
set -e

# Private server: full app on Railway's internal IPv6 network only.
# Cron-runner and backend reach this via *.railway.internal hostnames.
PRIVATE_PORT="${PRIVATE_PORT:-8001}"

# Public server: dashboard only on all IPv4 interfaces.
# Railway routes the public domain (data.courtvision.dev) to this port.
PUBLIC_PORT="${PORT:-8080}"

cleanup() {
    kill "$PRIVATE_PID" "$PUBLIC_PID" 2>/dev/null || true
    wait
}
trap cleanup EXIT SIGTERM SIGINT

echo "Starting private server on ::${PRIVATE_PORT}"
uvicorn main:app --host :: --port "$PRIVATE_PORT" &
PRIVATE_PID=$!

echo "Starting public server on 0.0.0.0:${PUBLIC_PORT}"
uvicorn main_public:app --host 0.0.0.0 --port "$PUBLIC_PORT" &
PUBLIC_PID=$!

# Block until either process exits, then cleanup kills the other
wait "$PRIVATE_PID" "$PUBLIC_PID"
