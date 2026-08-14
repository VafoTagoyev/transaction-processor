#!/usr/bin/env bash
# Starts the full three-instance topology and follows progress until the backlog is drained.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> Building and starting postgres, valkey, processor-1..3"
docker compose up -d --build

echo ">> Waiting for all three processors to report healthy"
for port in 8081 8082 8083; do
  until curl -fs "http://localhost:${port}/actuator/health" | grep -q '"status":"UP"'; do
    printf '.'
    sleep 2
  done
  echo " processor on :${port} is UP"
done

echo
echo ">> Live progress (Ctrl-C to stop watching; the processors keep running)"
exec ./scripts/watch.sh
