#!/usr/bin/env bash
set -euo pipefail

DEMO_DIR="$(mktemp -d /tmp/quorum-kv.XXXXXX)"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM

CLUSTER='n1=http://127.0.0.1:9001,n2=http://127.0.0.1:9002,n3=http://127.0.0.1:9003'
for member in n1:9001 n2:9002 n3:9003; do
  id="${member%%:*}"
  port="${member##*:}"
  go run ./engineering-labs/quorum-kv/cmd/kv-node -id "$id" -listen ":$port" -cluster "$CLUSTER" -data "$DEMO_DIR/$id" \
    >"$DEMO_DIR/$id.log" 2>&1 &
  PIDS+=("$!")
done

sleep 2
for port in 9001 9002 9003; do
  if curl -fsS -X PUT "http://127.0.0.1:$port/v1/kv/demo" \
    -H 'content-type: application/json' -d '{"value":"quorum-committed","ttl_seconds":60}'; then
    curl -fsS "http://127.0.0.1:$port/v1/kv/demo"
    exit 0
  fi
done
echo "no leader accepted the demo write" >&2
exit 1
