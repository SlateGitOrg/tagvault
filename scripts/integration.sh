#!/usr/bin/env bash
# Optional integration test against a real MQTT broker (Eclipse Mosquitto in Docker).
# Skips cleanly (exit 0) when Docker is not available. Not part of `make check`.
set -euo pipefail

if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
  echo "integration: Docker not available — skipping"
  exit 0
fi

IMAGE="eclipse-mosquitto:2.0.20"
NAME="p50-21-tagvault-mosquitto"
PORT="${TAGVAULT_IT_PORT:-18830}"
TV="${TAGVAULT_BIN:-target/release/tagvault}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/tagvault-it.XXXXXX")"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run -d --rm --name "$NAME" -p "127.0.0.1:${PORT}:1883" "$IMAGE" mosquitto -c /mosquitto-no-auth.conf >/dev/null
for _ in $(seq 1 50); do
  (exec 3<>"/dev/tcp/127.0.0.1/${PORT}") 2>/dev/null && break
  sleep 0.2
done

"$TV" mqtt --broker "127.0.0.1:${PORT}" --data "$WORK/archive" --spool "$WORK/spool" --group Plant1 \
  --comp-dev 0.1 --max-messages 400 &
HOST_PID=$!
sleep 1
"$TV" edge-sim --broker "127.0.0.1:${PORT}" --group Plant1 --nodes 2 --seconds 120 --speed 40
wait "$HOST_PID"

TAGS=$("$TV" tags --data "$WORK/archive" | tail -n +2 | wc -l | tr -d ' ')
echo "integration: archive holds ${TAGS} tags"
if [ "$TAGS" -lt 14 ]; then
  echo "integration: expected 14 tags" >&2
  exit 1
fi
"$TV" verify --data "$WORK/archive"
echo "integration: OK"
