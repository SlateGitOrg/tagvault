#!/usr/bin/env bash
# Offline end-to-end demo: simulated plant → Sparkplug host → exception → store-and-forward
# (with a link outage) → SDT archive → queries → HTTP API → torn-write recovery.
set -euo pipefail
cd "$(dirname "$0")/.."

TV="${TAGVAULT_BIN:-target/release/tagvault}"
OUT="out/demo"
rm -rf -- "$OUT"
mkdir -p "$OUT"
TAG_LEVEL="Plant1/Line1/Tank01/Level"
TAG_TEMP="Plant1/Line1/Reactor01/Temperature"
TAG_PUMP="Plant1/Line1/Tank01/PumpRunning"

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1. Simulate 2 h of a 2-line plant (Sparkplug B edge nodes, lossy broker link, one node restart)"
"$TV" simulate --out "$OUT/plant.tvcap" --hours 2 --seed 7

step "2. Ingest: host → exception filter → spool → archive, with the archive link down 00:40–00:55"
"$TV" ingest --capture "$OUT/plant.tvcap" --data "$OUT/archive" --spool "$OUT/spool" \
  --policy fixtures/policy.json --outage 40m:15m --events

step "3. Tags"
"$TV" tags --data "$OUT/archive"

step "4. 15-minute time-weighted average / min / max of $TAG_LEVEL"
"$TV" query agg --data "$OUT/archive" --tag "$TAG_LEVEL" \
  --start 2026-01-05T00:00:00Z --end 2026-01-05T02:00:00Z --interval 15m

step "5. Interpolated $TAG_TEMP across the Line1 restart at 01:00 (stale while offline)"
"$TV" query interp --data "$OUT/archive" --tag "$TAG_TEMP" \
  --start 2026-01-05T00:58:00Z --end 2026-01-05T01:04:00Z --step 1m

step "6. Raw values of the discrete tag $TAG_PUMP (first 6)"
"$TV" query raw --data "$OUT/archive" --tag "$TAG_PUMP" \
  --start 2026-01-05T00:00:00Z --end 2026-01-05T02:00:00Z --limit 6

step "7. HTTP API: GET /api/v1/aggregate"
"$TV" serve --data "$OUT/archive" --listen 127.0.0.1:0 --addr-file "$OUT/addr" --threads 2 >/dev/null &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
for _ in $(seq 1 100); do [ -s "$OUT/addr" ] && break; sleep 0.05; done
ADDR="$(cat "$OUT/addr")"
curl -s "http://$ADDR/api/v1/aggregate?tag=Plant1%2FLine2%2FReactor01%2FPressure&start=2026-01-05T00:00:00Z&end=2026-01-05T01:00:00Z&interval=30m"
echo
kill "$SERVER"; wait "$SERVER" 2>/dev/null || true
trap - EXIT

step "8. Crash simulation: append a torn record to the WAL, then reopen"
printf '\x40\x00\x00\x00\xde\xad\xbe' >> "$OUT/archive/wal.log"
"$TV" verify --data "$OUT/archive"

step "demo complete — artifacts in $OUT/"
