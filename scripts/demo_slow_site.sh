#!/usr/bin/env bash
# Slow-site demo: site B answers every compute call with a 5s delay.
# Shows that status queries and cancellation stay responsive, that the round
# stops entirely, and that both sites release their snapshots afterwards.
set -euo pipefail
COORD="${COORD:-http://localhost:8000}"
A="${SITE_A:-http://localhost:8001}"
B="${SITE_B:-http://localhost:8002}"

ms() { python3 -c 'import time; print(int(time.time()*1000))'; }

echo "== start a round (site B is slow: 5s per call) =="
RID=$(curl -s -X POST "$COORD/rounds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["round_id"])')
echo "round_id=$RID"
sleep 1

echo "== status queries stay fast while B is still computing =="
for i in 1 2 3; do
  t0=$(ms)
  ST=$(curl -s "$COORD/rounds/$RID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  t1=$(ms)
  echo "  poll $i: status=$ST  ($((t1-t0))ms)"
  sleep 1
done

echo "== cancel returns immediately =="
t0=$(ms)
curl -s -X POST "$COORD/rounds/$RID/cancel" | python3 -m json.tool
t1=$(ms)
echo "  cancel latency: $((t1-t0))ms"

echo "== wait for terminal state =="
while true; do
  ST=$(curl -s "$COORD/rounds/$RID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  status: $ST"
  case "$ST" in succeeded|failed|cancelled) break;; esac
  sleep 1
done

echo "== both sites released their snapshots =="
echo "site A: $(curl -s "$A/snapshots")"
echo "site B: $(curl -s "$B/snapshots")"

echo "== late snapshot request for the finished round is rejected =="
curl -s -o /dev/null -w "site A POST /snapshots round=$RID -> HTTP %{http_code}\n" \
  -X POST "$A/snapshots" -H 'Content-Type: application/json' -d "{\"round_id\": \"$RID\"}"
