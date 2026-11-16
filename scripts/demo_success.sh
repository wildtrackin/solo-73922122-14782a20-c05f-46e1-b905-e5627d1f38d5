#!/usr/bin/env bash
# Happy-path demo: run one federated KM round and print the combined curve.
set -euo pipefail
COORD="${COORD:-http://localhost:8000}"

echo "== start round =="
RID=$(curl -s -X POST "$COORD/rounds" | python3 -c 'import sys,json;print(json.load(sys.stdin)["round_id"])')
echo "round_id=$RID"

echo "== poll until terminal =="
while true; do
  ST=$(curl -s "$COORD/rounds/$RID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  status: $ST"
  case "$ST" in succeeded|failed|cancelled) break;; esac
  sleep 1
done

echo "== result =="
curl -s "$COORD/rounds/$RID" | python3 -c '
import sys, json
r = json.load(sys.stdin)
print("status:", r["status"])
res = r.get("result")
if not res:
    print("error:", r.get("error")); raise SystemExit(0)
print(f'{"t":>6} {"n_risk":>8} {"d":>4} {"survival":>10}')
for row in res["curve"]:
    print(f'{row["t"]:>6} {row["n_risk"]:>8} {row["d"]:>4} {row["survival"]:>10.4f}')
'

echo "== site snapshot slots (should be 0 active) =="
curl -s http://localhost:8001/snapshots | python3 -m json.tool
curl -s http://localhost:8002/snapshots | python3 -m json.tool
