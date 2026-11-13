#!/usr/bin/env bash
#
# End-to-end demonstration of the fermentation lab archiver.
#
# Prerequisites: Docker + the Compose plugin, and a free TCP 1883/8000.
# The script is intentionally verbose: every step prints the HTTP/curl command
# it runs so it can also be used as a copy-paste cookbook.
#
# Usage:  ./scripts/demo.sh
#
set -euo pipefail

COMPOSE="docker compose"
BASE="http://localhost:8000"
INSTRUMENT="FERM-01"
BATCH="BATCH-2026-011"

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run() { printf '\033[0;90m$ %s\033[0m\n' "$*"; "$@"; }

say "0/12 build and start the stack (persistent broker + API)"
$COMPOSE up -d --build broker api
$COMPOSE ps

say "1/12 wait for the API health endpoint (FastAPI /docs is on :8000/docs)"
for i in $(seq 1 30); do
  if curl -fsS "$BASE/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
run curl -sS "$BASE/health" | python3 -m json.tool

say "2/12 publish the guided instrument scenario (fresh, duplicate, conflict, burst, malformed)"
$COMPOSE run --rm publisher python publisher.py --guided

say "3/12 query stored samples for the instrument + batch"
run curl -sS "$BASE/samples?instrument_id=$INSTRUMENT&batch_no=$BATCH&limit=50" \
  | python3 -m json.tool

say "4/12 query retained conflicts (original samples are never overwritten)"
run curl -sS "$BASE/conflicts?instrument_id=$INSTRUMENT&batch_no=$BATCH" \
  | python3 -m json.tool

say "5/12 show processing counters (stored / duplicates / conflicts / ...)"
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

say "6/12 SLOW-WRITE demo: pause the writer BEFORE the DB write"
echo "The MQTT heartbeat and all GET endpoints stay alive; messages queue up."
run curl -sS -X POST "$BASE/demo/pause" -H 'Content-Type: application/json' \
  -d '{"seconds": 8}' | python3 -m json.tool
$COMPOSE run --rm publisher python publisher.py --seq 100 --temp 31.0 --ph 6.7
echo "-- a query still answers instantly while the writer is paused --"
run curl -sS "$BASE/health" | python3 -m json.tool
echo "-- waiting out the pause, then confirming the queued sample landed --"
sleep 9
run curl -sS "$BASE/samples?instrument_id=$INSTRUMENT&batch_no=$BATCH&limit=1&offset=0" \
  | python3 -m json.tool

say "7/12 POST-COMMIT/PRE-ACK window demo (the crash-redelivery guarantee)"
run curl -sS -X POST "$BASE/demo/pause-post-commit" \
  -H 'Content-Type: application/json' -d '{"seconds": 5}' | python3 -m json.tool
$COMPOSE run --rm publisher python publisher.py --seq 200 --temp 32.0 --ph 6.6
sleep 6
echo "-- seq 200 was committed before the PUBACK; a replay cannot duplicate it --"
$COMPOSE run --rm publisher python publisher.py --seq 200 --temp 32.0 --ph 6.6
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

say "8/12 DISCONNECT demo: force a TCP reconnect, session stays persistent"
run curl -sS -X POST "$BASE/demo/reconnect" | python3 -m json.tool
sleep 3
run curl -sS "$BASE/health" | python3 -m json.tool
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

say "9/12 PACKAGE: create a background ZIP for the instrument + batch"
echo "Both CSVs come from one read-only snapshot taken before reading starts."
JOB=$(run curl -sS -X POST "$BASE/packages" -H 'Content-Type: application/json' \
  -d "{\"instrument_id\": \"$INSTRUMENT\", \"batch_no\": \"$BATCH\"}")
echo "$JOB" | python3 -m json.tool
JOB_ID=$(echo "$JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["job_id"])')
for i in $(seq 1 30); do
  STATUS=$(curl -sS "$BASE/packages/$JOB_ID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  echo "  status: $STATUS"
  if [ "$STATUS" = "completed" ] || [ "$STATUS" = "failed" ] || [ "$STATUS" = "cancelled" ]; then break; fi
  sleep 1
done
run curl -sS "$BASE/packages/$JOB_ID" | python3 -m json.tool
echo "-- only one packaging job at a time: a concurrent request gets 409 --"
run curl -sS -o /dev/null -w "%{http_code}\n" -X POST "$BASE/packages" \
  -H 'Content-Type: application/json' \
  -d "{\"instrument_id\": \"$INSTRUMENT\", \"batch_no\": \"$BATCH\"}" || true

say "10/12 DOWNLOAD: fetch the completed ZIP and verify its contents"
run curl -sS -D - -o /tmp/delivery-package.zip "$BASE/packages/$JOB_ID/download" \
  | grep -iE 'HTTP/|content-type|x-job-id|x-sha256|content-disposition'
run python3 - "$JOB_ID" <<'PY'
import json, sys, zipfile
job_id = sys.argv[1]
with zipfile.ZipFile("/tmp/delivery-package.zip") as zf:
    assert zf.testzip() is None
    print("archive entries:", zf.namelist())
    print("samples.csv head:")
    print("\n".join(zf.read("samples.csv").decode().splitlines()[:4]))
    print("conflicts.csv:")
    print(zf.read("conflicts.csv").decode())
    print("manifest.json:")
    print(json.dumps(json.loads(zf.read("manifest.json")), indent=2))
PY

say "11/12 CANCEL WHILE READING: pause every DB batch, start a job, cancel mid-read"
run curl -sS -X POST "$BASE/demo/package-pause" -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 2.0, "pre_publish_seconds": 0.0}' | python3 -m json.tool
SLOW_JOB=$(curl -sS -X POST "$BASE/packages" -H 'Content-Type: application/json' \
  -d "{\"instrument_id\": \"$INSTRUMENT\", \"batch_no\": \"$BATCH\"}")
SLOW_ID=$(echo "$SLOW_JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["job_id"])')
sleep 1
echo "-- status while the worker is still reading --"
run curl -sS "$BASE/packages/$SLOW_ID" | python3 -m json.tool
echo "-- cancel blocks until IO stops, files close and staging is cleaned --"
run curl -sS -X POST "$BASE/packages/$SLOW_ID/cancel" | python3 -m json.tool
echo "-- a cancelled job offers no download --"
run curl -sS -o /dev/null -w "%{http_code}\n" "$BASE/packages/$SLOW_ID/download"

say "12/12 CANCEL IN THE WRITTEN-AWAITING-PUBLICATION WINDOW"
run curl -sS -X POST "$BASE/demo/package-pause" -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 0.0, "pre_publish_seconds": 10.0}' | python3 -m json.tool
HOLD_JOB=$(curl -sS -X POST "$BASE/packages" -H 'Content-Type: application/json' \
  -d "{\"instrument_id\": \"$INSTRUMENT\", \"batch_no\": \"$BATCH\"}")
HOLD_ID=$(echo "$HOLD_JOB" | python3 -c 'import sys,json; print(json.load(sys.stdin)["job_id"])')
for i in $(seq 1 20); do
  STATUS=$(curl -sS "$BASE/packages/$HOLD_ID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  echo "  status: $STATUS (the verified ZIP exists on disk but is not published)"
  if [ "$STATUS" = "verifying" ] || [ "$STATUS" = "publishing" ]; then break; fi
  sleep 1
done
echo "-- download is still 404 while publication is held --"
run curl -sS -o /dev/null -w "%{http_code}\n" "$BASE/packages/$HOLD_ID/download"
echo "-- cancel now suppresses the just-written ZIP --"
run curl -sS -X POST "$BASE/packages/$HOLD_ID/cancel" | python3 -m json.tool
run curl -sS -o /dev/null -w "%{http_code}\n" "$BASE/packages/$HOLD_ID/download"
echo "-- clear the demo pauses; the first package (job $JOB_ID) is still downloadable --"
run curl -sS -X POST "$BASE/demo/package-pause" -H 'Content-Type: application/json' \
  -d '{"read_batch_seconds": 0.0, "pre_publish_seconds": 0.0}' | python3 -m json.tool
run curl -sS "$BASE/packages" | python3 -m json.tool

cat <<TIP

Interactive docs:  open $BASE/docs   (Swagger UI, every endpoint callable)
OpenAPI schema:     $BASE/openapi.json
Raw SQLite (inside the api container's volume):
  $COMPOSE exec api python -c "import sqlite3,os; \\
     c=sqlite3.connect(os.environ['DB_PATH']); \\
     print(c.execute('select count(*) from samples').fetchone(), \\
           c.execute('select count(*) from conflicts').fetchone())"

Tear down (data is kept in named volumes):  $COMPOSE down
Full reset including data:                  $COMPOSE down -v
TIP
