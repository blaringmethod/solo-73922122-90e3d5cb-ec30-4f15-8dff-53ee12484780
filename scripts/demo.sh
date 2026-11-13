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

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
run() { printf '\033[0;90m$ %s\033[0m\n' "$*"; "$@"; }

say "0/8 build and start the stack (persistent broker + API)"
$COMPOSE up -d --build broker api
$COMPOSE ps

say "1/8 wait for the API health endpoint (FastAPI /docs is on :8000/docs)"
for i in $(seq 1 30); do
  if curl -fsS "$BASE/health" >/dev/null 2>&1; then break; fi
  sleep 1
done
run curl -sS "$BASE/health" | python3 -m json.tool

say "2/8 publish the guided instrument scenario (fresh, duplicate, conflict, burst, malformed)"
$COMPOSE run --rm publisher python publisher.py --guided

say "3/8 query stored samples for the instrument + batch"
run curl -sS "$BASE/samples?instrument_id=FERM-01&batch_no=BATCH-2026-011&limit=50" \
  | python3 -m json.tool

say "4/8 query retained conflicts (original samples are never overwritten)"
run curl -sS "$BASE/conflicts?instrument_id=FERM-01&batch_no=BATCH-2026-011" \
  | python3 -m json.tool

say "5/8 show processing counters (stored / duplicates / conflicts / ...)"
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

say "6/8 SLOW-WRITE demo: pause the writer BEFORE the DB write"
echo "The MQTT heartbeat and all GET endpoints stay alive; messages queue up."
run curl -sS -X POST "$BASE/demo/pause" -H 'Content-Type: application/json' \
  -d '{"seconds": 8}' | python3 -m json.tool
$COMPOSE run --rm publisher python publisher.py --seq 100 --temp 31.0 --ph 6.7
echo "-- a query still answers instantly while the writer is paused --"
run curl -sS "$BASE/health" | python3 -m json.tool
echo "-- waiting out the pause, then confirming the queued sample landed --"
sleep 9
run curl -sS "$BASE/samples?instrument_id=FERM-01&batch_no=BATCH-2026-011&limit=1&offset=0" \
  | python3 -m json.tool

say "7/8 POST-COMMIT/PRE-ACK window demo (the crash-redelivery guarantee)"
run curl -sS -X POST "$BASE/demo/pause-post-commit" \
  -H 'Content-Type: application/json' -d '{"seconds": 5}' | python3 -m json.tool
$COMPOSE run --rm publisher python publisher.py --seq 200 --temp 32.0 --ph 6.6
sleep 6
echo "-- seq 200 was committed before the PUBACK; a replay cannot duplicate it --"
$COMPOSE run --rm publisher python publisher.py --seq 200 --temp 32.0 --ph 6.6
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

say "8/8 DISCONNECT demo: force a TCP reconnect, session stays persistent"
run curl -sS -X POST "$BASE/demo/reconnect" | python3 -m json.tool
sleep 3
run curl -sS "$BASE/health" | python3 -m json.tool
run curl -sS "$BASE/demo/stats" | python3 -m json.tool

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
