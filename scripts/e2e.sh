#!/usr/bin/env bash
# End-to-end: stack up -> tables -> produce -> stream -> gold -> DQ gate -> verify in Trino -> replay -> verify again.
# Needs Docker, Java 17 and a Python env with requirements.txt installed.
set -euo pipefail
cd "$(dirname "$0")/.."
EVENTS="${EVENTS:-20000}"
export PYTHONPATH="$PWD" CHECKPOINT_DIR="${CHECKPOINT_DIR:-$PWD/.checkpoints}"
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password AWS_REGION=us-east-1

step() { echo; echo "=== $* ($(date -u +%H:%M:%S))"; }

step "start stack"
docker compose up -d --wait kafka s3 rest trino
docker compose ps

step "create tables"
python -m jobs.init_tables

step "produce $EVENTS events"
python producer/telemetry_producer.py --events "$EVENTS" --devices 200 --expect expectations.json

step "stream Kafka -> bronze / silver / quarantine (availableNow)"
time python -m jobs.stream_ingest --once

step "build gold hourly aggregates"
python -m jobs.build_gold --hours 48

step "data-quality gate"
python -m jobs.dq_checks --max-reject-rate 0.10 --report dq_report.json

step "verify against producer expectations (Trino)"
python scripts/verify_e2e.py --expect expectations.json --label "first run" --out e2e_first.json

step "replay: delete the streaming checkpoint and re-read the whole topic"
rm -rf "$CHECKPOINT_DIR/stream_ingest"
python -m jobs.stream_ingest --once
python -m jobs.build_gold --hours 48
python scripts/verify_e2e.py --expect expectations.json --label "after full replay" --out e2e_replay.json

step "table maintenance (compaction, snapshot expiry)"
python -m jobs.maintenance --retain-days 0

step "verify again after maintenance"
python scripts/verify_e2e.py --expect expectations.json --label "after maintenance" --out e2e_maintenance.json

echo; echo "E2E PASSED"
