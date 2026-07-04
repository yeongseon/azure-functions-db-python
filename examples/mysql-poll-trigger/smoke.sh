#!/usr/bin/env bash
#
# End-to-end smoke test for the MySQL polling-trigger example.
#
# Brings up MySQL + Azurite via docker compose, applies the schema,
# inserts and updates a row, and verifies that the package's polling
# logic produces the expected projection. Does NOT run `func start`
# (that requires the Azure Functions Core Tools and a long-lived
# process); instead it drives a single poll tick from Python so the
# script stays self-contained and CI-friendly.
#
# Requires Docker Compose v2 (`docker compose ...`, not `docker-compose`)
# because we rely on `--format json` and `up --wait`.
#
# Exits non-zero on any failure with a message identifying the step.
# Always tears down docker resources and the temporary venv on exit,
# even if a step fails.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly EXAMPLE_DIR
readonly MYSQL_URL="mysql+pymysql://app:app@127.0.0.1:3306/app"
readonly AZURITE_CONN_STR="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;QueueEndpoint=http://localhost:10001/devstoreaccount1;TableEndpoint=http://localhost:10002/devstoreaccount1;"
readonly CONTAINER_NAME="db-state"

log() { printf '\n[smoke] %s\n' "$*"; }
fail() { printf '\n[smoke][FAIL] %s\n' "$*" >&2; exit 1; }

cleanup() {
    local rc=$?
    log "cleanup (exit=${rc})"
    (cd "${EXAMPLE_DIR}" && docker compose down -v >/dev/null 2>&1) || true
    rm -rf "${EXAMPLE_DIR}/.smoke-venv"
    exit "${rc}"
}
trap cleanup EXIT

cd "${EXAMPLE_DIR}"

command -v docker >/dev/null || fail "docker is required"
command -v mysql >/dev/null || fail "mysql client is required (apt-get install mysql-client)"
command -v python3 >/dev/null || fail "python3 is required"
docker compose version >/dev/null 2>&1 \
    || fail "docker compose v2 is required (this script uses 'up --wait' and '--format json')"

log "1/7 docker compose up (waiting for healthchecks — MySQL first-boot is slow, ~20 s)"
docker compose up -d --wait || fail "docker compose up --wait failed"

log "2/7 verify MySQL and Azurite are healthy"
for service in mysql azurite; do
    docker compose ps --format json "${service}" 2>/dev/null \
        | grep -q '"Health":"healthy"' \
        || fail "${service} did not become healthy"
done

log "3/7 apply schema"
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app \
    < schema.sql >/dev/null || fail "schema.sql failed"

log "4/7 install Python dependencies"
python3 -m venv .smoke-venv
# shellcheck disable=SC1091
source .smoke-venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet \
    "azure-functions-db[mysql] @ file://${EXAMPLE_DIR}/../.." \
    azure-storage-blob \
    "pymysql>=1.1" \
    || fail "pip install failed"

log "5/7 seed orders rows"
# MySQL's TRUNCATE accepts only one table per statement (unlike PG's
# `TRUNCATE a, b`), so we issue them separately. TRUNCATE also resets
# AUTO_INCREMENT to 1 automatically — no RESTART IDENTITY clause needed.
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app <<'SQL' >/dev/null
TRUNCATE TABLE orders;
TRUNCATE TABLE processed_orders;
INSERT INTO orders (customer_name, amount, status)
VALUES ('Alice', 99.99, 'pending'),
       ('Bob',   49.50, 'pending');
UPDATE orders SET status = 'shipped', amount = 109.99
 WHERE customer_name = 'Alice';
SQL

log "6/7 drive one poll tick from Python and assert projection"
MYSQL_URL="${MYSQL_URL}" \
AZURITE_CONN_STR="${AZURITE_CONN_STR}" \
CONTAINER_NAME="${CONTAINER_NAME}" \
python3 - <<'PY'
import os
import sys
from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import ContainerClient
from sqlalchemy import create_engine, text

from azure_functions_db import (
    BlobCheckpointStore,
    PollTrigger,
    RowChange,
    SqlAlchemySource,
)

mysql_url = os.environ["MYSQL_URL"]
azurite = os.environ["AZURITE_CONN_STR"]
container_name = os.environ["CONTAINER_NAME"]

container = ContainerClient.from_connection_string(
    conn_str=azurite, container_name=container_name
)
try:
    container.create_container()
except ResourceExistsError:
    pass

# `schema=` intentionally omitted — MySQL uses the database name in the
# URL as the effective schema.
source = SqlAlchemySource(
    url=mysql_url,
    table="orders",
    cursor_column="updated_at",
    pk_columns=["id"],
)
checkpoint_store = BlobCheckpointStore(
    container_client=container,
    source_fingerprint=source.source_descriptor.fingerprint,
)

dest_engine = create_engine(mysql_url)


def handle(events: list[RowChange]) -> None:
    if not events:
        return
    # MySQL DATETIME is TZ-naive; write naive UTC to processed_at so it
    # matches source_cursor semantics (also naive UTC from the server).
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        {
            "order_id": e.pk["id"],
            # event.cursor is (cursor_column, *pk_columns) — keep the timestamp.
            "source_cursor": e.cursor[0],
            "customer_name": (e.after or {}).get("customer_name"),
            "amount": (e.after or {}).get("amount"),
            "status": (e.after or {}).get("status"),
            "processed_at": processed_at,
        }
        for e in events
        if e.after is not None
    ]
    with dest_engine.begin() as conn:
        for row in rows:
            # `INSERT IGNORE` is MySQL's equivalent of PG's
            # `ON CONFLICT DO NOTHING`: on a PRIMARY KEY / UNIQUE
            # conflict the row is silently skipped instead of raising.
            conn.execute(
                text(
                    "INSERT IGNORE INTO processed_orders "
                    "(order_id, source_cursor, customer_name, amount, status, processed_at) "
                    "VALUES (:order_id, :source_cursor, :customer_name, :amount, :status, "
                    ":processed_at)"
                ),
                row,
            )


trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=checkpoint_store,
)
events_processed = trigger.run(timer=None, handler=handle)
print(f"[smoke] poll tick processed {events_processed} event(s)")

with dest_engine.begin() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM processed_orders")).scalar_one()

if count < 2:
    print(
        f"[smoke][FAIL] expected at least 2 processed_orders rows, got {count}",
        file=sys.stderr,
    )
    sys.exit(1)

blobs = list(container.list_blobs())
if not blobs:
    print("[smoke][FAIL] no checkpoint blob present in Azurite", file=sys.stderr)
    sys.exit(1)

print(f"[smoke] processed_orders rows: {count}")
print(f"[smoke] checkpoint blobs: {[b.name for b in blobs]}")
PY

log "7/7 assertions passed (cleanup runs via EXIT trap)"
log "OK — smoke test passed"
