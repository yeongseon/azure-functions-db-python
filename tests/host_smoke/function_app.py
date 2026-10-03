from __future__ import annotations

import json
from pathlib import Path
import sqlite3

import azure.functions as func

from azure_functions_db import DbBindings, RowChange, SqlAlchemySource


class SqliteStateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        del poller_name, ttl_seconds
        return "host-smoke-lease"

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        del poller_name, lease_id, ttl_seconds

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        del poller_name, lease_id

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        with sqlite3.connect(self._path) as connection:
            row = connection.execute(
                "SELECT checkpoint FROM trigger_state WHERE poller_name = ?",
                (poller_name,),
            ).fetchone()
        return {} if row is None else json.loads(row[0])

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        del lease_id
        with sqlite3.connect(self._path) as connection:
            connection.execute(
                "INSERT OR REPLACE INTO trigger_state (poller_name, checkpoint) VALUES (?, ?)",
                (poller_name, json.dumps(checkpoint)),
            )


app = func.FunctionApp()
db = DbBindings()
database_path = Path(__file__).with_name("host-smoke.db")

with sqlite3.connect(database_path) as connection:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, updated_at INTEGER NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS trigger_state "
        "(poller_name TEXT PRIMARY KEY, checkpoint TEXT NOT NULL)"
    )
    connection.execute("INSERT OR IGNORE INTO events (id, updated_at) VALUES (1, 1)")

source = SqlAlchemySource(
    url=f"sqlite:///{database_path}",
    table="events",
    cursor_column="updated_at",
    pk_columns=["id"],
)


@app.function_name(name="db_trigger_host_smoke")
@app.schedule(
    schedule="0 0 0 1 1 *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=False,
)
@db.trigger(arg_name="events", source=source, checkpoint_store=SqliteStateStore(database_path))
def db_trigger_host_smoke(timer: func.TimerRequest, events: list[RowChange]) -> None:
    del timer
    print(f"DB_TRIGGER_HOST_SMOKE_OK events={len(events)}", flush=True)
