"""Offline demo: checkpoint progress and replay of a failed batch (SQLite, no Azure).

Run it from the repository root after ``pip install -e .``::

    python examples/poller_checkpoint_replay.py

What it shows, with a temporary SQLite database and a temporary JSON checkpoint file:

1. A tick whose handler succeeds commits the checkpoint (cursor + primary key).
2. A tick whose handler fails *after* handling the first event of the batch does not
   advance the checkpoint (``HandlerError``).
3. After a simulated restart (new objects, same checkpoint file) the same events are
   delivered again with the same ``event_id`` values. The event handled before the
   failure is therefore seen twice: delivery is at-least-once, not exactly-once, so a
   handler must be idempotent (here: it de-duplicates on ``event_id``).
4. A checkpoint commit that fails after the handler succeeded (``CommitError``) replays
   the batch the same way.

``LocalFileStateStore`` is a demo only. It has no real lease: it must not be used for
concurrent pollers. Use ``BlobCheckpointStore`` in a deployed app.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
import tempfile

from sqlalchemy import create_engine, text

from azure_functions_db import (
    CommitError,
    HandlerError,
    PollTrigger,
    RowChange,
    SqlAlchemySource,
)

POLLER_NAME = "orders-replay-demo"


class LocalFileStateStore:
    """JSON-file checkpoint store with a no-op lease (demo only, single process)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self.fail_next_commit = False

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        return "demo-lease"

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        return None

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        return None

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        if not self._path.exists():
            return {}
        loaded: dict[str, object] = json.loads(self._path.read_text())
        return loaded

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        if self.fail_next_commit:
            self.fail_next_commit = False
            raise OSError("simulated checkpoint write failure")
        self._path.write_text(json.dumps(checkpoint, default=list))


def seed(url: str) -> None:
    """Create an indexed ``orders`` table with four synthetic rows (ids 1-4, versions 10-40)."""
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER)"))
            conn.execute(text("CREATE INDEX idx_orders_version ON orders (version)"))
        insert_orders(url, range(1, 5))
    finally:
        engine.dispose()


def insert_orders(url: str, ids: range) -> None:
    """Insert synthetic rows; ``version`` grows with ``id`` so the cursor only moves forward."""
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            for row_id in ids:
                conn.execute(
                    text("INSERT INTO orders VALUES (:i, :v)"), {"i": row_id, "v": row_id * 10}
                )
    finally:
        engine.dispose()


def make_trigger(url: str, store: LocalFileStateStore) -> PollTrigger:
    """Build a trigger as a freshly started worker would (batches of two rows)."""
    source = SqlAlchemySource(url=url, table="orders", cursor_column="version", pk_columns=["id"])
    return PollTrigger(name=POLLER_NAME, source=source, checkpoint_store=store, batch_size=2)


class Sink:
    """Idempotent downstream: counts every delivery, applies each ``event_id`` once."""

    def __init__(self) -> None:
        self.deliveries: list[int] = []
        self.applied: dict[str, int] = {}
        self.fail_after_first = False

    def __call__(self, events: list[RowChange]) -> None:
        for index, event in enumerate(events):
            order_id = event.pk["id"]
            if not isinstance(order_id, int):
                raise TypeError("demo rows use integer ids")
            self.deliveries.append(order_id)
            self.applied.setdefault(event.event_id, order_id)
            if self.fail_after_first and index == 0:
                self.fail_after_first = False
                raise RuntimeError("simulated downstream outage after the first event")


def run_demo(directory: Path) -> dict[str, object]:
    """Run the replay scenario and return what happened (also asserted by the tests)."""
    url = f"sqlite:///{directory / 'orders.db'}"
    seed(url)
    store = LocalFileStateStore(directory / "checkpoint.json")
    sink = Sink()
    result: dict[str, object] = {}

    # 1. Success: rows 1 and 2 are delivered and the checkpoint is committed.
    first = make_trigger(url, store)
    result["tick1_count"] = first.run(timer=None, handler=sink)
    result["checkpoint_after_success"] = store.load_checkpoint(POLLER_NAME)["cursor"]

    # 2. Failure after the first event of the next batch: checkpoint must not move.
    sink.fail_after_first = True
    try:
        first.run(timer=None, handler=sink)
    except HandlerError:
        result["tick2_error"] = "HandlerError"
    result["checkpoint_after_failure"] = store.load_checkpoint(POLLER_NAME)["cursor"]
    result["deliveries_before_restart"] = list(sink.deliveries)

    # 3. Simulated restart: new trigger/source objects, same checkpoint file.
    restarted = make_trigger(url, store)
    result["tick3_count"] = restarted.run(timer=None, handler=sink)
    result["checkpoint_after_replay"] = store.load_checkpoint(POLLER_NAME)["cursor"]
    result["deliveries"] = list(sink.deliveries)
    result["applied_once"] = sorted(sink.applied.values())

    # 4. Two new rows arrive. The handler succeeds but the checkpoint commit fails,
    #    so that batch is replayed too.
    insert_orders(url, range(5, 7))
    store.fail_next_commit = True
    try:
        restarted.run(timer=None, handler=sink)
    except CommitError:
        result["tick4_error"] = "CommitError"
    result["checkpoint_after_commit_failure"] = store.load_checkpoint(POLLER_NAME)["cursor"]
    result["tick5_count"] = restarted.run(timer=None, handler=sink)
    result["final_deliveries"] = list(sink.deliveries)
    result["final_applied_once"] = sorted(sink.applied.values())
    return result


def main() -> None:
    logging.getLogger("azure_functions_db").setLevel(logging.CRITICAL)  # keep demo output short
    with tempfile.TemporaryDirectory() as directory:
        for key, value in run_demo(Path(directory)).items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
