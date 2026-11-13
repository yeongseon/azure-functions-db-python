"""Offline demo: poller metrics around a SQLite read and a deliberately failed query.

Run it from the repository root after ``pip install -e .``::

    python examples/observability_metrics.py

The supported metrics surface is the ``MetricsCollector`` protocol that ``PollTrigger``
accepts (``metrics=``). ``DbReader`` and ``DbWriter`` emit no metrics. The demo uses
synthetic rows in a temporary SQLite file:

* a tick that reads two rows and succeeds emits ``azfdb_batches_total{result=success}``,
  ``azfdb_events_total`` and the duration/size histograms;
* a tick whose query fails (the table does not exist) emits ``azfdb_failures_total`` with an
  ``error_type`` label and ``azfdb_batches_total{result=failure}``.

Labels are limited to ``poller_name``, ``source``, ``result`` and ``error_type``. Query text,
connection credentials and record values are never labels; the demo checks that. Duration
values vary from run to run, so only their names and labels are asserted, never thresholds.
"""

from __future__ import annotations

from collections.abc import Mapping
import contextlib
import logging
from pathlib import Path
import tempfile

from sqlalchemy import create_engine, text

from azure_functions_db import FetchError, PollTrigger, SqlAlchemySource

ALLOWED_LABELS = frozenset({"poller_name", "source", "result", "error_type"})
CANARY_ROW_VALUE = "SYNTHETIC_ROW_VALUE"
FAILING_QUERY = "SELECT id, version FROM table_that_does_not_exist WHERE version > :cursor"

Sample = tuple[str, str, float, dict[str, str]]


class RecordingCollector:
    """In-memory ``MetricsCollector``: keeps (kind, name, value, labels) tuples."""

    def __init__(self) -> None:
        self.samples: list[Sample] = []

    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        self.samples.append(("counter", name, value, dict(labels or {})))

    def observe(self, name: str, value: float, *, labels: Mapping[str, str] | None = None) -> None:
        self.samples.append(("histogram", name, value, dict(labels or {})))

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        self.samples.append(("gauge", name, value, dict(labels or {})))

    def counter(self, name: str) -> list[tuple[float, dict[str, str]]]:
        return [
            (v, labels) for kind, n, v, labels in self.samples if kind == "counter" and n == name
        ]

    def names(self) -> set[str]:
        return {name for _, name, _, _ in self.samples}


class MemoryStore:
    """Minimal checkpoint store (single process, demo only)."""

    def __init__(self) -> None:
        self._checkpoint: dict[str, object] = {}

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        return "demo-lease"

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        return None

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        return None

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        return dict(self._checkpoint)

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        self._checkpoint = dict(checkpoint)


def seed(url: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("CREATE TABLE orders (id INTEGER PRIMARY KEY, version INTEGER, note TEXT)")
            )
            conn.execute(text("CREATE INDEX idx_orders_version ON orders (version)"))
            conn.execute(
                text("INSERT INTO orders VALUES (1, 10, :n), (2, 20, :n)"),
                {"n": CANARY_ROW_VALUE},
            )
    finally:
        engine.dispose()


def run_demo(directory: Path) -> dict[str, RecordingCollector]:
    """Run one successful and one failing poll; return the metrics each one recorded."""
    url = f"sqlite:///{directory / 'orders.db'}"
    seed(url)

    ok_metrics = RecordingCollector()
    ok = PollTrigger(
        name="orders",
        source=SqlAlchemySource(
            url=url, table="orders", cursor_column="version", pk_columns=["id"]
        ),
        checkpoint_store=MemoryStore(),
        metrics=ok_metrics,
    )
    ok.run(timer=None, handler=lambda events: None)

    failed_metrics = RecordingCollector()
    failing = PollTrigger(
        name="orders",
        source=SqlAlchemySource(
            url=url, query=FAILING_QUERY, cursor_column="version", pk_columns=["id"]
        ),
        checkpoint_store=MemoryStore(),
        metrics=failed_metrics,
    )
    with contextlib.suppress(FetchError):  # expected: the metrics are what this demo shows
        failing.run(timer=None, handler=lambda events: None)
    return {"success": ok_metrics, "failure": failed_metrics}


def main() -> None:
    logging.getLogger("azure_functions_db").setLevel(logging.CRITICAL)  # keep demo output short
    with tempfile.TemporaryDirectory() as directory:
        for scenario, metrics in run_demo(Path(directory)).items():
            print(f"[{scenario}]")
            for kind, name, value, labels in metrics.samples:
                shown = value if kind == "counter" else "<varies>"
                print(f"  {kind:9} {name} {shown} {labels}")


if __name__ == "__main__":
    main()
