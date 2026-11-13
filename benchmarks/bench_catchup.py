"""Local catch-up throughput probe for ``PollRunner`` + ``SqlAlchemySource``.

Opt-in. Run it from a development environment where the package is installed::

    pip install -e ".[dev]"
    python benchmarks/bench_catchup.py                     # defaults: 10000 rows, batch 500
    python benchmarks/bench_catchup.py --rows 50000 --batch-size 1000
    python benchmarks/bench_catchup.py --json              # one machine-readable line

What the timing measures: draining ``--rows`` pre-seeded rows from a private, in-memory SQLite
database through repeated ``PollRunner.tick()`` calls with an in-process fake state store and a
no-op handler. It covers cursor queries, row normalization, batching and checkpoint handling in
the runner.

What it does not measure: network latency, a real database server, Azure Blob checkpoint and
lease calls, handler work, concurrency or any Azure hosting. The numbers depend on the machine
and are not a performance claim for production; compare runs only on the same machine and with
the same parameters. The script asserts correctness (every row processed exactly once, the
expected number of batches, the final checkpoint at the last cursor) and enforces no speed
threshold.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
import math
import platform
import sqlite3
import sys
import time
import uuid

import sqlalchemy
from sqlalchemy import Column, Index, Integer, MetaData, Table, create_engine, insert

import azure_functions_db
from azure_functions_db.adapter import SqlAlchemySource
from azure_functions_db.trigger.normalizers import make_normalizer
from azure_functions_db.trigger.runner import PollRunner

DEFAULT_ROWS = 10_000
DEFAULT_BATCH_SIZE = 500
_POLLER_NAME = "catchup_benchmark"


class FakeStateStore:
    """In-process lease and checkpoint store; no Azure Storage involved."""

    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[str, object]] = {}
        self.leases: dict[str, str] = {}
        self.lease_counter: int = 0

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        del ttl_seconds
        self.lease_counter += 1
        lease_id = f"lease-{self.lease_counter}"
        self.leases[poller_name] = lease_id
        return lease_id

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        del poller_name, lease_id, ttl_seconds

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        if self.leases.get(poller_name) == lease_id:
            _ = self.leases.pop(poller_name, None)

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        return dict(self.checkpoints.get(poller_name, {}))

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        if self.leases.get(poller_name) != lease_id:
            msg = "lease lost"
            raise RuntimeError(msg)
        self.checkpoints[poller_name] = dict(checkpoint)


@dataclass(frozen=True)
class CatchupResult:
    """Configuration, environment and measurements of one benchmark run."""

    rows: int
    batch_size: int
    rows_processed: int
    batches: int
    expected_batches: int
    ticks: int
    elapsed_seconds: float
    rows_per_second: float
    python: str
    sqlite: str
    sqlalchemy: str
    azure_functions_db: str
    platform: str


def _require(condition: bool, message: str) -> None:
    """Fail the run with ``AssertionError`` (explicit raise, so ``-O`` cannot disable it)."""
    if not condition:
        raise AssertionError(message)


def run_catchup(rows: int = DEFAULT_ROWS, batch_size: int = DEFAULT_BATCH_SIZE) -> CatchupResult:
    """Seed ``rows`` rows, drain them with a ``PollRunner`` and return the measurements.

    Raises ``AssertionError`` if the run does not process exactly ``rows`` rows in the expected
    number of batches or leaves the checkpoint short of the last cursor. All data lives in a
    private in-memory SQLite database that is disposed before returning.
    """
    if rows < 1:
        msg = "rows must be >= 1"
        raise ValueError(msg)
    if batch_size < 1:
        msg = "batch_size must be >= 1"
        raise ValueError(msg)

    expected_batches = math.ceil(rows / batch_size)
    # Unique name: a shared-cache in-memory database is private to this run.
    db_url = f"sqlite+pysqlite:///file:catchup_{uuid.uuid4().hex}?mode=memory&cache=shared&uri=true"

    seed_engine = create_engine(db_url)
    source: SqlAlchemySource | None = None
    try:
        metadata = MetaData()
        events = Table(
            "events",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("updated_at", Integer, nullable=False),
            Column("payload", Integer, nullable=False),
            Index("idx_events_updated_at", "updated_at"),
        )
        metadata.create_all(seed_engine)
        with seed_engine.begin() as connection:
            connection.execute(
                insert(events),
                [{"id": i, "updated_at": i, "payload": i} for i in range(1, rows + 1)],
            )

        source = SqlAlchemySource(
            url=db_url,
            table="events",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        store = FakeStateStore()
        batch_counter = 0
        seen = 0

        def handler(events_batch: list[object]) -> None:
            nonlocal batch_counter, seen
            batch_counter += 1
            seen += len(events_batch)

        runner = PollRunner(
            name=_POLLER_NAME,
            source=source,
            state_store=store,
            normalizer=make_normalizer(cursor_column="updated_at", pk_columns=["id"]),
            handler=handler,
            batch_size=batch_size,
            max_batches_per_tick=expected_batches,
        )

        total_processed = 0
        ticks = 0
        started_at = time.perf_counter()
        # Each tick drains up to ``expected_batches`` batches, so a correct run finishes in at
        # most two ticks (the second returns 0). The bound turns a regression into a failure
        # instead of an endless loop.
        while ticks < 2 + expected_batches:
            processed = runner.tick()
            ticks += 1
            if processed == 0:
                break
            total_processed += processed
        else:
            _require(False, "catch-up did not terminate")
        elapsed = time.perf_counter() - started_at

        _require(total_processed == rows, f"processed {total_processed} rows, expected {rows}")
        _require(seen == rows, f"handler saw {seen} rows, expected {rows}")
        _require(
            batch_counter == expected_batches,
            f"{batch_counter} batches, expected {expected_batches}",
        )
        checkpoint = store.checkpoints.get(_POLLER_NAME, {})
        _require(bool(checkpoint), "no checkpoint committed")
        cursor = checkpoint.get("cursor")
        _require(
            isinstance(cursor, (tuple, list)) and tuple(cursor) == (rows, rows),
            f"final checkpoint {checkpoint!r} is not the last (cursor, pk) ({rows}, {rows})",
        )
    finally:
        if source is not None:
            source.dispose()
        seed_engine.dispose()

    return CatchupResult(
        rows=rows,
        batch_size=batch_size,
        rows_processed=total_processed,
        batches=batch_counter,
        expected_batches=expected_batches,
        ticks=ticks,
        elapsed_seconds=elapsed,
        rows_per_second=total_processed / elapsed if elapsed > 0 else 0.0,
        python=sys.version.split()[0],
        sqlite=sqlite3.sqlite_version,
        sqlalchemy=sqlalchemy.__version__,
        azure_functions_db=azure_functions_db.__version__,
        platform=platform.platform(),
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local PollRunner catch-up benchmark (opt-in).")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS, help="rows to seed and drain")
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="PollRunner batch size"
    )
    parser.add_argument("--json", action="store_true", help="print one JSON object")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    result = run_catchup(rows=args.rows, batch_size=args.batch_size)
    if args.json:
        print(json.dumps(asdict(result), sort_keys=True))
        return
    print("Catch-up benchmark (local, in-memory SQLite, fake state store)")
    print(f"Rows seeded     : {result.rows}")
    print(f"Batch size      : {result.batch_size}")
    print(f"Rows processed  : {result.rows_processed}")
    print(f"Batches         : {result.batches} (expected {result.expected_batches})")
    print(f"Ticks           : {result.ticks}")
    print(f"Elapsed seconds : {result.elapsed_seconds:.4f}")
    print(f"Rows / second   : {result.rows_per_second:.2f}")
    print(f"Python          : {result.python}")
    print(f"SQLite          : {result.sqlite}")
    print(f"SQLAlchemy      : {result.sqlalchemy}")
    print(f"azure-functions-db: {result.azure_functions_db}")
    print(f"Platform        : {result.platform}")
    print("Timing is machine-dependent and is not a production performance claim.")


if __name__ == "__main__":
    main()
