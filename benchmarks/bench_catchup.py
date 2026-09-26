from __future__ import annotations

from pathlib import Path
import sys
import time

from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from azure_functions_db.adapter import SqlAlchemySource
from azure_functions_db.trigger.normalizers import make_normalizer
from azure_functions_db.trigger.runner import PollRunner


class FakeStateStore:
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


def main() -> None:
    row_count = 10_000
    batch_size = 500
    db_url = "sqlite+pysqlite:///file:phase7_benchmark?mode=memory&cache=shared&uri=true"

    engine = create_engine(db_url)
    metadata = MetaData()
    events = Table(
        "events",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("updated_at", Integer, nullable=False),
        Column("payload", Integer, nullable=False),
    )
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(
            insert(events),
            [{"id": idx, "updated_at": idx, "payload": idx} for idx in range(1, row_count + 1)],
        )

    source = SqlAlchemySource(
        url=db_url,
        table="events",
        cursor_column="updated_at",
        pk_columns=["id"],
    )
    store = FakeStateStore()
    normalizer = make_normalizer(cursor_column="updated_at", pk_columns=["id"])
    batch_counter = 0

    def handler(events_batch: list[object]) -> None:
        nonlocal batch_counter
        batch_counter += 1
        del events_batch

    runner = PollRunner(
        name="benchmark_poller",
        source=source,
        state_store=store,
        normalizer=normalizer,
        handler=handler,
        batch_size=batch_size,
        max_batches_per_tick=row_count,
    )

    total_processed = 0
    started_at = time.perf_counter()
    while True:
        processed = runner.tick()
        if processed == 0:
            break
        total_processed += processed
    elapsed = time.perf_counter() - started_at

    rows_per_second = total_processed / elapsed if elapsed > 0 else 0.0
    print("Catch-up benchmark")
    print(f"Rows processed : {total_processed}")
    print(f"Batches        : {batch_counter}")
    print(f"Elapsed seconds: {elapsed:.4f}")
    print(f"Rows / second  : {rows_per_second:.2f}")


if __name__ == "__main__":
    main()
