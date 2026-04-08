from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, select

from azure_functions_db.adapter.sqlalchemy import SqlAlchemySource
from azure_functions_db.binding.reader import DbReader
from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.poll import PollTrigger


class FakeStateStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[str, object]] = {}
        self.leases: dict[str, str] = {}
        self.lease_counter = 0
        self.commit_error: Exception | None = None

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        del ttl_seconds
        self.lease_counter += 1
        lease_id = f"lease-{self.lease_counter}"
        self.leases[poller_name] = lease_id
        return lease_id

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        del poller_name, lease_id, ttl_seconds

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        del lease_id
        self.leases.pop(poller_name, None)

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        return self.checkpoints.get(poller_name, {})

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        del lease_id
        if self.commit_error is not None:
            raise self.commit_error
        self.checkpoints[poller_name] = checkpoint


def _create_source_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("updated_at", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["orders"]),
            [
                {"id": 1, "name": "Alice", "updated_at": 100},
                {"id": 2, "name": "Bob", "updated_at": 200},
                {"id": 3, "name": "Charlie", "updated_at": 300},
            ],
        )
    engine.dispose()
    return url


def _create_dest_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "processed",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("cursor_val", String(100)),
    )
    metadata.create_all(engine)
    engine.dispose()
    return url


def _read_all(url: str, table_name: str) -> list[dict[str, object]]:
    engine = create_engine(url)
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    tbl = metadata.tables[table_name]
    with engine.connect() as conn:
        result = conn.execute(select(tbl))
        rows = [dict(row._mapping) for row in result]
    engine.dispose()
    return rows


@pytest.fixture()
def source_url(tmp_path: Path) -> str:
    return _create_source_db(tmp_path / "source.db")


@pytest.fixture()
def dest_url(tmp_path: Path) -> str:
    return _create_dest_db(tmp_path / "dest.db")


class TestTriggerWithWriter:
    def test_trigger_detects_changes_and_writes_to_dest(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()
        trigger = PollTrigger(
            name="orders_integ",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        def handler(events: list[RowChange]) -> None:
            with DbWriter(url=dest_url, table="processed") as writer:
                for event in events:
                    assert event.after is not None
                    writer.insert(
                        data={
                            "id": event.pk["id"],
                            "name": event.after["name"],
                            "cursor_val": str(event.cursor),
                        }
                    )

        try:
            count = trigger.run(timer=object(), handler=handler)
        finally:
            source.dispose()

        assert count == 3
        rows = _read_all(dest_url, "processed")
        assert len(rows) == 3
        by_id = {r["id"]: r for r in rows}
        assert by_id[1]["name"] == "Alice"
        assert by_id[2]["name"] == "Bob"
        assert by_id[3]["name"] == "Charlie"

    def test_second_tick_with_checkpoint_is_noop(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()
        trigger = PollTrigger(
            name="orders_integ",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        def handler(events: list[RowChange]) -> None:
            with DbWriter(url=dest_url, table="processed") as writer:
                for event in events:
                    assert event.after is not None
                    writer.upsert(
                        data={
                            "id": event.pk["id"],
                            "name": event.after["name"],
                            "cursor_val": str(event.cursor),
                        },
                        conflict_columns=["id"],
                    )

        try:
            first_count = trigger.run(timer=object(), handler=handler)
            assert first_count == 3

            second_count = trigger.run(timer=object(), handler=handler)
            assert second_count == 0
        finally:
            source.dispose()

        rows = _read_all(dest_url, "processed")
        assert len(rows) == 3


class TestTriggerWithReaderAndWriter:
    def test_trigger_reads_enriched_data_and_writes(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()
        trigger = PollTrigger(
            name="orders_enrich",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        def handler(events: list[RowChange]) -> None:
            reader = DbReader(url=source_url, table="orders")
            writer = DbWriter(url=dest_url, table="processed")
            try:
                for event in events:
                    row = reader.get(pk={"id": event.pk["id"]})
                    if row is not None:
                        writer.insert(
                            data={
                                "id": row["id"],
                                "name": row["name"],
                                "cursor_val": str(event.cursor),
                            }
                        )
            finally:
                reader.close()
                writer.close()

        try:
            count = trigger.run(timer=object(), handler=handler)
        finally:
            source.dispose()

        assert count == 3
        rows = _read_all(dest_url, "processed")
        assert len(rows) == 3
        names = sorted(str(r["name"]) for r in rows)
        assert names == ["Alice", "Bob", "Charlie"]


class TestEngineProviderSharing:
    def test_shared_engine_provider_across_reader_and_writer(
        self, source_url: str, dest_url: str
    ) -> None:
        provider = EngineProvider()
        try:
            source = SqlAlchemySource(
                url=source_url,
                table="orders",
                cursor_column="updated_at",
                pk_columns=["id"],
                engine_provider=provider,
            )
            state_store = FakeStateStore()
            trigger = PollTrigger(
                name="shared_engine",
                source=source,
                checkpoint_store=state_store,
                batch_size=100,
            )

            def handler(events: list[RowChange]) -> None:
                with DbWriter(
                    url=dest_url, table="processed", engine_provider=provider
                ) as writer:
                    for event in events:
                        assert event.after is not None
                        writer.upsert(
                            data={
                                "id": event.pk["id"],
                                "name": event.after["name"],
                                "cursor_val": str(event.cursor),
                            },
                            conflict_columns=["id"],
                        )

            count = trigger.run(timer=object(), handler=handler)

            assert count == 3
            rows = _read_all(dest_url, "processed")
            assert len(rows) == 3
        finally:
            provider.dispose_all()

    def test_reader_and_writer_same_db_shared_provider(
        self, source_url: str
    ) -> None:
        provider = EngineProvider()
        try:
            with DbReader(
                url=source_url, table="orders", engine_provider=provider
            ) as reader:
                row = reader.get(pk={"id": 1})
                assert row is not None
                assert row["name"] == "Alice"

            dest_url = source_url
            with DbWriter(
                url=dest_url, table="orders", engine_provider=provider
            ) as writer:
                writer.update(data={"name": "Alice Updated"}, pk={"id": 1})

            with DbReader(
                url=source_url, table="orders", engine_provider=provider
            ) as reader:
                row = reader.get(pk={"id": 1})
                assert row is not None
                assert row["name"] == "Alice Updated"
        finally:
            provider.dispose_all()


class TestCheckpointResumeAfterFailure:
    def test_handler_failure_does_not_commit_checkpoint(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()
        trigger = PollTrigger(
            name="fail_resume",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        call_count = 0

        def handler(events: list[RowChange]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                with DbWriter(url=dest_url, table="processed") as writer:
                    for event in events:
                        assert event.after is not None
                        writer.insert(
                            data={
                                "id": event.pk["id"],
                                "name": event.after["name"],
                                "cursor_val": str(event.cursor),
                            }
                        )
                raise RuntimeError("Simulated handler failure")

        from azure_functions_db.trigger.errors import HandlerError

        try:
            with pytest.raises(HandlerError):
                trigger.run(timer=object(), handler=handler)
        finally:
            source.dispose()

        assert "fail_resume" not in state_store.checkpoints

    def test_commit_failure_preserves_writes_and_allows_retry(
        self, source_url: str, dest_url: str
    ) -> None:
        """Test at-least-once: writes succeed but checkpoint commit fails.

        The handler writes via upsert, but checkpoint commit raises an error.
        On retry (after clearing the error), the same rows are upserted again
        idempotently, resulting in no duplicates.
        """
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()

        def handler(events: list[RowChange]) -> None:
            with DbWriter(url=dest_url, table="processed") as writer:
                for event in events:
                    assert event.after is not None
                    writer.upsert(
                        data={
                            "id": event.pk["id"],
                            "name": event.after["name"],
                            "cursor_val": str(event.cursor),
                        },
                        conflict_columns=["id"],
                    )

        # First run: writes succeed but checkpoint commit fails
        state_store.commit_error = RuntimeError("Checkpoint commit failure")
        trigger1 = PollTrigger(
            name="commit_fail",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        try:
            # The commit error should propagate (not silently swallowed)
            with pytest.raises(Exception):  # noqa: B017, PT011
                trigger1.run(timer=object(), handler=handler)

            # Checkpoint was NOT persisted
            assert "commit_fail" not in state_store.checkpoints

            # But writes DID land in the destination
            rows = _read_all(dest_url, "processed")
            assert len(rows) == 3

            # Retry: clear the error, run again — upsert is idempotent
            state_store.commit_error = None
            trigger2 = PollTrigger(
                name="commit_fail",
                source=source,
                checkpoint_store=state_store,
                batch_size=100,
            )
            count = trigger2.run(timer=object(), handler=handler)
            assert count == 3

            # Still exactly 3 rows (no duplicates)
            rows = _read_all(dest_url, "processed")
            assert len(rows) == 3

            # Checkpoint is now persisted
            assert "commit_fail" in state_store.checkpoints
        finally:
            source.dispose()

    def test_retry_with_upsert_is_idempotent(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()

        def handler(events: list[RowChange]) -> None:
            with DbWriter(url=dest_url, table="processed") as writer:
                for event in events:
                    assert event.after is not None
                    writer.upsert(
                        data={
                            "id": event.pk["id"],
                            "name": event.after["name"],
                            "cursor_val": str(event.cursor),
                        },
                        conflict_columns=["id"],
                    )

        trigger1 = PollTrigger(
            name="idempotent_test",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        try:
            count1 = trigger1.run(timer=object(), handler=handler)
            assert count1 == 3

            state_store.checkpoints.clear()

            trigger2 = PollTrigger(
                name="idempotent_test",
                source=source,
                checkpoint_store=state_store,
                batch_size=100,
            )
            count2 = trigger2.run(timer=object(), handler=handler)
            assert count2 == 3
        finally:
            source.dispose()

        rows = _read_all(dest_url, "processed")
        assert len(rows) == 3
        by_id = {r["id"]: r for r in rows}
        assert by_id[1]["name"] == "Alice"
        assert by_id[2]["name"] == "Bob"
        assert by_id[3]["name"] == "Charlie"


class TestUpsertManyBatchIntegration:
    def test_trigger_with_batch_upsert(
        self, source_url: str, dest_url: str
    ) -> None:
        source = SqlAlchemySource(
            url=source_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        state_store = FakeStateStore()
        trigger = PollTrigger(
            name="batch_upsert",
            source=source,
            checkpoint_store=state_store,
            batch_size=100,
        )

        def handler(events: list[RowChange]) -> None:
            rows: list[dict[str, object]] = []
            for event in events:
                assert event.after is not None
                rows.append(
                    {
                        "id": event.pk["id"],
                        "name": event.after["name"],
                        "cursor_val": str(event.cursor),
                    }
                )
            if rows:
                with DbWriter(url=dest_url, table="processed") as writer:
                    writer.upsert_many(rows=rows, conflict_columns=["id"])

        try:
            count = trigger.run(timer=object(), handler=handler)
        finally:
            source.dispose()

        assert count == 3
        rows = _read_all(dest_url, "processed")
        assert len(rows) == 3
