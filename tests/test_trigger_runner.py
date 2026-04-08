from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import pytest

from azure_functions_db.core.types import CursorValue, SourceDescriptor
from azure_functions_db.trigger.context import PollContext
from azure_functions_db.trigger.errors import (
    CommitError,
    FetchError,
    HandlerError,
    LeaseAcquireError,
    LostLeaseError,
    SerializationError,
    SourceConfigurationError,
)
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.runner import PollRunner, RawRecord


class FakeStateStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[str, object]] = {}
        self.leases: dict[str, str] = {}
        self.lease_counter = 0
        self.acquire_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.load_error: Exception | None = None

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        if self.acquire_error:
            raise self.acquire_error
        self.lease_counter += 1
        lease_id = f"lease-{self.lease_counter}"
        self.leases[poller_name] = lease_id
        return lease_id

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        pass

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        self.leases.pop(poller_name, None)

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        if self.load_error:
            raise self.load_error
        return self.checkpoints.get(poller_name, {})

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        if self.commit_error:
            raise self.commit_error
        self.checkpoints[poller_name] = checkpoint


class FakeSourceAdapter:
    def __init__(self, batches: list[list[RawRecord]]) -> None:
        self._batches = list(batches)
        self._call_count = 0
        self._descriptor = SourceDescriptor(
            name="test_table", kind="sqlalchemy", fingerprint="fp_test"
        )
        self.fetch_error: Exception | None = None
        self.descriptor_error: Exception | None = None

    @property
    def source_descriptor(self) -> SourceDescriptor:
        if self.descriptor_error:
            raise self.descriptor_error
        return self._descriptor

    def fetch(
        self, cursor: CursorValue | None, batch_size: int
    ) -> Sequence[RawRecord]:
        if self.fetch_error:
            raise self.fetch_error
        if self._call_count >= len(self._batches):
            return []
        batch = self._batches[self._call_count]
        self._call_count += 1
        return batch


def _default_normalizer(record: RawRecord, source: SourceDescriptor) -> RowChange:
    raw_cursor = record.get("updated_at")
    cursor = (
        raw_cursor
        if isinstance(raw_cursor, (str, int, float, bool)) or raw_cursor is None
        else None
    )
    return RowChange(
        event_id=f"evt-{record.get('id', 0)}",
        op="upsert",
        source=source,
        cursor=cursor,
        pk={"id": record.get("id", 0)},
        before=None,
        after=dict(record),
    )


class TestPollRunner:
    def test_tick_processes_single_batch(self) -> None:
        records: list[RawRecord] = [
            {"id": 1, "updated_at": 100, "name": "a"},
            {"id": 2, "updated_at": 200, "name": "b"},
        ]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        handled: list[list[RowChange]] = []

        def handler(events: list[RowChange]) -> None:
            handled.append(events)

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=handler,
            batch_size=100,
        )

        count = runner.tick()

        assert count == 2
        assert len(handled) == 1
        assert len(handled[0]) == 2
        assert handled[0][0].event_id == "evt-1"
        assert handled[0][1].event_id == "evt-2"

    def test_tick_advances_checkpoint(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()

        checkpoint = store.checkpoints["test_poller"]
        assert checkpoint["cursor"] == 100

    def test_tick_empty_batch_returns_zero(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        count = runner.tick()
        assert count == 0

    def test_tick_multiple_batches(self) -> None:
        batch1: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        batch2: list[RawRecord] = [{"id": 2, "updated_at": 200}]
        source = FakeSourceAdapter(batches=[batch1, batch2])
        store = FakeStateStore()
        call_count = 0

        def handler(events: list[RowChange]) -> None:
            nonlocal call_count
            call_count += 1

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=handler,
            max_batches_per_tick=3,
        )

        count = runner.tick()

        assert count == 2
        assert call_count == 2
        assert store.checkpoints["test_poller"]["cursor"] == 200

    def test_handler_with_context(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        contexts: list[PollContext] = []

        def handler(events: list[RowChange], context: PollContext) -> None:
            contexts.append(context)

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=handler,
        )

        runner.tick()

        assert len(contexts) == 1
        ctx = contexts[0]
        assert ctx.poller_name == "test_poller"
        assert ctx.source_name == "test_table"
        assert isinstance(ctx.tick_started_at, datetime)

    def test_lease_acquire_failure_raises(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()
        store.acquire_error = RuntimeError("blob unavailable")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(LeaseAcquireError, match="Failed to acquire lease"):
            runner.tick()

    def test_fetch_failure_raises(self) -> None:
        source = FakeSourceAdapter(batches=[])
        source.fetch_error = RuntimeError("db connection lost")
        store = FakeStateStore()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(FetchError, match="Failed to fetch"):
            runner.tick()

    def test_handler_failure_raises(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        def bad_handler(events: list[RowChange]) -> None:
            raise ValueError("processing failed")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=bad_handler,
        )

        with pytest.raises(HandlerError, match="Handler failed"):
            runner.tick()

    def test_commit_failure_raises(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        store.commit_error = RuntimeError("blob write failed")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(CommitError, match="Checkpoint commit failed"):
            runner.tick()

    def test_handler_failure_does_not_advance_checkpoint(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        def bad_handler(events: list[RowChange]) -> None:
            raise ValueError("fail")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=bad_handler,
        )

        with pytest.raises(HandlerError):
            runner.tick()

        assert "test_poller" not in store.checkpoints

    def test_lease_released_on_success(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()
        assert "test_poller" not in store.leases

    def test_lease_released_on_handler_failure(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        def bad_handler(events: list[RowChange]) -> None:
            raise ValueError("fail")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=bad_handler,
        )

        with pytest.raises(HandlerError):
            runner.tick()

        assert "test_poller" not in store.leases

    def test_name_property(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        runner = PollRunner(
            name="my_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        assert runner.name == "my_poller"

    def test_async_handler_rejected(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        async def async_handler(events: list[RowChange]) -> None:
            pass

        with pytest.raises(TypeError, match="Async handlers are not supported"):
            PollRunner(
                name="test_poller",
                source=source,
                state_store=store,
                normalizer=_default_normalizer,
                handler=async_handler,
            )

    def test_invalid_batch_size_rejected(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        with pytest.raises(ValueError, match="batch_size must be >= 1"):
            PollRunner(
                name="test_poller",
                source=source,
                state_store=store,
                normalizer=_default_normalizer,
                handler=lambda events: None,
                batch_size=0,
            )

    def test_invalid_max_batches_rejected(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        with pytest.raises(ValueError, match="max_batches_per_tick must be >= 1"):
            PollRunner(
                name="test_poller",
                source=source,
                state_store=store,
                normalizer=_default_normalizer,
                handler=lambda events: None,
                max_batches_per_tick=0,
            )

    def test_invalid_lease_ttl_rejected(self) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()

        with pytest.raises(ValueError, match="lease_ttl_seconds must be >= 1"):
            PollRunner(
                name="test_poller",
                source=source,
                state_store=store,
                normalizer=_default_normalizer,
                handler=lambda events: None,
                lease_ttl_seconds=0,
            )

    def test_load_checkpoint_failure_raises_fetch_error(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        store.load_error = RuntimeError("blob read failed")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(FetchError, match="Failed to load checkpoint"):
            runner.tick()

    def test_source_descriptor_failure_raises(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        source.descriptor_error = RuntimeError("config broken")
        store = FakeStateStore()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(
            SourceConfigurationError,
            match="Failed to get source descriptor",
        ):
            runner.tick()

    def test_normalizer_failure_raises_serialization_error(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()

        def bad_normalizer(
            record: RawRecord, source: SourceDescriptor
        ) -> RowChange:
            msg = "bad data"
            raise ValueError(msg)

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=bad_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(
            SerializationError, match="Failed to normalize records"
        ):
            runner.tick()

    def test_lost_lease_error_propagates(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        store.commit_error = LostLeaseError("lease stolen")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(LostLeaseError, match="lease stolen"):
            runner.tick()

    def test_composite_cursor_list_roundtrip(self) -> None:
        source = FakeSourceAdapter(
            batches=[[{"id": 1, "updated_at": 100}]]
        )
        store = FakeStateStore()
        store.checkpoints["test_poller"] = {
            "cursor": ["2026-01-01", 42],
        }

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()
        assert store.checkpoints["test_poller"]["cursor"] == 100

    def test_unsupported_cursor_type_raises(self) -> None:
        source = FakeSourceAdapter(
            batches=[[{"id": 1, "updated_at": 100}]]
        )
        store = FakeStateStore()
        store.checkpoints["test_poller"] = {
            "cursor": {"complex": "object"},
        }

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with pytest.raises(SerializationError, match="Unsupported cursor type"):
            runner.tick()
