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

    @property
    def source_descriptor(self) -> SourceDescriptor:
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
    return RowChange(
        event_id=f"evt-{record.get('id', 0)}",
        op="upsert",
        source=source,
        cursor=record.get("updated_at"),
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
