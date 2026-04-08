from __future__ import annotations

from collections.abc import Sequence

import pytest

from azure_functions_db.core.types import CursorValue, SourceDescriptor
from azure_functions_db.trigger.errors import FetchError, HandlerError
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.normalizers import default_normalizer
from azure_functions_db.trigger.poll import PollTrigger
from azure_functions_db.trigger.runner import RawRecord
from tests.test_trigger_runner import RecordingMetricsCollector


class FakeStateStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[str, object]] = {}
        self.leases: dict[str, str] = {}
        self.lease_counter = 0
        self.acquire_error: Exception | None = None

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        del ttl_seconds
        if self.acquire_error is not None:
            raise self.acquire_error
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
        self.checkpoints[poller_name] = checkpoint


class FakeSourceAdapter:
    cursor_column = "updated_at"
    pk_columns = ["id"]

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
        del cursor, batch_size
        if self.fetch_error is not None:
            raise self.fetch_error
        if self._call_count >= len(self._batches):
            return []
        batch = self._batches[self._call_count]
        self._call_count += 1
        return batch


class FakeSourceAdapterNoCursorPk:
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
        del cursor, batch_size
        if self.fetch_error is not None:
            raise self.fetch_error
        if self._call_count >= len(self._batches):
            return []
        batch = self._batches[self._call_count]
        self._call_count += 1
        return batch


def test_stores_name() -> None:
    trigger = PollTrigger(
        name="my_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    assert trigger.name == "my_poller"  # noqa: S101


def test_run_returns_processed_count() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    handled: list[list[RowChange]] = []

    def handler(events: list[RowChange]) -> None:
        handled.append(events)

    count = trigger.run(timer=object(), handler=handler)

    assert count == 1  # noqa: S101
    assert len(handled) == 1  # noqa: S101
    assert len(handled[0]) == 1  # noqa: S101


def test_run_creates_fresh_runner_each_call() -> None:
    source = FakeSourceAdapter(
        batches=[[{"id": 1, "updated_at": 100}], [{"id": 2, "updated_at": 200}]]
    )
    trigger = PollTrigger(
        name="test_poller",
        source=source,
        checkpoint_store=FakeStateStore(),
    )
    first_calls = 0
    second_calls = 0

    def first_handler(events: list[RowChange]) -> None:
        nonlocal first_calls
        first_calls += len(events)

    def second_handler(events: list[RowChange]) -> None:
        nonlocal second_calls
        second_calls += len(events)

    first_count = trigger.run(timer=object(), handler=first_handler)
    second_count = trigger.run(timer=object(), handler=second_handler)

    assert first_count == 1  # noqa: S101
    assert second_count == 1  # noqa: S101
    assert first_calls == 1  # noqa: S101
    assert second_calls == 1  # noqa: S101


def test_run_accepts_timer_ignores_it() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )

    count = trigger.run(timer={"any": "object"}, handler=lambda events: None)
    assert count == 1  # noqa: S101


def test_run_swallows_lease_acquire_error_returns_zero() -> None:
    store = FakeStateStore()
    store.acquire_error = RuntimeError("lease conflict")
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=store,
    )

    count = trigger.run(timer=object(), handler=lambda events: None)
    assert count == 0  # noqa: S101


def test_run_propagates_fetch_error() -> None:
    source = FakeSourceAdapter(batches=[])
    source.fetch_error = RuntimeError("db unavailable")
    trigger = PollTrigger(
        name="test_poller",
        source=source,
        checkpoint_store=FakeStateStore(),
    )

    with pytest.raises(FetchError, match="Failed to fetch"):
        trigger.run(timer=object(), handler=lambda events: None)


def test_run_propagates_handler_error() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )

    def bad_handler(events: list[RowChange]) -> None:
        raise ValueError("boom")

    with pytest.raises(HandlerError, match="Handler failed"):
        trigger.run(timer=object(), handler=bad_handler)


def test_run_rejects_async_handler() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )

    async def async_handler(events: list[RowChange]) -> None:
        del events

    with pytest.raises(TypeError, match="Async handlers are not supported"):
        trigger.run(timer=object(), handler=async_handler)


def test_run_uses_make_normalizer_when_source_has_cursor_pk() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100, "name": "A"}]]),
        checkpoint_store=FakeStateStore(),
    )
    handled: list[list[RowChange]] = []

    def handler(events: list[RowChange]) -> None:
        handled.append(events)

    count = trigger.run(timer=object(), handler=handler)

    assert count == 1  # noqa: S101
    assert handled[0][0].cursor == (100, 1)  # noqa: S101
    assert handled[0][0].pk == {"id": 1}  # noqa: S101


def test_source_without_metadata_raises_valueerror() -> None:
    with pytest.raises(ValueError, match="Cannot auto-detect normalizer"):
        PollTrigger(
            name="test_poller",
            source=FakeSourceAdapterNoCursorPk(batches=[]),
            checkpoint_store=FakeStateStore(),
        )


def test_explicit_normalizer_used() -> None:
    calls: list[tuple[RawRecord, SourceDescriptor]] = []

    def explicit_normalizer(record: RawRecord, descriptor: SourceDescriptor) -> RowChange:
        calls.append((record, descriptor))
        return default_normalizer(record, descriptor)

    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapterNoCursorPk(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
        normalizer=explicit_normalizer,
    )

    count = trigger.run(timer=object(), handler=lambda events: None)

    assert count == 1  # noqa: S101
    assert len(calls) == 1  # noqa: S101
    assert calls[0][0] == {"id": 1, "updated_at": 100}  # noqa: S101


def test_async_handler_callable_object() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )

    class AsyncCallableHandler:
        async def __call__(self, events: list[RowChange]) -> None:
            del events

    with pytest.raises(TypeError, match="Async handlers are not supported"):
        trigger.run(timer=object(), handler=AsyncCallableHandler())


def test_metrics_forwarded() -> None:
    metrics = RecordingMetricsCollector()
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
        metrics=metrics,
    )

    trigger.run(timer=object(), handler=lambda events: None)

    assert metrics.increments  # noqa: S101


def test_default_metrics_works() -> None:
    trigger = PollTrigger(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )

    assert trigger.run(timer=object(), handler=lambda events: None) == 1  # noqa: S101


def test_batch_size_validation() -> None:
    with pytest.raises(ValueError, match="batch_size must be >= 1"):
        PollTrigger(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
            batch_size=0,
        )


def test_max_batches_validation() -> None:
    with pytest.raises(ValueError, match="max_batches_per_tick must be >= 1"):
        PollTrigger(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
            max_batches_per_tick=0,
        )


def test_lease_ttl_validation() -> None:
    with pytest.raises(ValueError, match="lease_ttl_seconds must be >= 1"):
        PollTrigger(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
            lease_ttl_seconds=0,
        )
