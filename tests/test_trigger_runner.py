from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
import logging

import pytest

from azure_functions_db.core.types import CursorValue, RawRecord, SourceDescriptor
from azure_functions_db.observability import (
    METRIC_BATCHES_TOTAL,
    METRIC_COMMIT_DURATION_MS,
    METRIC_EVENTS_TOTAL,
    METRIC_FAILURES_TOTAL,
    METRIC_FETCH_DURATION_MS,
    METRIC_HANDLER_DURATION_MS,
    METRIC_LAG_SECONDS,
    METRIC_LAST_SUCCESS_TIMESTAMP,
    NoOpCollector,
)
from azure_functions_db.state.errors import LeaseConflictError
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
from azure_functions_db.trigger.runner import PollRunner


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


class RecordingMetricsCollector:
    def __init__(self) -> None:
        self.increments: list[tuple[str, float, Mapping[str, str] | None]] = []
        self.observations: list[tuple[str, float, Mapping[str, str] | None]] = []
        self.gauges: list[tuple[str, float, Mapping[str, str] | None]] = []

    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        self.increments.append((name, value, labels))

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        self.observations.append((name, value, labels))

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        self.gauges.append((name, value, labels))


class IncrementFailingCollector(RecordingMetricsCollector):
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        msg = "increment failed"
        raise RuntimeError(msg)


class GaugeFailingCollector(RecordingMetricsCollector):
    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        msg = "gauge failed"
        raise RuntimeError(msg)


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
    def test_accepts_metrics_collector(self) -> None:
        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=RecordingMetricsCollector(),
        )

        assert runner is not None  # noqa: S101

    def test_default_metrics_is_noop(self) -> None:
        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        assert isinstance(runner._metrics, NoOpCollector)  # noqa: SLF001,S101

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

    def test_successful_tick_emits_batch_metrics(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        assert (
            METRIC_BATCHES_TOTAL,
            1,
            {"poller_name": "test_poller", "source": "test_table", "result": "success"},
        ) in metrics.increments  # noqa: S101
        assert (
            METRIC_EVENTS_TOTAL,
            1.0,
            {"poller_name": "test_poller", "source": "test_table"},
        ) in metrics.increments  # noqa: S101

    def test_successful_tick_emits_duration_histograms(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        observed_names = {name for name, _, _ in metrics.observations}
        assert METRIC_FETCH_DURATION_MS in observed_names  # noqa: S101
        assert METRIC_HANDLER_DURATION_MS in observed_names  # noqa: S101
        assert METRIC_COMMIT_DURATION_MS in observed_names  # noqa: S101

    def test_successful_tick_emits_last_success_timestamp(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        last_success = [
            gauge
            for gauge in metrics.gauges
            if gauge[0] == METRIC_LAST_SUCCESS_TIMESTAMP
        ]
        assert len(last_success) == 1  # noqa: S101
        assert last_success[0][2] == {"poller_name": "test_poller"}  # noqa: S101

    def test_failure_emits_failure_counter(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        def bad_handler(events: list[RowChange]) -> None:
            raise ValueError("processing failed")

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=bad_handler,
            metrics=metrics,
        )

        with pytest.raises(HandlerError):
            runner.tick()

        assert (
            METRIC_FAILURES_TOTAL,
            1,
            {
                "poller_name": "test_poller",
                "error_type": "ValueError",
                "source": "test_table",
            },
        ) in metrics.increments  # noqa: S101
        assert (
            METRIC_BATCHES_TOTAL,
            1,
            {
                "poller_name": "test_poller",
                "result": "failure",
                "source": "test_table",
            },
        ) in metrics.increments  # noqa: S101

    def test_fetch_failure_emits_failure_counter(self) -> None:
        source = FakeSourceAdapter(batches=[])
        source.fetch_error = RuntimeError("db connection lost")
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        with pytest.raises(FetchError):
            runner.tick()

        assert (
            METRIC_FAILURES_TOTAL,
            1,
            {
                "poller_name": "test_poller",
                "error_type": "RuntimeError",
                "source": "test_table",
            },
        ) in metrics.increments  # noqa: S101

    def test_collector_exception_does_not_break_tick(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=IncrementFailingCollector(),
        )

        assert runner.tick() == 1

    def test_batch_complete_log_checkpoint_before_is_old(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        store = FakeStateStore()
        store.checkpoints["test_poller"] = {
            "cursor": 50,
            "batch_id": "prior-batch",
        }
        caplog.set_level(logging.INFO)

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()

        batch_complete = next(
            record
            for record in caplog.records
            if getattr(record, "event", None) == "batch_complete"
        )
        batch_id = getattr(batch_complete, "batch_id")
        checkpoint_before = getattr(batch_complete, "checkpoint_before")
        checkpoint_after = getattr(batch_complete, "checkpoint_after")

        assert checkpoint_before == {
            "cursor": 50,
            "batch_id": "prior-batch",
        }
        assert checkpoint_after == {
            "cursor": 100,
            "batch_id": batch_id,
        }

    def test_lag_negative_clamped_to_zero(self) -> None:
        future_cursor = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        records: list[RawRecord] = [{"id": 1, "updated_at": future_cursor}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        lag_gauges = [
            gauge for gauge in metrics.gauges if gauge[0] == METRIC_LAG_SECONDS
        ]
        assert len(lag_gauges) == 1  # noqa: S101
        assert lag_gauges[0][1] == 0.0  # noqa: S101

    def test_lag_naive_datetime_cursor_skipped(self) -> None:
        naive_cursor = datetime.now().replace(microsecond=0).isoformat()
        records: list[RawRecord] = [{"id": 1, "updated_at": naive_cursor}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        assert all(
            name != METRIC_LAG_SECONDS for name, _, _ in metrics.gauges
        )  # noqa: S101

    def test_collector_exception_on_gauge_does_not_break_tick(self) -> None:
        lagging_cursor = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        records: list[RawRecord] = [{"id": 1, "updated_at": lagging_cursor}]

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=GaugeFailingCollector(),
        )

        assert runner.tick() == 1

    def test_empty_batch_no_batch_metrics(self) -> None:
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        assert all(name != METRIC_EVENTS_TOTAL for name, _, _ in metrics.increments)  # noqa: S101
        assert all(name != METRIC_FETCH_DURATION_MS for name, _, _ in metrics.observations)  # noqa: S101

    def test_metric_labels_contain_poller_name(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        labels = [
            labels
            for _, _, labels in (
                metrics.increments + metrics.observations + metrics.gauges
            )
            if labels is not None
        ]
        assert labels  # noqa: S101
        assert all("poller_name" in item for item in labels)  # noqa: S101

    def test_metric_labels_no_high_cardinality(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        metrics = RecordingMetricsCollector()

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
            metrics=metrics,
        )

        runner.tick()

        labels = [
            labels
            for _, _, labels in (
                metrics.increments + metrics.observations + metrics.gauges
            )
            if labels is not None
        ]
        forbidden = {"invocation_id", "batch_id", "lease_owner"}
        assert all(forbidden.isdisjoint(item.keys()) for item in labels)  # noqa: S101

    def test_structured_log_extra_fields_present(self, caplog: pytest.LogCaptureFixture) -> None:
        records: list[RawRecord] = [
            {
                "id": 1,
                "updated_at": (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
            }
        ]
        caplog.set_level(logging.DEBUG)

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()

        event_records = [
            record
            for record in caplog.records
            if getattr(record, "poller_name", None) == "test_poller"
        ]
        assert event_records  # noqa: S101
        required_fields = {
            "event",
            "poller_name",
            "invocation_id",
            "batch_id",
            "source",
            "schedule_time",
            "fetched_count",
            "batch_size",
            "committed",
            "checkpoint_before",
            "checkpoint_after",
            "lease_owner",
            "duration_ms",
            "fetch_duration_ms",
            "handler_duration_ms",
            "commit_duration_ms",
            "lag_seconds",
            "error_type",
            "result",
        }
        for record in event_records:
            for field in required_fields:
                assert hasattr(record, field)  # noqa: S101

    def test_structured_log_event_names(self, caplog: pytest.LogCaptureFixture) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        caplog.set_level(logging.DEBUG)

        runner = PollRunner(
            name="test_poller",
            source=FakeSourceAdapter(batches=[records]),
            state_store=FakeStateStore(),
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        runner.tick()

        events = {getattr(record, "event", None) for record in caplog.records}
        assert "tick_start" in events  # noqa: S101
        assert "batch_complete" in events  # noqa: S101
        assert "tick_complete" in events  # noqa: S101

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

    def test_lease_conflict_is_silent_noop(self) -> None:
        records: list[RawRecord] = [{"id": 1, "updated_at": 100}]
        source = FakeSourceAdapter(batches=[records])
        store = FakeStateStore()
        store.acquire_error = LeaseConflictError("held by another instance")
        store.load_error = AssertionError("checkpoint must not be loaded on conflict")
        metrics = RecordingMetricsCollector()

        handler_calls: list[list[RowChange]] = []

        def handler(events: list[RowChange]) -> None:
            handler_calls.append(events)

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=handler,
            metrics=metrics,
        )

        result = runner.tick()

        assert result == 0
        assert handler_calls == []
        assert all(
            name != METRIC_FAILURES_TOTAL
            for name, _value, _labels in metrics.increments
        )
        assert all(
            not (
                name == METRIC_BATCHES_TOTAL
                and (labels or {}).get("result") == "failure"
            )
            for name, _value, labels in metrics.increments
        )

    def test_lease_conflict_logs_at_debug_not_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        source = FakeSourceAdapter(batches=[])
        store = FakeStateStore()
        store.acquire_error = LeaseConflictError("held by another instance")

        runner = PollRunner(
            name="test_poller",
            source=source,
            state_store=store,
            normalizer=_default_normalizer,
            handler=lambda events: None,
        )

        with caplog.at_level(
            logging.DEBUG, logger="azure_functions_db.trigger.runner"
        ):
            runner.tick()

        assert any(
            record.levelno == logging.DEBUG
            and getattr(record, "event", None) == "lease_acquire_skipped"
            for record in caplog.records
        )
        assert not any(
            record.levelno >= logging.ERROR for record in caplog.records
        )

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

        with pytest.raises(SerializationError, match="Unsupported cursor"):
            runner.tick()
