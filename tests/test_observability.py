from __future__ import annotations

from collections.abc import Mapping

from azure_functions_db.observability import (
    METRIC_BATCH_SIZE,
    METRIC_BATCHES_TOTAL,
    METRIC_COMMIT_DURATION_MS,
    METRIC_EVENTS_TOTAL,
    METRIC_FAILURES_TOTAL,
    METRIC_FETCH_DURATION_MS,
    METRIC_HANDLER_DURATION_MS,
    METRIC_LAG_SECONDS,
    METRIC_LAST_SUCCESS_TIMESTAMP,
    MetricsCollector,
    NoOpCollector,
    build_log_fields,
)


class CustomCollector:
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        del name, value, labels

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        del name, value, labels

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        del name, value, labels


def test_metrics_collector_is_runtime_checkable_protocol() -> None:
    assert isinstance(NoOpCollector(), MetricsCollector)  # noqa: S101


def test_noop_collector_satisfies_protocol() -> None:
    collector = NoOpCollector()
    assert isinstance(collector, MetricsCollector)  # noqa: S101


def test_noop_collector_methods_do_not_raise() -> None:
    collector = NoOpCollector()
    collector.increment("metric")
    collector.observe("metric", 1.23)
    collector.set_gauge("metric", 4.56)


def test_noop_collector_with_labels() -> None:
    collector = NoOpCollector()
    labels = {"poller_name": "orders", "source": "orders_table"}
    collector.increment("metric", labels=labels)
    collector.observe("metric", 1.0, labels=labels)
    collector.set_gauge("metric", 2.0, labels=labels)


def test_all_metric_constants_exist_and_are_strings() -> None:
    metric_names = [
        METRIC_BATCHES_TOTAL,
        METRIC_EVENTS_TOTAL,
        METRIC_FAILURES_TOTAL,
        METRIC_FETCH_DURATION_MS,
        METRIC_HANDLER_DURATION_MS,
        METRIC_COMMIT_DURATION_MS,
        METRIC_BATCH_SIZE,
        METRIC_LAG_SECONDS,
        METRIC_LAST_SUCCESS_TIMESTAMP,
    ]

    assert all(isinstance(name, str) for name in metric_names)  # noqa: S101


def test_build_log_fields_defaults() -> None:
    fields = build_log_fields(event="tick_start", poller_name="orders")

    assert fields == {
        "event": "tick_start",
        "poller_name": "orders",
        "invocation_id": None,
        "batch_id": None,
        "source": None,
        "schedule_time": None,
        "fetched_count": None,
        "batch_size": None,
        "committed": None,
        "checkpoint_before": None,
        "checkpoint_after": None,
        "lease_owner": None,
        "duration_ms": None,
        "fetch_duration_ms": None,
        "handler_duration_ms": None,
        "commit_duration_ms": None,
        "lag_seconds": None,
        "error_type": None,
        "result": None,
    }


def test_build_log_fields_all_fields() -> None:
    fields = build_log_fields(
        event="batch_complete",
        poller_name="orders",
        invocation_id="inv-1",
        batch_id="batch-1",
        source="orders_table",
        schedule_time="2026-04-08T00:00:00+00:00",
        fetched_count=10,
        batch_size=25,
        committed=True,
        checkpoint_before={"cursor": 1},
        checkpoint_after={"cursor": 2},
        lease_owner="lease-1",
        duration_ms=100.0,
        fetch_duration_ms=10.0,
        handler_duration_ms=20.0,
        commit_duration_ms=5.0,
        lag_seconds=30.0,
        error_type="ValueError",
        result="success",
    )

    assert fields["event"] == "batch_complete"  # noqa: S101
    assert fields["poller_name"] == "orders"  # noqa: S101
    assert fields["invocation_id"] == "inv-1"  # noqa: S101
    assert fields["batch_id"] == "batch-1"  # noqa: S101
    assert fields["source"] == "orders_table"  # noqa: S101
    assert fields["schedule_time"] == "2026-04-08T00:00:00+00:00"  # noqa: S101
    assert fields["fetched_count"] == 10  # noqa: S101
    assert fields["batch_size"] == 25  # noqa: S101
    assert fields["committed"] is True  # noqa: S101
    assert fields["checkpoint_before"] == {"cursor": 1}  # noqa: S101
    assert fields["checkpoint_after"] == {"cursor": 2}  # noqa: S101
    assert fields["lease_owner"] == "lease-1"  # noqa: S101
    assert fields["duration_ms"] == 100.0  # noqa: S101
    assert fields["fetch_duration_ms"] == 10.0  # noqa: S101
    assert fields["handler_duration_ms"] == 20.0  # noqa: S101
    assert fields["commit_duration_ms"] == 5.0  # noqa: S101
    assert fields["lag_seconds"] == 30.0  # noqa: S101
    assert fields["error_type"] == "ValueError"  # noqa: S101
    assert fields["result"] == "success"  # noqa: S101


def test_custom_collector_satisfies_protocol() -> None:
    assert isinstance(CustomCollector(), MetricsCollector)  # noqa: S101
