from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)

METRIC_BATCHES_TOTAL = "azfdb_batches_total"
METRIC_EVENTS_TOTAL = "azfdb_events_total"
METRIC_FAILURES_TOTAL = "azfdb_failures_total"
METRIC_FETCH_DURATION_MS = "azfdb_fetch_duration_ms"
METRIC_HANDLER_DURATION_MS = "azfdb_handler_duration_ms"
METRIC_COMMIT_DURATION_MS = "azfdb_commit_duration_ms"
METRIC_BATCH_SIZE = "azfdb_batch_size"
METRIC_LAG_SECONDS = "azfdb_lag_seconds"
METRIC_LAST_SUCCESS_TIMESTAMP = "azfdb_last_success_timestamp"


@runtime_checkable
class MetricsCollector(Protocol):
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None: ...

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None: ...

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None: ...


class NoOpCollector:
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


def build_log_fields(
    *,
    event: str,
    poller_name: str,
    invocation_id: str | None = None,
    batch_id: str | None = None,
    source: str | None = None,
    schedule_time: str | None = None,
    fetched_count: int | None = None,
    batch_size: int | None = None,
    committed: bool | None = None,
    checkpoint_before: object = None,
    checkpoint_after: object = None,
    lease_owner: str | None = None,
    duration_ms: float | None = None,
    fetch_duration_ms: float | None = None,
    handler_duration_ms: float | None = None,
    commit_duration_ms: float | None = None,
    lag_seconds: float | None = None,
    error_type: str | None = None,
    result: str | None = None,
) -> dict[str, object]:
    return {
        "event": event,
        "poller_name": poller_name,
        "invocation_id": invocation_id,
        "batch_id": batch_id,
        "source": source,
        "schedule_time": schedule_time,
        "fetched_count": fetched_count,
        "batch_size": batch_size,
        "committed": committed,
        "checkpoint_before": checkpoint_before,
        "checkpoint_after": checkpoint_after,
        "lease_owner": lease_owner,
        "duration_ms": duration_ms,
        "fetch_duration_ms": fetch_duration_ms,
        "handler_duration_ms": handler_duration_ms,
        "commit_duration_ms": commit_duration_ms,
        "lag_seconds": lag_seconds,
        "error_type": error_type,
        "result": result,
    }
