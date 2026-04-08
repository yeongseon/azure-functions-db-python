from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
import inspect
import logging
import time
from typing import Any, Protocol, runtime_checkable
import uuid

from azure_functions_db.core.types import CursorValue, SourceDescriptor
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
from azure_functions_db.trigger.retry import RetryPolicy

logger = logging.getLogger(__name__)

RawRecord = dict[str, object]
EventNormalizer = Callable[[RawRecord, SourceDescriptor], RowChange]


@runtime_checkable
class StateStore(Protocol):
    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str: ...
    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None: ...
    def release_lease(self, poller_name: str, lease_id: str) -> None: ...
    def load_checkpoint(self, poller_name: str) -> dict[str, object]: ...
    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None: ...


@runtime_checkable
class SourceAdapter(Protocol):
    @property
    def source_descriptor(self) -> SourceDescriptor: ...
    def fetch(
        self, cursor: CursorValue | None, batch_size: int
    ) -> Sequence[RawRecord]: ...


def _detect_handler_arity(handler: Callable[..., Any]) -> int:
    sig = inspect.signature(handler)
    params = [
        p
        for p in sig.parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
    ]
    return len(params)


def _extract_cursor(checkpoint: dict[str, object]) -> CursorValue | None:
    raw = checkpoint.get("cursor")
    if raw is None or isinstance(raw, (str, int, float, bool)):
        return raw
    if isinstance(raw, tuple):
        return raw
    if isinstance(raw, list):
        return tuple(raw)
    msg = f"Unsupported cursor type in checkpoint: {type(raw).__name__}"
    raise SerializationError(msg)


class PollRunner:
    def __init__(
        self,
        *,
        name: str,
        source: SourceAdapter,
        state_store: StateStore,
        normalizer: EventNormalizer,
        handler: Callable[..., Any],
        batch_size: int = 100,
        max_batches_per_tick: int = 1,
        lease_ttl_seconds: int = 120,
        retry_policy: RetryPolicy | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
        if asyncio.iscoroutinefunction(handler):
            msg = (
                "Async handlers are not supported. "
                "Pass a synchronous function instead."
            )
            raise TypeError(msg)
        if batch_size < 1:
            msg = "batch_size must be >= 1"
            raise ValueError(msg)
        if max_batches_per_tick < 1:
            msg = "max_batches_per_tick must be >= 1"
            raise ValueError(msg)
        if lease_ttl_seconds < 1:
            msg = "lease_ttl_seconds must be >= 1"
            raise ValueError(msg)

        self._name = name
        self._source = source
        self._state_store = state_store
        self._normalizer = normalizer
        self._handler = handler
        self._batch_size = batch_size
        self._max_batches_per_tick = max_batches_per_tick
        self._lease_ttl_seconds = lease_ttl_seconds
        self._retry_policy = retry_policy or RetryPolicy()
        self._metrics = metrics or NoOpCollector()
        self._handler_arity = _detect_handler_arity(handler)

    @property
    def name(self) -> str:
        return self._name

    def tick(self) -> int:
        invocation_id = uuid.uuid4().hex
        tick_started_at = datetime.now(UTC)
        tick_started_monotonic = time.monotonic()

        def emit_failure_metrics(exc: Exception) -> None:
            self._metrics.increment(
                METRIC_FAILURES_TOTAL,
                labels={"poller_name": self._name, "error_type": type(exc).__name__},
            )
            self._metrics.increment(
                METRIC_BATCHES_TOTAL,
                labels={"poller_name": self._name, "result": "failure"},
            )

        try:
            lease_id = self._state_store.acquire_lease(
                self._name, self._lease_ttl_seconds
            )
        except Exception as exc:
            logger.exception(
                "Failed to acquire lease for poller '%s'",
                self._name,
                extra=build_log_fields(
                    event="lease_acquire_failed",
                    poller_name=self._name,
                    invocation_id=invocation_id,
                    error_type=type(exc).__name__,
                    result="failure",
                ),
            )
            emit_failure_metrics(exc)
            raise LeaseAcquireError(
                f"Failed to acquire lease for poller '{self._name}'"
            ) from exc

        logger.debug(
            "Tick started for poller '%s'",
            self._name,
            extra=build_log_fields(
                event="tick_start",
                poller_name=self._name,
                invocation_id=invocation_id,
                lease_owner=lease_id,
            ),
        )

        total_processed = 0
        try:
            try:
                checkpoint = self._state_store.load_checkpoint(self._name)
            except Exception as exc:
                logger.exception(
                    "Failed to load checkpoint for poller '%s'",
                    self._name,
                    extra=build_log_fields(
                        event="checkpoint_load_failed",
                        poller_name=self._name,
                        invocation_id=invocation_id,
                        lease_owner=lease_id,
                        error_type=type(exc).__name__,
                        result="failure",
                    ),
                )
                emit_failure_metrics(exc)
                raise FetchError(
                    f"Failed to load checkpoint for poller '{self._name}'"
                ) from exc

            cursor = _extract_cursor(checkpoint)

            try:
                descriptor = self._source.source_descriptor
            except Exception as exc:
                logger.exception(
                    "Failed to get source descriptor for poller '%s'",
                    self._name,
                    extra=build_log_fields(
                        event="source_descriptor_failed",
                        poller_name=self._name,
                        invocation_id=invocation_id,
                        lease_owner=lease_id,
                        checkpoint_before=checkpoint,
                        error_type=type(exc).__name__,
                        result="failure",
                    ),
                )
                emit_failure_metrics(exc)
                raise SourceConfigurationError(
                    f"Failed to get source descriptor for poller '{self._name}'"
                ) from exc

            base_labels = {"poller_name": self._name, "source": descriptor.name}

            for batch_idx in range(self._max_batches_per_tick):
                batch_id = f"{invocation_id}-{batch_idx}"

                try:
                    fetch_started_monotonic = time.monotonic()
                    raw_records = self._source.fetch(cursor, self._batch_size)
                    fetch_duration_ms = round(
                        (time.monotonic() - fetch_started_monotonic) * 1000, 2
                    )
                except Exception as exc:
                    logger.exception(
                        "Failed to fetch from source for poller '%s'",
                        self._name,
                        extra=build_log_fields(
                            event="fetch_failed",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            batch_size=self._batch_size,
                            checkpoint_before=checkpoint,
                            lease_owner=lease_id,
                            error_type=type(exc).__name__,
                            result="failure",
                        ),
                    )
                    emit_failure_metrics(exc)
                    raise FetchError(
                        f"Failed to fetch from source for poller '{self._name}'"
                    ) from exc

                if not raw_records:
                    logger.debug(
                        "Poller '%s' batch %s: no records, stopping",
                        self._name,
                        batch_id,
                        extra=build_log_fields(
                            event="fetch_empty",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            fetched_count=0,
                        ),
                    )
                    break

                try:
                    events = [
                        self._normalizer(record, descriptor)
                        for record in raw_records
                    ]
                except Exception as exc:
                    logger.exception(
                        "Failed to normalize records for poller '%s'",
                        self._name,
                        extra=build_log_fields(
                            event="normalize_failed",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            fetched_count=len(raw_records),
                            batch_size=self._batch_size,
                            checkpoint_before=checkpoint,
                            lease_owner=lease_id,
                            fetch_duration_ms=fetch_duration_ms,
                            error_type=type(exc).__name__,
                            result="failure",
                        ),
                    )
                    emit_failure_metrics(exc)
                    raise SerializationError(
                        f"Failed to normalize records for poller '{self._name}'"
                    ) from exc

                last_event = events[-1]
                new_checkpoint: dict[str, object] = {
                    "cursor": last_event.cursor,
                    "batch_id": batch_id,
                }

                context = PollContext(
                    poller_name=self._name,
                    invocation_id=invocation_id,
                    batch_id=batch_id,
                    lease_owner=lease_id,
                    checkpoint_before=checkpoint,
                    checkpoint_after_candidate=new_checkpoint,
                    tick_started_at=tick_started_at,
                    source_name=descriptor.name,
                )

                try:
                    handler_started_monotonic = time.monotonic()
                    if self._handler_arity >= 2:  # noqa: PLR2004
                        self._handler(events, context)
                    else:
                        self._handler(events)
                    handler_duration_ms = round(
                        (time.monotonic() - handler_started_monotonic) * 1000, 2
                    )
                except Exception as exc:
                    logger.exception(
                        "Handler failed for poller '%s' batch '%s'",
                        self._name,
                        batch_id,
                        extra=build_log_fields(
                            event="handler_failed",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            fetched_count=len(events),
                            batch_size=self._batch_size,
                            committed=False,
                            checkpoint_before=checkpoint,
                            checkpoint_after=new_checkpoint,
                            lease_owner=lease_id,
                            fetch_duration_ms=fetch_duration_ms,
                            error_type=type(exc).__name__,
                            result="failure",
                        ),
                    )
                    emit_failure_metrics(exc)
                    raise HandlerError(
                        f"Handler failed for poller '{self._name}' batch '{batch_id}'"
                    ) from exc

                try:
                    commit_started_monotonic = time.monotonic()
                    self._state_store.commit_checkpoint(
                        self._name, new_checkpoint, lease_id
                    )
                    commit_duration_ms = round(
                        (time.monotonic() - commit_started_monotonic) * 1000, 2
                    )
                except LostLeaseError:
                    logger.exception(
                        "Checkpoint commit failed for poller '%s' batch '%s'",
                        self._name,
                        batch_id,
                        extra=build_log_fields(
                            event="commit_failed",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            fetched_count=len(events),
                            batch_size=self._batch_size,
                            committed=False,
                            checkpoint_before=checkpoint,
                            checkpoint_after=new_checkpoint,
                            lease_owner=lease_id,
                            fetch_duration_ms=fetch_duration_ms,
                            handler_duration_ms=handler_duration_ms,
                            error_type="LostLeaseError",
                            result="failure",
                        ),
                    )
                    emit_failure_metrics(LostLeaseError("Lost lease during commit"))
                    raise
                except Exception as exc:
                    logger.exception(
                        "Checkpoint commit failed for poller '%s' batch '%s'",
                        self._name,
                        batch_id,
                        extra=build_log_fields(
                            event="commit_failed",
                            poller_name=self._name,
                            invocation_id=invocation_id,
                            batch_id=batch_id,
                            source=descriptor.name,
                            fetched_count=len(events),
                            batch_size=self._batch_size,
                            committed=False,
                            checkpoint_before=checkpoint,
                            checkpoint_after=new_checkpoint,
                            lease_owner=lease_id,
                            fetch_duration_ms=fetch_duration_ms,
                            handler_duration_ms=handler_duration_ms,
                            error_type=type(exc).__name__,
                            result="failure",
                        ),
                    )
                    emit_failure_metrics(exc)
                    raise CommitError(
                        f"Checkpoint commit failed for poller '{self._name}'"
                        f" batch '{batch_id}'"
                    ) from exc

                self._metrics.increment(
                    METRIC_BATCHES_TOTAL,
                    labels={**base_labels, "result": "success"},
                )
                self._metrics.increment(
                    METRIC_EVENTS_TOTAL,
                    value=float(len(events)),
                    labels=base_labels,
                )
                self._metrics.observe(
                    METRIC_BATCH_SIZE,
                    float(len(events)),
                    labels=base_labels,
                )
                self._metrics.observe(
                    METRIC_FETCH_DURATION_MS,
                    fetch_duration_ms,
                    labels=base_labels,
                )
                self._metrics.observe(
                    METRIC_HANDLER_DURATION_MS,
                    handler_duration_ms,
                    labels=base_labels,
                )
                self._metrics.observe(
                    METRIC_COMMIT_DURATION_MS,
                    commit_duration_ms,
                    labels=base_labels,
                )

                lag_seconds: float | None = None
                cursor_for_lag = new_checkpoint.get("cursor")
                if isinstance(cursor_for_lag, str):
                    try:
                        cursor_dt = datetime.fromisoformat(cursor_for_lag)
                        if cursor_dt.tzinfo is not None:
                            lag_seconds = (
                                datetime.now(UTC) - cursor_dt
                            ).total_seconds()
                            self._metrics.set_gauge(
                                METRIC_LAG_SECONDS,
                                lag_seconds,
                                labels=base_labels,
                            )
                    except (ValueError, TypeError):
                        pass
                elif isinstance(cursor_for_lag, tuple) and cursor_for_lag:
                    first_part = cursor_for_lag[0]
                    if isinstance(first_part, str):
                        try:
                            cursor_dt = datetime.fromisoformat(first_part)
                            if cursor_dt.tzinfo is not None:
                                lag_seconds = (
                                    datetime.now(UTC) - cursor_dt
                                ).total_seconds()
                                self._metrics.set_gauge(
                                    METRIC_LAG_SECONDS,
                                    lag_seconds,
                                    labels=base_labels,
                                )
                        except (ValueError, TypeError):
                            pass

                checkpoint = new_checkpoint
                cursor = last_event.cursor
                total_processed += len(events)

                logger.info(
                    "Poller '%s' batch %s: processed %d events",
                    self._name,
                    batch_id,
                    len(events),
                    extra=build_log_fields(
                        event="batch_complete",
                        poller_name=self._name,
                        invocation_id=invocation_id,
                        batch_id=batch_id,
                        source=descriptor.name,
                        fetched_count=len(events),
                        committed=True,
                        checkpoint_before=checkpoint,
                        checkpoint_after=new_checkpoint,
                        lease_owner=lease_id,
                        fetch_duration_ms=fetch_duration_ms,
                        handler_duration_ms=handler_duration_ms,
                        commit_duration_ms=commit_duration_ms,
                        lag_seconds=lag_seconds,
                        result="success",
                    ),
                )

            tick_duration_ms = round(
                (time.monotonic() - tick_started_monotonic) * 1000, 2
            )
            self._metrics.set_gauge(
                METRIC_LAST_SUCCESS_TIMESTAMP,
                datetime.now(UTC).timestamp(),
                labels={"poller_name": self._name},
            )
            logger.info(
                "Tick completed for poller '%s': %d events total",
                self._name,
                total_processed,
                extra=build_log_fields(
                    event="tick_complete",
                    poller_name=self._name,
                    invocation_id=invocation_id,
                    duration_ms=tick_duration_ms,
                    result="success",
                ),
            )

        finally:
            try:
                self._state_store.release_lease(self._name, lease_id)
            except Exception as exc:
                logger.warning(
                    "Failed to release lease for poller '%s'",
                    self._name,
                    exc_info=True,
                    extra=build_log_fields(
                        event="lease_release_failed",
                        poller_name=self._name,
                        invocation_id=invocation_id,
                        lease_owner=lease_id,
                        error_type=type(exc).__name__,
                        result="failure",
                    ),
                )

        return total_processed
