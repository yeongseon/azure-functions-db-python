from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
import inspect
import logging
import time
from typing import Any, Protocol, TypeVar, runtime_checkable
import uuid

from ..core.errors import CursorSerializationError
from ..core.serializers import parse_checkpoint_cursor
from ..core.types import CursorValue, RawRecord, SourceDescriptor
from ..observability import (
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
from ..state.errors import LeaseConflictError
from .context import PollContext
from .errors import (
    CommitError,
    FetchError,
    HandlerError,
    LeaseAcquireError,
    LostLeaseError,
    SerializationError,
    SourceConfigurationError,
)
from .events import RowChange
from .retry import RetryPolicy

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

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
    try:
        return parse_checkpoint_cursor(checkpoint.get("cursor"))
    except CursorSerializationError as exc:
        from .errors import SerializationError

        raise SerializationError(str(exc)) from exc


@dataclass
class _TickState:
    """Mutable accumulator threaded through the stages of a single tick."""

    invocation_id: str
    tick_started_at: datetime
    tick_started_monotonic: float
    lease_id: str = ""
    checkpoint: dict[str, object] = field(default_factory=dict)
    cursor: CursorValue | None = None
    descriptor: SourceDescriptor | None = None
    base_labels: dict[str, str] = field(default_factory=dict)
    total_processed: int = 0
    batch_idx: int = 0
    batch_id: str = ""
    fetch_duration_ms: float = 0.0
    handler_duration_ms: float = 0.0
    commit_duration_ms: float = 0.0


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
        self._retry_policy = retry_policy
        self._sleep: Callable[[float], None] = time.sleep
        self._metrics = metrics or NoOpCollector()
        self._handler_arity = _detect_handler_arity(handler)

    def _safe_emit(self, fn: Callable[[], None]) -> None:
        try:
            fn()
        except Exception:
            logger.debug(
                "Metrics emission failed for poller '%s'",
                self._name,
                exc_info=True,
            )

    @property
    def name(self) -> str:
        return self._name

    def _invoke_handler_with_retry(
        self,
        events: list[RowChange],
        context: PollContext,
        *,
        batch_id: str,
        invocation_id: str,
        source_name: str,
    ) -> None:
        """Invoke the handler, applying the retry policy on failure.

        When no ``RetryPolicy`` is configured, the handler is invoked exactly
        once and any exception propagates to the caller (preserving the
        historical single-attempt behavior). When a policy is configured,
        transient exceptions trigger up to ``max_retries`` additional attempts
        with exponential backoff sourced from :meth:`RetryPolicy.delay_for_attempt`.
        The final exception is re-raised once attempts are exhausted so the
        outer batch loop can emit failure metrics and raise ``HandlerError``.
        """
        policy = self._retry_policy
        max_attempts = 1 if policy is None else policy.max_retries + 1
        for attempt in range(max_attempts):
            try:
                if self._handler_arity >= 2:  # noqa: PLR2004
                    self._handler(events, context)
                else:
                    self._handler(events)
                return
            except Exception as exc:
                remaining = max_attempts - attempt - 1
                if remaining <= 0 or policy is None:
                    raise
                delay = policy.delay_for_attempt(attempt)
                logger.warning(
                    "Handler attempt %d/%d failed for poller '%s' batch '%s':"
                    " %s; retrying in %.2fs",
                    attempt + 1,
                    max_attempts,
                    self._name,
                    batch_id,
                    type(exc).__name__,
                    delay,
                    extra=build_log_fields(
                        event="handler_retry",
                        poller_name=self._name,
                        invocation_id=invocation_id,
                        batch_id=batch_id,
                        source=source_name,
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        delay_seconds=delay,
                        error_type=type(exc).__name__,
                        result="retry",
                    ),
                )
                self._sleep(delay)

    def _emit_failure_metrics(
        self, exc: Exception, *, source: str | None = None
    ) -> None:
        labels: dict[str, str] = {
            "poller_name": self._name,
            "error_type": type(exc).__name__,
        }
        if source is not None:
            labels["source"] = source
        self._safe_emit(
            lambda: self._metrics.increment(
                METRIC_FAILURES_TOTAL,
                labels=labels,
            )
        )
        failure_labels: dict[str, str] = {
            "poller_name": self._name,
            "result": "failure",
        }
        if source is not None:
            failure_labels["source"] = source
        self._safe_emit(
            lambda: self._metrics.increment(
                METRIC_BATCHES_TOTAL,
                labels=failure_labels,
            )
        )

    def _emit_lag_metric(self, value: float, base_labels: dict[str, str]) -> None:
        self._safe_emit(
            lambda: self._metrics.set_gauge(
                METRIC_LAG_SECONDS,
                value,
                labels=base_labels,
            )
        )

    def _run_stage(
        self,
        fn: Callable[[], _T],
        *,
        error_cls: type[Exception],
        message: str,
        event: str,
        invocation_id: str,
        extra_fields: dict[str, object],
        source: str | None = None,
    ) -> _T:
        """Run a tick stage with uniform failure logging/metrics/raising.

        The interpolated ``message`` is reused verbatim for both the
        ``logger.exception`` record and the raised ``error_cls`` so callers
        keep a single source of truth per stage.
        """
        try:
            return fn()
        except Exception as exc:
            extra = build_log_fields(
                event=event,
                poller_name=self._name,
                invocation_id=invocation_id,
                error_type=type(exc).__name__,
                result="failure",
            )
            extra.update(extra_fields)
            logger.exception(
                message,
                extra=extra,
            )
            self._emit_failure_metrics(exc, source=source)
            raise error_cls(message) from exc

    def _acquire_lease(self, ctx: _TickState) -> str | None:
        try:
            return self._state_store.acquire_lease(
                self._name, self._lease_ttl_seconds
            )
        except LeaseConflictError:
            # Scale-out contention: another instance holds the lease this
            # cycle. Treating it as a failure (logger.exception /
            # failures_total / LeaseAcquireError) would generate alert noise,
            # so skip the tick silently.
            logger.debug(
                "Poller '%s': lease already held by another instance; skipping tick",
                self._name,
                extra=build_log_fields(
                    event="lease_acquire_skipped",
                    poller_name=self._name,
                    invocation_id=ctx.invocation_id,
                    result="skipped",
                ),
            )
            return None
        except Exception as exc:
            logger.exception(
                "Failed to acquire lease for poller '%s'",
                self._name,
                extra=build_log_fields(
                    event="lease_acquire_failed",
                    poller_name=self._name,
                    invocation_id=ctx.invocation_id,
                    error_type=type(exc).__name__,
                    result="failure",
                ),
            )
            self._emit_failure_metrics(exc)
            raise LeaseAcquireError(
                f"Failed to acquire lease for poller '{self._name}'"
            ) from exc

    def _load_checkpoint(self, ctx: _TickState) -> dict[str, object]:
        return self._run_stage(
            lambda: self._state_store.load_checkpoint(self._name),
            error_cls=FetchError,
            message=f"Failed to load checkpoint for poller '{self._name}'",
            event="checkpoint_load_failed",
            invocation_id=ctx.invocation_id,
            extra_fields={"lease_owner": ctx.lease_id},
        )

    def _resolve_source_descriptor(self, ctx: _TickState) -> SourceDescriptor:
        return self._run_stage(
            lambda: self._source.source_descriptor,
            error_cls=SourceConfigurationError,
            message=f"Failed to get source descriptor for poller '{self._name}'",
            event="source_descriptor_failed",
            invocation_id=ctx.invocation_id,
            extra_fields={
                "lease_owner": ctx.lease_id,
                "checkpoint_before": ctx.checkpoint,
            },
        )

    def _fetch_batch(self, ctx: _TickState) -> Sequence[RawRecord] | None:
        assert ctx.descriptor is not None

        def _fetch() -> Sequence[RawRecord]:
            fetch_started_monotonic = time.monotonic()
            raw_records = self._source.fetch(ctx.cursor, self._batch_size)
            ctx.fetch_duration_ms = round(
                (time.monotonic() - fetch_started_monotonic) * 1000, 2
            )
            return raw_records

        raw_records = self._run_stage(
            _fetch,
            error_cls=FetchError,
            message=f"Failed to fetch from source for poller '{self._name}'",
            event="fetch_failed",
            invocation_id=ctx.invocation_id,
            extra_fields={
                "batch_id": ctx.batch_id,
                "source": ctx.descriptor.name,
                "batch_size": self._batch_size,
                "checkpoint_before": ctx.checkpoint,
                "lease_owner": ctx.lease_id,
            },
            source=ctx.descriptor.name,
        )
        if not raw_records:
            logger.debug(
                "Poller '%s' batch %s: no records, stopping",
                self._name,
                ctx.batch_id,
                extra=build_log_fields(
                    event="fetch_empty",
                    poller_name=self._name,
                    invocation_id=ctx.invocation_id,
                    batch_id=ctx.batch_id,
                    source=ctx.descriptor.name,
                    fetched_count=0,
                ),
            )
            return None
        return raw_records

    def _normalize_batch(
        self, ctx: _TickState, raw_records: Sequence[RawRecord]
    ) -> list[RowChange]:
        descriptor = ctx.descriptor
        assert descriptor is not None
        return self._run_stage(
            lambda: [
                self._normalizer(record, descriptor) for record in raw_records
            ],
            error_cls=SerializationError,
            message=f"Failed to normalize records for poller '{self._name}'",
            event="normalize_failed",
            invocation_id=ctx.invocation_id,
            extra_fields={
                "batch_id": ctx.batch_id,
                "source": descriptor.name,
                "fetched_count": len(raw_records),
                "batch_size": self._batch_size,
                "checkpoint_before": ctx.checkpoint,
                "lease_owner": ctx.lease_id,
                "fetch_duration_ms": ctx.fetch_duration_ms,
            },
            source=descriptor.name,
        )

    def _invoke_handler_stage(
        self,
        ctx: _TickState,
        events: list[RowChange],
        context: PollContext,
        new_checkpoint: dict[str, object],
    ) -> None:
        descriptor = ctx.descriptor
        assert descriptor is not None

        def _invoke() -> None:
            handler_started_monotonic = time.monotonic()
            self._invoke_handler_with_retry(
                events,
                context,
                batch_id=ctx.batch_id,
                invocation_id=ctx.invocation_id,
                source_name=descriptor.name,
            )
            ctx.handler_duration_ms = round(
                (time.monotonic() - handler_started_monotonic) * 1000, 2
            )

        self._run_stage(
            _invoke,
            error_cls=HandlerError,
            message=(
                f"Handler failed for poller '{self._name}' batch '{ctx.batch_id}'"
            ),
            event="handler_failed",
            invocation_id=ctx.invocation_id,
            extra_fields={
                "batch_id": ctx.batch_id,
                "source": descriptor.name,
                "fetched_count": len(events),
                "batch_size": self._batch_size,
                "committed": False,
                "checkpoint_before": ctx.checkpoint,
                "checkpoint_after": new_checkpoint,
                "lease_owner": ctx.lease_id,
                "fetch_duration_ms": ctx.fetch_duration_ms,
            },
            source=descriptor.name,
        )

    def _commit_log_extra(
        self,
        ctx: _TickState,
        events: list[RowChange],
        new_checkpoint: dict[str, object],
        error_type: str,
    ) -> dict[str, object]:
        assert ctx.descriptor is not None
        return build_log_fields(
            event="commit_failed",
            poller_name=self._name,
            invocation_id=ctx.invocation_id,
            batch_id=ctx.batch_id,
            source=ctx.descriptor.name,
            fetched_count=len(events),
            batch_size=self._batch_size,
            committed=False,
            checkpoint_before=ctx.checkpoint,
            checkpoint_after=new_checkpoint,
            lease_owner=ctx.lease_id,
            fetch_duration_ms=ctx.fetch_duration_ms,
            handler_duration_ms=ctx.handler_duration_ms,
            error_type=error_type,
            result="failure",
        )

    def _commit_checkpoint(
        self,
        ctx: _TickState,
        events: list[RowChange],
        new_checkpoint: dict[str, object],
    ) -> None:
        assert ctx.descriptor is not None
        try:
            commit_started_monotonic = time.monotonic()
            self._state_store.commit_checkpoint(
                self._name, new_checkpoint, ctx.lease_id
            )
            ctx.commit_duration_ms = round(
                (time.monotonic() - commit_started_monotonic) * 1000, 2
            )
        except LostLeaseError:
            logger.exception(
                "Checkpoint commit failed for poller '%s' batch '%s'",
                self._name,
                ctx.batch_id,
                extra=self._commit_log_extra(
                    ctx, events, new_checkpoint, "LostLeaseError"
                ),
            )
            self._emit_failure_metrics(
                LostLeaseError("Lost lease during commit"),
                source=ctx.descriptor.name,
            )
            raise
        except Exception as exc:
            logger.exception(
                "Checkpoint commit failed for poller '%s' batch '%s'",
                self._name,
                ctx.batch_id,
                extra=self._commit_log_extra(
                    ctx, events, new_checkpoint, type(exc).__name__
                ),
            )
            self._emit_failure_metrics(exc, source=ctx.descriptor.name)
            raise CommitError(
                f"Checkpoint commit failed for poller '{self._name}'"
                f" batch '{ctx.batch_id}'"
            ) from exc

    def _emit_success_metrics(
        self, ctx: _TickState, events: list[RowChange]
    ) -> None:
        base_labels = ctx.base_labels
        self._safe_emit(
            lambda: self._metrics.increment(
                METRIC_BATCHES_TOTAL,
                labels={**base_labels, "result": "success"},
            )
        )
        self._safe_emit(
            lambda: self._metrics.increment(
                METRIC_EVENTS_TOTAL,
                value=float(len(events)),
                labels=base_labels,
            )
        )
        self._safe_emit(
            lambda: self._metrics.observe(
                METRIC_BATCH_SIZE,
                float(len(events)),
                labels=base_labels,
            )
        )
        self._safe_emit(
            lambda: self._metrics.observe(
                METRIC_FETCH_DURATION_MS,
                ctx.fetch_duration_ms,
                labels=base_labels,
            )
        )
        self._safe_emit(
            lambda: self._metrics.observe(
                METRIC_HANDLER_DURATION_MS,
                ctx.handler_duration_ms,
                labels=base_labels,
            )
        )
        self._safe_emit(
            lambda: self._metrics.observe(
                METRIC_COMMIT_DURATION_MS,
                ctx.commit_duration_ms,
                labels=base_labels,
            )
        )

    def _compute_lag(
        self, ctx: _TickState, new_checkpoint: dict[str, object]
    ) -> float | None:
        lag_seconds: float | None = None
        cursor_for_lag = new_checkpoint.get("cursor")
        if isinstance(cursor_for_lag, str):
            try:
                cursor_dt = datetime.fromisoformat(cursor_for_lag)
                if cursor_dt.tzinfo is not None:
                    lag_seconds = max(
                        0.0,
                        (datetime.now(timezone.utc) - cursor_dt).total_seconds(),
                    )
                    self._emit_lag_metric(lag_seconds, ctx.base_labels)
                else:
                    logger.debug(
                        "Poller '%s': cursor datetime '%s' is tz-naive;"
                        " lag metric skipped. Use a tz-aware ISO timestamp"
                        " (e.g. '\u2026+00:00') to enable lag tracking.",
                        self._name,
                        cursor_for_lag,
                    )
            except (ValueError, TypeError):
                pass
        elif isinstance(cursor_for_lag, tuple) and cursor_for_lag:
            first_part = cursor_for_lag[0]
            if isinstance(first_part, str):
                try:
                    cursor_dt = datetime.fromisoformat(first_part)
                    if cursor_dt.tzinfo is not None:
                        lag_seconds = max(
                            0.0,
                            (datetime.now(timezone.utc) - cursor_dt).total_seconds(),
                        )
                        self._emit_lag_metric(lag_seconds, ctx.base_labels)
                    else:
                        logger.debug(
                            "Poller '%s': cursor tuple[0] '%s' is"
                            " tz-naive; lag metric skipped.",
                            self._name,
                            first_part,
                        )
                except (ValueError, TypeError):
                    pass
        return lag_seconds

    def _process_batch(self, ctx: _TickState) -> bool:
        """Process a single batch. Returns ``False`` to stop the tick loop."""
        assert ctx.descriptor is not None
        ctx.batch_id = f"{ctx.invocation_id}-{ctx.batch_idx}"

        raw_records = self._fetch_batch(ctx)
        if raw_records is None:
            return False

        events = self._normalize_batch(ctx, raw_records)

        last_event = events[-1]
        new_checkpoint: dict[str, object] = {
            "cursor": last_event.cursor,
            "batch_id": ctx.batch_id,
        }

        context = PollContext(
            poller_name=self._name,
            invocation_id=ctx.invocation_id,
            batch_id=ctx.batch_id,
            lease_owner=ctx.lease_id,
            checkpoint_before=ctx.checkpoint,
            checkpoint_after_candidate=new_checkpoint,
            tick_started_at=ctx.tick_started_at,
            source_name=ctx.descriptor.name,
        )

        self._invoke_handler_stage(ctx, events, context, new_checkpoint)
        self._commit_checkpoint(ctx, events, new_checkpoint)
        self._emit_success_metrics(ctx, events)
        lag_seconds = self._compute_lag(ctx, new_checkpoint)

        old_checkpoint = ctx.checkpoint
        ctx.checkpoint = new_checkpoint
        ctx.cursor = last_event.cursor
        ctx.total_processed += len(events)

        logger.info(
            "Poller '%s' batch %s: processed %d events",
            self._name,
            ctx.batch_id,
            len(events),
            extra=build_log_fields(
                event="batch_complete",
                poller_name=self._name,
                invocation_id=ctx.invocation_id,
                batch_id=ctx.batch_id,
                source=ctx.descriptor.name,
                fetched_count=len(events),
                committed=True,
                checkpoint_before=old_checkpoint,
                checkpoint_after=new_checkpoint,
                lease_owner=ctx.lease_id,
                fetch_duration_ms=ctx.fetch_duration_ms,
                handler_duration_ms=ctx.handler_duration_ms,
                commit_duration_ms=ctx.commit_duration_ms,
                lag_seconds=lag_seconds,
                result="success",
            ),
        )
        return True

    def tick(self) -> int:
        ctx = _TickState(
            invocation_id=uuid.uuid4().hex,
            tick_started_at=datetime.now(timezone.utc),
            tick_started_monotonic=time.monotonic(),
        )

        lease_id = self._acquire_lease(ctx)
        if lease_id is None:
            return 0
        ctx.lease_id = lease_id

        logger.debug(
            "Tick started for poller '%s'",
            self._name,
            extra=build_log_fields(
                event="tick_start",
                poller_name=self._name,
                invocation_id=ctx.invocation_id,
                lease_owner=lease_id,
            ),
        )

        try:
            ctx.checkpoint = self._load_checkpoint(ctx)
            ctx.cursor = _extract_cursor(ctx.checkpoint)
            ctx.descriptor = self._resolve_source_descriptor(ctx)
            ctx.base_labels = {
                "poller_name": self._name,
                "source": ctx.descriptor.name,
            }

            for batch_idx in range(self._max_batches_per_tick):
                ctx.batch_idx = batch_idx
                if not self._process_batch(ctx):
                    break

            tick_duration_ms = round(
                (time.monotonic() - ctx.tick_started_monotonic) * 1000, 2
            )
            self._safe_emit(
                lambda: self._metrics.set_gauge(
                    METRIC_LAST_SUCCESS_TIMESTAMP,
                    datetime.now(timezone.utc).timestamp(),
                    labels={"poller_name": self._name},
                )
            )
            logger.info(
                "Tick completed for poller '%s': %d events total",
                self._name,
                ctx.total_processed,
                extra=build_log_fields(
                    event="tick_complete",
                    poller_name=self._name,
                    invocation_id=ctx.invocation_id,
                    duration_ms=tick_duration_ms,
                    result="success",
                ),
            )

        finally:
            try:
                self._state_store.release_lease(self._name, ctx.lease_id)
            except Exception as exc:
                logger.warning(
                    "Failed to release lease for poller '%s'",
                    self._name,
                    exc_info=True,
                    extra=build_log_fields(
                        event="lease_release_failed",
                        poller_name=self._name,
                        invocation_id=ctx.invocation_id,
                        lease_owner=ctx.lease_id,
                        error_type=type(exc).__name__,
                        result="failure",
                    ),
                )

        return ctx.total_processed
