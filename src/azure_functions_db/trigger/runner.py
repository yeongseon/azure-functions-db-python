from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
import inspect
import logging
from typing import Any, Protocol, runtime_checkable
import uuid

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
        self._handler_arity = _detect_handler_arity(handler)

    @property
    def name(self) -> str:
        return self._name

    def tick(self) -> int:
        invocation_id = uuid.uuid4().hex
        tick_started_at = datetime.now(UTC)

        try:
            lease_id = self._state_store.acquire_lease(
                self._name, self._lease_ttl_seconds
            )
        except Exception as exc:
            raise LeaseAcquireError(
                f"Failed to acquire lease for poller '{self._name}'"
            ) from exc

        total_processed = 0
        try:
            try:
                checkpoint = self._state_store.load_checkpoint(self._name)
            except Exception as exc:
                raise FetchError(
                    f"Failed to load checkpoint for poller '{self._name}'"
                ) from exc

            cursor = _extract_cursor(checkpoint)

            for batch_idx in range(self._max_batches_per_tick):
                batch_id = f"{invocation_id}-{batch_idx}"

                try:
                    raw_records = self._source.fetch(cursor, self._batch_size)
                except Exception as exc:
                    raise FetchError(
                        f"Failed to fetch from source for poller '{self._name}'"
                    ) from exc

                if not raw_records:
                    logger.debug(
                        "Poller '%s' batch %s: no records, stopping",
                        self._name,
                        batch_id,
                    )
                    break

                try:
                    descriptor = self._source.source_descriptor
                except Exception as exc:
                    raise SourceConfigurationError(
                        f"Failed to get source descriptor for poller '{self._name}'"
                    ) from exc

                try:
                    events = [
                        self._normalizer(record, descriptor)
                        for record in raw_records
                    ]
                except Exception as exc:
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
                    if self._handler_arity >= 2:  # noqa: PLR2004
                        self._handler(events, context)
                    else:
                        self._handler(events)
                except Exception as exc:
                    raise HandlerError(
                        f"Handler failed for poller '{self._name}' batch '{batch_id}'"
                    ) from exc

                try:
                    self._state_store.commit_checkpoint(
                        self._name, new_checkpoint, lease_id
                    )
                except LostLeaseError:
                    raise
                except Exception as exc:
                    raise CommitError(
                        f"Checkpoint commit failed for poller '{self._name}'"
                        f" batch '{batch_id}'"
                    ) from exc

                checkpoint = new_checkpoint
                cursor = last_event.cursor
                total_processed += len(events)

                logger.info(
                    "Poller '%s' batch %s: processed %d events",
                    self._name,
                    batch_id,
                    len(events),
                )

        finally:
            try:
                self._state_store.release_lease(self._name, lease_id)
            except Exception:
                logger.warning(
                    "Failed to release lease for poller '%s'",
                    self._name,
                    exc_info=True,
                )

        return total_processed
