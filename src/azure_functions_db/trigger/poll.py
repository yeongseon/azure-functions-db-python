from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from typing import Any

from azure_functions_db.observability import MetricsCollector
from azure_functions_db.trigger.errors import LeaseAcquireError
from azure_functions_db.trigger.normalizers import EventNormalizer, make_normalizer
from azure_functions_db.trigger.retry import RetryPolicy
from azure_functions_db.trigger.runner import PollRunner, SourceAdapter, StateStore

logger = logging.getLogger(__name__)


class PollTrigger:
    def __init__(
        self,
        *,
        name: str,
        source: SourceAdapter,
        checkpoint_store: StateStore,
        normalizer: EventNormalizer | None = None,
        batch_size: int = 100,
        max_batches_per_tick: int = 1,
        lease_ttl_seconds: int = 120,
        retry_policy: RetryPolicy | None = None,
        metrics: MetricsCollector | None = None,
    ) -> None:
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
        self._checkpoint_store = checkpoint_store
        self._batch_size = batch_size
        self._max_batches_per_tick = max_batches_per_tick
        self._lease_ttl_seconds = lease_ttl_seconds
        self._retry_policy = retry_policy
        self._metrics = metrics

        if normalizer is not None:
            self._normalizer = normalizer
        else:
            cursor_column = getattr(source, "cursor_column", None)
            pk_columns = getattr(source, "pk_columns", None)
            if isinstance(cursor_column, str) and isinstance(pk_columns, list):
                self._normalizer = make_normalizer(
                    cursor_column=cursor_column,
                    pk_columns=pk_columns,
                )
            else:
                msg = (
                    "Source does not expose 'cursor_column' and 'pk_columns' "
                    "attributes. Cannot auto-detect normalizer. Use a source that "
                    "exposes cursor metadata (e.g., SqlAlchemySource) or pass an "
                    "explicit normalizer."
                )
                raise ValueError(msg)

    @property
    def name(self) -> str:
        return self._name

    def run(self, *, timer: object, handler: Callable[..., Any]) -> int:
        del timer

        handler_call = getattr(handler, "__call__", None)
        if asyncio.iscoroutinefunction(handler) or asyncio.iscoroutinefunction(
            handler_call
        ):
            msg = "Async handlers are not supported. Pass a synchronous function instead."
            raise TypeError(msg)

        runner = PollRunner(
            name=self._name,
            source=self._source,
            state_store=self._checkpoint_store,
            normalizer=self._normalizer,
            handler=handler,
            batch_size=self._batch_size,
            max_batches_per_tick=self._max_batches_per_tick,
            lease_ttl_seconds=self._lease_ttl_seconds,
            retry_policy=self._retry_policy,
            metrics=self._metrics,
        )

        try:
            return runner.tick()
        except LeaseAcquireError:
            logger.debug(
                "Poller '%s' could not acquire lease, skipping tick",
                self._name,
            )
            return 0
