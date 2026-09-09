"""Async proxy wrappers that offload blocking DB calls to worker threads."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
import functools
import logging
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from ..binding.reader import DbReader
from ..binding.writer import DbWriter
from .out import DbOut

logger = logging.getLogger(__name__)

_ProxyTargetT = TypeVar("_ProxyTargetT")
_ProxyResultT = TypeVar("_ProxyResultT")


class _AsyncProxyBase(Generic[_ProxyTargetT]):
    """Base for async proxies that offload blocking calls to a worker thread.

    Subclasses declare explicit, fully-typed async methods and delegate their
    bodies to :meth:`_offload`.  This keeps each proxy's public surface and
    method signatures intact (unlike a generic ``__getattr__`` proxy, which
    would erase typing and leak withheld methods) while sharing the
    construction and ``asyncio.to_thread`` offload boilerplate.
    """

    def __init__(self, target: _ProxyTargetT) -> None:
        self._target = target

    @staticmethod
    async def _offload(
        func: Callable[..., _ProxyResultT], /, *args: Any, **kwargs: Any
    ) -> _ProxyResultT:
        return await asyncio.to_thread(func, *args, **kwargs)


class _AsyncDbOutProxy(_AsyncProxyBase[DbOut]):
    """Async wrapper for :class:`DbOut` used in async handlers.

    Delegates ``.set()`` to a worker thread via ``asyncio.to_thread()``
    so the event loop stays free.
    """

    async def set(
        self,
        data: (
            dict[str, object] | Sequence[dict[str, object]] | BaseModel | Sequence[BaseModel] | None
        ),
    ) -> None:
        """Async version of :meth:`DbOut.set`."""
        await self._offload(self._target.set, data)


class _AsyncDbReaderProxy(_AsyncProxyBase[DbReader]):
    async def get(self, *, pk: dict[str, object]) -> dict[str, object] | None:
        return await self._offload(self._target.get, pk=pk)

    async def query(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        return await self._offload(self._target.query, sql, params=params)

    async def scalar(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> object | None:
        return await self._offload(self._target.scalar, sql, params=params)

    async def one(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        return await self._offload(self._target.one, sql, params=params)

    async def one_or_none(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        return await self._offload(self._target.one_or_none, sql, params=params)

    def close(self) -> None:
        self._target.close()


class _AsyncDbWriterProxy(_AsyncProxyBase[DbWriter]):
    """Async wrapper around :class:`DbWriter` for async handlers.

    Each write call is offloaded to a worker thread via
    :func:`asyncio.to_thread` so the event loop is not blocked.

    For multi-statement atomicity, use :meth:`transaction` as an async
    context manager.  It pins the whole transaction to a single dedicated
    worker thread (SQLAlchemy ``Connection`` / ``Transaction`` objects are
    not safe to share across threads), committing on success and rolling
    back on error::

        async with writer.transaction() as tx:
            await tx.insert(data={...})
            await tx.update(data={...}, pk={...})
    """

    async def insert(self, *, data: dict[str, object]) -> None:
        await self._offload(self._target.insert, data=data)

    async def insert_many(self, *, rows: list[dict[str, object]]) -> None:
        await self._offload(self._target.insert_many, rows=rows)

    async def upsert(self, *, data: dict[str, object], conflict_columns: list[str]) -> None:
        await self._offload(self._target.upsert, data=data, conflict_columns=conflict_columns)

    async def upsert_many(
        self,
        *,
        rows: list[dict[str, object]],
        conflict_columns: list[str],
    ) -> None:
        await self._offload(self._target.upsert_many, rows=rows, conflict_columns=conflict_columns)

    def close(self) -> None:
        self._target.close()

    @asynccontextmanager
    async def transaction(self) -> "AsyncIterator[_AsyncTxWriterProxy]":
        """Group multiple async writes into a single SQL transaction.

        Because SQLAlchemy ``Connection`` / ``Transaction`` objects are not
        safe to share across threads, this context manager pins the entire
        transaction to a single dedicated worker thread for the duration of
        the ``async with`` block.  Every write issued through the yielded
        proxy is routed to that one thread, so the underlying connection is
        only ever touched by a single thread.  The transaction is committed
        on normal exit and rolled back if the block raises.

        Concurrent writes issued inside the block (e.g. via
        :func:`asyncio.gather`) are **serialized** onto the pinned thread;
        a SQLAlchemy connection cannot be used concurrently even on one
        thread.

        Example::

            async with writer.transaction() as tx:
                await tx.insert(data={"id": 1, "status": "pending"})
                await tx.update(data={"status": "shipped"}, pk={"id": 1})
        """
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="db-tx")
        cm = self._target.transaction()
        try:
            await loop.run_in_executor(executor, cm.__enter__)
        except BaseException:
            executor.shutdown(wait=False)
            raise

        try:
            try:
                yield _AsyncTxWriterProxy(self._target, loop, executor)
            except BaseException as exc:
                # Roll back on the pinned thread; preserve the original error.
                await self._async_exit(
                    loop, executor, cm, type(exc), exc, exc.__traceback__, swallow=True
                )
                raise
            else:
                # Commit on the pinned thread; let commit failures propagate.
                await self._async_exit(loop, executor, cm, None, None, None, swallow=False)
        finally:
            executor.shutdown(wait=True)

    @staticmethod
    async def _async_exit(
        loop: asyncio.AbstractEventLoop,
        executor: ThreadPoolExecutor,
        cm: Any,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
        *,
        swallow: bool,
    ) -> None:
        """Drive the sync transaction ``__exit__`` on the pinned thread.

        Shields the cleanup from cancellation so commit/rollback still runs.
        When *swallow* is true (rollback path) errors raised by ``__exit__``
        are logged and suppressed so the original exception is preserved;
        otherwise (commit path) they propagate.
        """
        call = functools.partial(cm.__exit__, exc_type, exc, tb)
        try:
            await asyncio.shield(loop.run_in_executor(executor, call))
        except asyncio.CancelledError:
            # Cleanup was cancelled: force synchronous completion on the
            # still-alive worker thread so the transaction is not left open.
            future = executor.submit(call)
            try:
                future.result(timeout=5.0)
            except Exception:
                logger.warning(
                    "async transaction cleanup failed after cancellation",
                    exc_info=True,
                )
            raise
        except BaseException:
            if not swallow:
                raise
            logger.warning(
                "async transaction rollback raised; original exception preserved",
                exc_info=True,
            )


class _AsyncTxWriterProxy:
    """Transactional async writer proxy bound to a single pinned thread.

    Yielded by :meth:`_AsyncDbWriterProxy.transaction`.  Every write is
    routed through *executor* (a single-worker thread pool) so the
    underlying SQLAlchemy connection is only ever used by one thread.
    """

    def __init__(
        self,
        writer: DbWriter,
        loop: asyncio.AbstractEventLoop,
        executor: ThreadPoolExecutor,
    ) -> None:
        self._writer = writer
        self._loop = loop
        self._executor = executor

    async def _run(self, func: Callable[..., Any], /, **kwargs: Any) -> None:
        await self._loop.run_in_executor(self._executor, functools.partial(func, **kwargs))

    async def insert(self, *, data: dict[str, object]) -> None:
        await self._run(self._writer.insert, data=data)

    async def insert_many(self, *, rows: list[dict[str, object]]) -> None:
        await self._run(self._writer.insert_many, rows=rows)

    async def upsert(self, *, data: dict[str, object], conflict_columns: list[str]) -> None:
        await self._run(self._writer.upsert, data=data, conflict_columns=conflict_columns)

    async def upsert_many(
        self, *, rows: list[dict[str, object]], conflict_columns: list[str]
    ) -> None:
        await self._run(self._writer.upsert_many, rows=rows, conflict_columns=conflict_columns)

    async def update(self, *, data: dict[str, object], pk: dict[str, object]) -> None:
        await self._run(self._writer.update, data=data, pk=pk)

    async def delete(self, *, pk: dict[str, object]) -> None:
        await self._run(self._writer.delete, pk=pk)


__all__ = [
    "_AsyncProxyBase",
    "_AsyncDbOutProxy",
    "_AsyncDbReaderProxy",
    "_AsyncDbWriterProxy",
    "_AsyncTxWriterProxy",
]
