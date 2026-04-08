from __future__ import annotations

import asyncio
from collections.abc import Callable
import functools
import inspect
import logging
from typing import Any

from .binding.reader import DbReader
from .binding.writer import DbWriter
from .core.engine import EngineProvider
from .core.errors import ConfigurationError
from .observability import MetricsCollector
from .trigger.normalizers import EventNormalizer
from .trigger.poll import PollTrigger
from .trigger.retry import RetryPolicy
from .trigger.runner import SourceAdapter, StateStore

logger = logging.getLogger(__name__)

# Parameter names reserved by Azure Functions runtime.
_RESERVED_ARGS = frozenset({"timer", "req", "context", "msg", "input", "output"})


def _validate_arg_name(arg_name: str, fn: Callable[..., Any], decorator_name: str) -> None:
    """Validate that *arg_name* exists in *fn*'s signature and does not collide."""
    sig = inspect.signature(fn, follow_wrapped=False)
    if arg_name not in sig.parameters:
        msg = (
            f"{decorator_name} arg_name='{arg_name}' not found in "
            f"function '{fn.__name__}' parameters"
        )
        raise ConfigurationError(msg)

    if arg_name in _RESERVED_ARGS:
        msg = (
            f"{decorator_name} arg_name='{arg_name}' conflicts with Azure Functions "
            f"reserved parameter name. Avoid: {sorted(_RESERVED_ARGS)}"
        )
        raise ConfigurationError(msg)


def _build_host_signature(
    fn: Callable[..., Any],
    injected: set[str],
) -> inspect.Signature:
    """Return a signature hiding *injected* params from Azure runtime."""
    sig = inspect.signature(fn, follow_wrapped=False)
    params = [p for name, p in sig.parameters.items() if name not in injected]
    return sig.replace(parameters=params)


class DbFunctionApp:
    """Azure Functions-style decorator API for database integration.

    Provides ``db_trigger``, ``db_input``, and ``db_output`` decorator
    methods that wrap the imperative API (PollTrigger, DbReader, DbWriter)
    in an Azure Functions-native decorator experience.

    Decorator order contract:
        Azure decorators outermost, db decorators closest to the function::

            @app.function_name(name="my_func")
            @app.schedule(...)          # Azure trigger (outermost)
            @db.db_trigger(...)         # db decorator (closest to fn)
            def my_func(events): ...

    Note: This is a pseudo-trigger implementation. ``db_trigger`` requires
    an actual Azure Functions trigger (e.g. ``@app.schedule``) to fire.
    It does not register a native Azure Functions binding.
    """

    def db_trigger(
        self,
        *,
        arg_name: str,
        source: SourceAdapter,
        checkpoint_store: StateStore,
        name: str | None = None,
        normalizer: EventNormalizer | None = None,
        batch_size: int = 100,
        max_batches_per_tick: int = 1,
        lease_ttl_seconds: int = 120,
        retry_policy: RetryPolicy | None = None,
        metrics: MetricsCollector | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator for database change detection (pseudo-trigger).

        Wraps a handler function so that on each invocation it polls the
        database source for new/changed rows and passes them to the handler.

        The decorated function's ``arg_name`` parameter will receive the
        list of :class:`RowChange` events.  An optional parameter named
        ``context`` will receive the :class:`PollContext`.

        Must be used together with an actual Azure Functions trigger
        (e.g. ``@app.schedule(...)``).

        Parameters
        ----------
        arg_name:
            Name of the handler parameter that receives the events list.
        source:
            Database source adapter (e.g. ``SqlAlchemySource``).
        checkpoint_store:
            State store for checkpointing (e.g. ``BlobCheckpointStore``).
        name:
            Trigger name for logging/metrics.  Defaults to the function name.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _validate_arg_name(arg_name, fn, "db_trigger")

            # Reject async handlers: PollTrigger.run is synchronous and
            # calling an async function without await would silently return
            # an unawaited coroutine.
            if asyncio.iscoroutinefunction(fn):
                msg = "db_trigger does not support async handlers. Use a sync handler instead."
                raise ConfigurationError(msg)

            trigger_name = name or fn.__name__
            trigger = PollTrigger(
                name=trigger_name,
                source=source,
                checkpoint_store=checkpoint_store,
                normalizer=normalizer,
                batch_size=batch_size,
                max_batches_per_tick=max_batches_per_tick,
                lease_ttl_seconds=lease_ttl_seconds,
                retry_policy=retry_policy,
                metrics=metrics,
            )

            fn_sig = inspect.signature(fn, follow_wrapped=False)
            has_context = "context" in fn_sig.parameters

            # Identify which params are injected by db decorators vs host
            # trigger params that must be forwarded to inner wrappers.
            db_injected = {arg_name}
            if has_context:
                db_injected.add("context")
            host_params = [p_name for p_name in fn_sig.parameters if p_name not in db_injected]

            def invoke_handler(events: Any, context: Any | None = None) -> Any:
                kwargs: dict[str, Any] = {arg_name: events}
                if has_context and context is not None:
                    kwargs["context"] = context
                return fn(**kwargs)

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> int:
                # Extract the host trigger argument (e.g. timer) to pass
                # to PollTrigger.run.  Support both positional and keyword.
                timer: Any = None
                if args:
                    timer = args[0]
                elif host_params:
                    timer = kwargs.get(host_params[0])
                return trigger.run(timer=timer, handler=invoke_handler)

            # Keep host trigger params visible in __signature__ so Azure
            # worker binding validation can find them.  Only hide the
            # db-injected params (events, context).
            wrapper.__signature__ = _build_host_signature(fn, db_injected)  # type: ignore[attr-defined]

            return wrapper

        return decorator

    def db_input(
        self,
        arg_name: str,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects a :class:`DbReader` instance into the handler.

        The handler parameter named ``arg_name`` will receive a pre-configured
        ``DbReader`` instance.  The reader is created fresh per invocation and
        closed automatically after the handler returns.

        Supports both sync and async handlers.

        Parameters
        ----------
        arg_name:
            Name of the handler parameter that receives the ``DbReader``.
        url:
            SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
        table:
            Optional table name for ``get()`` operations.
        schema:
            Optional schema qualifier.
        engine_provider:
            Optional shared ``EngineProvider`` for connection pooling.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _validate_arg_name(arg_name, fn, "db_input")
            is_async = asyncio.iscoroutinefunction(fn)

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    reader = DbReader(
                        url=url,
                        table=table,
                        schema=schema,
                        engine_provider=engine_provider,
                    )
                    try:
                        kwargs[arg_name] = reader
                        return await fn(*args, **kwargs)
                    finally:
                        reader.close()

                async_wrapper.__signature__ = _build_host_signature(  # type: ignore[attr-defined]
                    fn,
                    {arg_name},
                )
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                reader = DbReader(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )
                try:
                    kwargs[arg_name] = reader
                    return fn(*args, **kwargs)
                finally:
                    reader.close()

            wrapper.__signature__ = _build_host_signature(fn, {arg_name})  # type: ignore[attr-defined]
            return wrapper

        return decorator

    def db_output(
        self,
        arg_name: str,
        *,
        url: str,
        table: str,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects a :class:`DbWriter` instance into the handler.

        The handler parameter named ``arg_name`` will receive a pre-configured
        ``DbWriter`` instance.  The writer is created fresh per invocation and
        closed automatically after the handler returns.

        Supports both sync and async handlers.

        Parameters
        ----------
        arg_name:
            Name of the handler parameter that receives the ``DbWriter``.
        url:
            SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
        table:
            Table name for write operations.
        schema:
            Optional schema qualifier.
        engine_provider:
            Optional shared ``EngineProvider`` for connection pooling.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _validate_arg_name(arg_name, fn, "db_output")
            is_async = asyncio.iscoroutinefunction(fn)

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    writer = DbWriter(
                        url=url,
                        table=table,
                        schema=schema,
                        engine_provider=engine_provider,
                    )
                    try:
                        kwargs[arg_name] = writer
                        return await fn(*args, **kwargs)
                    finally:
                        writer.close()

                async_wrapper.__signature__ = _build_host_signature(  # type: ignore[attr-defined]
                    fn,
                    {arg_name},
                )
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                writer = DbWriter(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )
                try:
                    kwargs[arg_name] = writer
                    return fn(*args, **kwargs)
                finally:
                    writer.close()

            wrapper.__signature__ = _build_host_signature(fn, {arg_name})  # type: ignore[attr-defined]
            return wrapper

        return decorator
