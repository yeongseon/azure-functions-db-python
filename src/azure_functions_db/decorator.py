from __future__ import annotations

import asyncio
from collections.abc import Callable
import functools
import inspect
import logging
from typing import Any, Literal

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


def _validate_resolver(
    resolver: Callable[..., dict[str, object]],
    fn: Callable[..., Any],
    injected_args: set[str],
    param_label: str,
    decorator_name: str,
) -> list[str]:
    """Validate a resolver callable at decoration time.

    Ensures the resolver's parameter names are a subset of the handler's
    non-injected parameters and that it does not use ``*args`` or ``**kwargs``.

    Returns the list of parameter names the resolver expects.
    """
    resolver_sig = inspect.signature(resolver)
    for p in resolver_sig.parameters.values():
        if p.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            msg = f"{decorator_name} {param_label} callable must not use *args or **kwargs"
            raise ConfigurationError(msg)

    handler_sig = inspect.signature(fn, follow_wrapped=False)
    handler_params = {name for name in handler_sig.parameters if name not in injected_args}
    resolver_params = list(resolver_sig.parameters.keys())
    unknown = set(resolver_params) - handler_params
    if unknown:
        msg = (
            f"{decorator_name} {param_label} callable references parameters "
            f"{sorted(unknown)} not found in handler '{fn.__name__}'. "
            f"Available: {sorted(handler_params)}"
        )
        raise ConfigurationError(msg)
    return resolver_params


def _resolve_callable(
    resolver: Callable[..., dict[str, object]],
    resolver_params: list[str],
    all_kwargs: dict[str, Any],
) -> dict[str, object]:
    """Call a resolver with matching kwargs extracted from the handler invocation."""
    call_kwargs = {name: all_kwargs[name] for name in resolver_params if name in all_kwargs}
    return resolver(**call_kwargs)


class DbFunctionApp:
    """Azure Functions-style decorator API for database integration.

    Provides ``db_trigger``, ``db_input``, ``db_output``, ``db_reader``,
    and ``db_writer`` decorator methods that wrap the imperative API
    (PollTrigger, DbReader, DbWriter) in an Azure Functions-native
    decorator experience.

    **Data injection** (``db_input`` / ``db_output``):
        Handlers receive actual data or have return values auto-written.

    **Client injection** (``db_reader`` / ``db_writer``):
        Handlers receive ``DbReader`` / ``DbWriter`` instances for
        imperative control.

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

    # ------------------------------------------------------------------
    # Data injection decorators
    # ------------------------------------------------------------------

    def db_input(
        self,
        arg_name: str,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        pk: dict[str, object] | Callable[..., dict[str, object]] | None = None,
        query: str | None = None,
        params: dict[str, object] | Callable[..., dict[str, object]] | None = None,
        on_not_found: Literal["none", "raise"] = "none",
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects query results into the handler.

        The handler parameter named ``arg_name`` will receive the actual
        data from the database, not a ``DbReader`` instance.  Exactly one
        of ``pk`` or ``query`` must be provided.

        **PK mode** (single row):
            The parameter receives ``dict[str, object] | None``.  Use a
            static dict for fixed lookups or a callable for dynamic
            resolution from other handler parameters::

                @db.db_input("user", url=..., table="users",
                             pk=lambda req: {"id": req.params["id"]})
                def handler(req, user): ...

        **Query mode** (multiple rows):
            The parameter receives ``list[dict[str, object]]``::

                @db.db_input("users", url=...,
                             query="SELECT * FROM users WHERE active = :active",
                             params={"active": True})
                def handler(users): ...

        Parameters
        ----------
        arg_name:
            Name of the handler parameter that receives the data.
        url:
            SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
        table:
            Table name.  Required when using ``pk``.
        schema:
            Optional schema qualifier.
        pk:
            Primary key for single-row lookup.  Either a static dict or a
            callable whose parameter names match other handler parameters.
        query:
            SQL query string for multi-row results.  Use ``:name``
            placeholders for parameters.
        params:
            Parameters for ``query``.  Either a static dict or a callable.
        on_not_found:
            Behavior when ``pk`` lookup returns no row.  ``"none"`` (default)
            injects ``None``; ``"raise"`` raises ``NotFoundError``.
        engine_provider:
            Optional shared ``EngineProvider`` for connection pooling.
        """
        # Validate mutual exclusion at call time (before decoration).
        if pk is not None and query is not None:
            msg = "db_input requires exactly one of 'pk' or 'query', not both"
            raise ConfigurationError(msg)
        if pk is None and query is None:
            msg = "db_input requires exactly one of 'pk' or 'query'"
            raise ConfigurationError(msg)
        if pk is not None and table is None:
            msg = "db_input with 'pk' requires 'table' to be set"
            raise ConfigurationError(msg)
        if params is not None and query is None:
            msg = "db_input 'params' is only valid with 'query'"
            raise ConfigurationError(msg)

        use_pk = pk is not None
        pk_callable: Callable[..., dict[str, object]] | None = pk if callable(pk) else None
        pk_static: dict[str, object] | None = None if callable(pk) else pk
        params_callable: Callable[..., dict[str, object]] | None = (
            params if callable(params) else None
        )
        params_static: dict[str, object] | None = None if callable(params) else params

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _validate_arg_name(arg_name, fn, "db_input")

            pk_resolver_params: list[str] = []
            params_resolver_params: list[str] = []
            if pk_callable is not None:
                pk_resolver_params = _validate_resolver(
                    pk_callable, fn, {arg_name}, "pk", "db_input"
                )
            if params_callable is not None:
                params_resolver_params = _validate_resolver(
                    params_callable, fn, {arg_name}, "params", "db_input"
                )

            is_async = asyncio.iscoroutinefunction(fn)

            def _do_read(all_kwargs: dict[str, Any]) -> Any:
                """Execute the DB read (runs in calling thread)."""
                reader = DbReader(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )
                try:
                    if use_pk:
                        resolved_pk: dict[str, object]
                        if pk_callable is not None:
                            resolved_pk = _resolve_callable(
                                pk_callable, pk_resolver_params, all_kwargs
                            )
                        elif pk_static is not None:
                            resolved_pk = pk_static
                        else:
                            msg = "db_input: unreachable – neither pk callable nor pk static"
                            raise ConfigurationError(msg)
                        result = reader.get(pk=resolved_pk)
                        if result is None and on_not_found == "raise":
                            from .core.errors import NotFoundError

                            msg = f"db_input: no row found for pk={resolved_pk} in table '{table}'"
                            raise NotFoundError(msg)
                        return result
                    else:
                        resolved_params: dict[str, object] | None = None
                        if params_callable is not None:
                            resolved_params = _resolve_callable(
                                params_callable, params_resolver_params, all_kwargs
                            )
                        elif params_static is not None:
                            resolved_params = params_static
                        if query is None:
                            msg = "db_input: unreachable – query mode but query is None"
                            raise ConfigurationError(msg)
                        return reader.query(query, params=resolved_params)
                finally:
                    reader.close()

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    data = await asyncio.to_thread(_do_read, kwargs)
                    kwargs[arg_name] = data
                    return await fn(*args, **kwargs)

                async_wrapper.__signature__ = _build_host_signature(  # type: ignore[attr-defined]
                    fn,
                    {arg_name},
                )
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                data = _do_read(kwargs)
                kwargs[arg_name] = data
                return fn(*args, **kwargs)

            wrapper.__signature__ = _build_host_signature(fn, {arg_name})  # type: ignore[attr-defined]
            return wrapper

        return decorator

    def db_output(
        self,
        *,
        url: str,
        table: str,
        schema: str | None = None,
        action: Literal["insert", "upsert"] = "insert",
        conflict_columns: list[str] | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that auto-writes the handler's return value to the database.

        The handler's return value is intercepted and written to the
        configured table.  No ``arg_name`` is needed.

        Return value contract:
            - ``dict`` -> single-row write
            - ``list[dict]`` -> batch write
            - ``None`` -> no-op

        Parameters
        ----------
        url:
            SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
        table:
            Table name for write operations.
        schema:
            Optional schema qualifier.
        action:
            Write action: ``"insert"`` (default) or ``"upsert"``.
        conflict_columns:
            Columns for upsert conflict resolution.  Required when
            ``action="upsert"``.
        engine_provider:
            Optional shared ``EngineProvider`` for connection pooling.
        """
        if action == "upsert" and not conflict_columns:
            msg = "db_output with action='upsert' requires 'conflict_columns'"
            raise ConfigurationError(msg)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            is_async = asyncio.iscoroutinefunction(fn)

            def _do_write(result: Any) -> None:
                """Write the handler result to DB (runs in calling thread)."""
                if result is None:
                    return

                writer = DbWriter(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )
                try:
                    if isinstance(result, dict):
                        if action == "upsert":
                            if conflict_columns is None:
                                msg = "db_output: unreachable – upsert without conflict_columns"
                                raise ConfigurationError(msg)
                            writer.upsert(data=result, conflict_columns=conflict_columns)
                        else:
                            writer.insert(data=result)
                    elif isinstance(result, list):
                        if action == "upsert":
                            if conflict_columns is None:
                                msg = "db_output: unreachable – upsert without conflict_columns"
                                raise ConfigurationError(msg)
                            writer.upsert_many(rows=result, conflict_columns=conflict_columns)
                        else:
                            writer.insert_many(rows=result)
                    else:
                        msg = (
                            f"db_output: handler returned {type(result).__name__}, "
                            f"expected dict, list[dict], or None"
                        )
                        raise ConfigurationError(msg)
                finally:
                    writer.close()

            fn_sig = inspect.signature(fn, follow_wrapped=False)

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    result = await fn(*args, **kwargs)
                    await asyncio.to_thread(_do_write, result)
                    return result

                async_wrapper.__signature__ = fn_sig  # type: ignore[attr-defined]
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = fn(*args, **kwargs)
                _do_write(result)
                return result

            wrapper.__signature__ = fn_sig  # type: ignore[attr-defined]
            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # Client injection decorators (imperative escape hatches)
    # ------------------------------------------------------------------

    def db_reader(
        self,
        arg_name: str,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects a :class:`DbReader` instance into the handler.

        Use this when you need imperative control over reads (multiple
        queries, dynamic SQL, etc.).  For simple data injection, prefer
        :meth:`db_input`.

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
            _validate_arg_name(arg_name, fn, "db_reader")
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

    def db_writer(
        self,
        arg_name: str,
        *,
        url: str,
        table: str,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects a :class:`DbWriter` instance into the handler.

        Use this when you need imperative control over writes (multiple
        operations, transactions, update/delete, etc.).  For simple
        auto-write, prefer :meth:`db_output`.

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
            _validate_arg_name(arg_name, fn, "db_writer")
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
