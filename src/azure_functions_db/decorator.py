from __future__ import annotations

import asyncio
from collections.abc import Callable
import functools
import inspect
import logging
from typing import Any, Literal

from pydantic import BaseModel

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


class _AsyncDbReaderProxy:
    def __init__(self, reader: DbReader) -> None:
        self._reader = reader

    async def get(self, *, pk: dict[str, object]) -> dict[str, object] | None:
        return await asyncio.to_thread(self._reader.get, pk=pk)

    async def query(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._reader.query, sql, params=params)

    def close(self) -> None:
        self._reader.close()


class _AsyncDbWriterProxy:
    def __init__(self, writer: DbWriter) -> None:
        self._writer = writer

    async def insert(self, *, data: dict[str, object]) -> None:
        await asyncio.to_thread(self._writer.insert, data=data)

    async def insert_many(self, *, rows: list[dict[str, object]]) -> None:
        await asyncio.to_thread(self._writer.insert_many, rows=rows)

    async def upsert(self, *, data: dict[str, object], conflict_columns: list[str]) -> None:
        await asyncio.to_thread(self._writer.upsert, data=data, conflict_columns=conflict_columns)

    async def upsert_many(
        self,
        *,
        rows: list[dict[str, object]],
        conflict_columns: list[str],
    ) -> None:
        await asyncio.to_thread(
            self._writer.upsert_many, rows=rows, conflict_columns=conflict_columns
        )

    def close(self) -> None:
        self._writer.close()


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
        if p.kind == inspect.Parameter.POSITIONAL_ONLY:
            msg = (
                f"{decorator_name} {param_label} callable must not use positional-only "
                f"parameters ('{p.name}'). Use keyword-compatible parameters instead."
            )
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


def _validate_model_type(model: object | None) -> None:
    if model is None:
        return
    if not isinstance(model, type) or not issubclass(model, BaseModel):
        model_name = model.__name__ if isinstance(model, type) else type(model).__name__
        msg = f"input model must be a subclass of BaseModel, got '{model_name}'"
        raise ConfigurationError(msg)


def _apply_input_model(
    result: dict[str, object] | list[dict[str, object]] | None,
    model: type[BaseModel] | None,
) -> dict[str, object] | list[dict[str, object]] | BaseModel | list[BaseModel] | None:
    if model is None:
        return result
    if result is None:
        return None
    if isinstance(result, list):
        return [model.model_validate(row) for row in result]
    return model.model_validate(result)


def _normalize_output_row(row: dict[str, object] | BaseModel) -> dict[str, object]:
    if isinstance(row, BaseModel):
        return row.model_dump()
    return row


class DbBindings:
    """Azure Functions-style decorator API for database integration.

    Provides ``trigger``, ``input``, ``output``, ``inject_reader``,
    and ``inject_writer`` decorator methods that wrap the imperative API
    (PollTrigger, DbReader, DbWriter) in an Azure Functions-native
    decorator experience.

    **Data injection** (``input`` / ``output``):
        Handlers receive actual data or have return values auto-written.

    **Client injection** (``inject_reader`` / ``inject_writer``):
        Handlers receive ``DbReader`` / ``DbWriter`` instances for
        imperative control.

    Decorator order contract:
        Azure decorators outermost, db decorators closest to the function::

            @app.function_name(name="my_func")
            @app.schedule(...)          # Azure trigger (outermost)
            @db.trigger(...)            # db decorator (closest to fn)
            def my_func(events): ...

    Note: This is a pseudo-trigger implementation. ``trigger`` requires
    an actual Azure Functions trigger (e.g. ``@app.schedule``) to fire.
    It does not register a native Azure Functions binding.
    """

    def trigger(
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
            _validate_arg_name(arg_name, fn, "trigger")

            # Reject async handlers: PollTrigger.run is synchronous and
            # calling an async function without await would silently return
            # an unawaited coroutine.
            if inspect.iscoroutinefunction(fn):
                msg = "trigger does not support async handlers. Use a sync handler instead."
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

            db_injected = {arg_name}
            if has_context:
                db_injected.add("context")
            host_params = [p_name for p_name in fn_sig.parameters if p_name not in db_injected]

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> int:
                timer: Any = None
                if args:
                    timer = args[0]
                elif host_params:
                    timer = kwargs.get(host_params[0])

                bound_args = dict(kwargs)
                if args and host_params:
                    for i, val in enumerate(args):
                        if i < len(host_params):
                            bound_args[host_params[i]] = val

                def invoke_handler(events: Any, context: Any | None = None) -> Any:
                    call_kwargs: dict[str, Any] = dict(bound_args)
                    call_kwargs[arg_name] = events
                    if has_context and context is not None:
                        call_kwargs["context"] = context
                    return fn(**call_kwargs)

                return trigger.run(timer=timer, handler=invoke_handler)

            # Keep host trigger params visible in __signature__ so Azure
            # worker binding validation can find them.  Only hide the
            # db-injected params (events, context).
            setattr(wrapper, "__signature__", _build_host_signature(fn, db_injected))

            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # Data injection decorators
    # ------------------------------------------------------------------

    def input(
        self,
        arg_name: str,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        pk: dict[str, object] | Callable[..., dict[str, object]] | None = None,
        query: str | None = None,
        params: dict[str, object] | Callable[..., dict[str, object]] | None = None,
        model: type[BaseModel] | None = None,
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

                @db.input("user", url=..., table="users",
                             pk=lambda req: {"id": req.params["id"]})
                def handler(req, user): ...

        **Query mode** (multiple rows):
            The parameter receives ``list[dict[str, object]]``::

                @db.input("users", url=...,
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
        if on_not_found not in ("none", "raise"):
            msg = f"input on_not_found must be 'none' or 'raise', got '{on_not_found}'"
            raise ConfigurationError(msg)
        if pk is not None and query is not None:
            msg = "input requires exactly one of 'pk' or 'query', not both"
            raise ConfigurationError(msg)
        if pk is None and query is None:
            msg = "input requires exactly one of 'pk' or 'query'"
            raise ConfigurationError(msg)
        if pk is not None and table is None:
            msg = "input with 'pk' requires 'table' to be set"
            raise ConfigurationError(msg)
        if params is not None and query is None:
            msg = "input 'params' is only valid with 'query'"
            raise ConfigurationError(msg)
        _validate_model_type(model)

        use_pk = pk is not None
        pk_callable: Callable[..., dict[str, object]] | None = pk if callable(pk) else None
        pk_static: dict[str, object] | None = None if callable(pk) else pk
        params_callable: Callable[..., dict[str, object]] | None = (
            params if callable(params) else None
        )
        params_static: dict[str, object] | None = None if callable(params) else params

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _validate_arg_name(arg_name, fn, "input")

            pk_resolver_params: list[str] = []
            params_resolver_params: list[str] = []
            if pk_callable is not None:
                pk_resolver_params = _validate_resolver(pk_callable, fn, {arg_name}, "pk", "input")
            if params_callable is not None:
                params_resolver_params = _validate_resolver(
                    params_callable, fn, {arg_name}, "params", "input"
                )

            is_async = inspect.iscoroutinefunction(fn)

            def _resolve_read_args(
                all_kwargs: dict[str, Any],
            ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
                """Resolve pk/params from handler kwargs (safe to call on any thread)."""
                resolved_pk: dict[str, object] | None = None
                resolved_params: dict[str, object] | None = None
                if use_pk:
                    if pk_callable is not None:
                        resolved_pk = _resolve_callable(pk_callable, pk_resolver_params, all_kwargs)
                    elif pk_static is not None:
                        resolved_pk = pk_static
                    else:
                        msg = "input: unreachable – neither pk callable nor pk static"
                        raise ConfigurationError(msg)
                else:
                    if params_callable is not None:
                        resolved_params = _resolve_callable(
                            params_callable, params_resolver_params, all_kwargs
                        )
                    elif params_static is not None:
                        resolved_params = params_static
                return resolved_pk, resolved_params

            def _execute_read(
                resolved_pk: dict[str, object] | None,
                resolved_params: dict[str, object] | None,
            ) -> Any:
                """Execute DB I/O (blocking — run in worker thread for async)."""
                reader = DbReader(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )
                try:
                    if use_pk:
                        if resolved_pk is None:
                            msg = "input: unreachable – pk mode but resolved_pk is None"
                            raise ConfigurationError(msg)
                        result = reader.get(pk=resolved_pk)
                        if result is None and on_not_found == "raise":
                            from .core.errors import NotFoundError

                            msg = f"input: no row found for pk={resolved_pk} in table '{table}'"
                            raise NotFoundError(msg)
                        return result
                    else:
                        if query is None:
                            msg = "input: unreachable – query mode but query is None"
                            raise ConfigurationError(msg)
                        return reader.query(query, params=resolved_params)
                finally:
                    reader.close()

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    r_pk, r_params = _resolve_read_args(kwargs)
                    data = await asyncio.to_thread(_execute_read, r_pk, r_params)
                    kwargs[arg_name] = _apply_input_model(data, model)
                    return await fn(*args, **kwargs)

                setattr(async_wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                r_pk, r_params = _resolve_read_args(kwargs)
                data = _execute_read(r_pk, r_params)
                kwargs[arg_name] = _apply_input_model(data, model)
                return fn(*args, **kwargs)

            setattr(wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
            return wrapper

        return decorator

    def output(
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
        if action not in ("insert", "upsert"):
            msg = f"output action must be 'insert' or 'upsert', got '{action}'"
            raise ConfigurationError(msg)
        if action == "upsert" and not conflict_columns:
            msg = "output with action='upsert' requires 'conflict_columns'"
            raise ConfigurationError(msg)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            is_async = inspect.iscoroutinefunction(fn)

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
                    if isinstance(result, (dict, BaseModel)):
                        row = _normalize_output_row(result)
                        if action == "upsert":
                            if conflict_columns is None:
                                msg = "output: unreachable – upsert without conflict_columns"
                                raise ConfigurationError(msg)
                            writer.upsert(data=row, conflict_columns=conflict_columns)
                        else:
                            writer.insert(data=row)
                    elif isinstance(result, list):
                        bad = next(
                            (
                                i
                                for i, row in enumerate(result)
                                if not isinstance(row, (dict, BaseModel))
                            ),
                            None,
                        )
                        if bad is not None:
                            bad_type = type(result[bad]).__name__
                            msg = (
                                f"output: handler returned list with non-dict element "
                                f"at index {bad} ({bad_type}); expected list[dict | BaseModel]"
                            )
                            raise ConfigurationError(msg)
                        rows = [_normalize_output_row(row) for row in result]
                        if action == "upsert":
                            if conflict_columns is None:
                                msg = "output: unreachable – upsert without conflict_columns"
                                raise ConfigurationError(msg)
                            writer.upsert_many(rows=rows, conflict_columns=conflict_columns)
                        else:
                            writer.insert_many(rows=rows)
                    else:
                        msg = (
                            f"output: handler returned {type(result).__name__}, "
                            f"expected dict, list[dict], BaseModel, list[dict | BaseModel], or None"
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

                setattr(async_wrapper, "__signature__", fn_sig)
                return async_wrapper

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = fn(*args, **kwargs)
                _do_write(result)
                return result

            setattr(wrapper, "__signature__", fn_sig)
            return wrapper

        return decorator

    # ------------------------------------------------------------------
    # Client injection decorators (imperative escape hatches)
    # ------------------------------------------------------------------

    def inject_reader(
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
        :meth:`input`.

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
            _validate_arg_name(arg_name, fn, "inject_reader")
            is_async = inspect.iscoroutinefunction(fn)

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    reader = DbReader(
                        url=url,
                        table=table,
                        schema=schema,
                        engine_provider=engine_provider,
                    )
                    proxy = _AsyncDbReaderProxy(reader)
                    try:
                        kwargs[arg_name] = proxy
                        return await fn(*args, **kwargs)
                    finally:
                        reader.close()

                setattr(async_wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
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

            setattr(wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
            return wrapper

        return decorator

    def inject_writer(
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
        auto-write, prefer :meth:`output`.

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
            _validate_arg_name(arg_name, fn, "inject_writer")
            is_async = inspect.iscoroutinefunction(fn)

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    writer = DbWriter(
                        url=url,
                        table=table,
                        schema=schema,
                        engine_provider=engine_provider,
                    )
                    proxy = _AsyncDbWriterProxy(writer)
                    try:
                        kwargs[arg_name] = proxy
                        return await fn(*args, **kwargs)
                    finally:
                        writer.close()

                setattr(async_wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
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

            setattr(wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
            return wrapper

        return decorator
