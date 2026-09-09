"""The public :class:`DbBindings` decorator API."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import functools
import inspect
from typing import Any, Literal

from pydantic import BaseModel

from ..binding.reader import DbReader
from ..binding.writer import DbWriter
from ..core.engine import EngineProvider
from ..core.errors import ConfigurationError
from ..observability import MetricsCollector
from ..trigger.normalizers import EventNormalizer
from ..trigger.poll import PollTrigger
from ..trigger.retry import RetryPolicy
from ..trigger.runner import SourceAdapter, StateStore
from .async_proxies import (
    _AsyncDbOutProxy,
    _AsyncDbReaderProxy,
    _AsyncDbWriterProxy,
)
from .composition import (
    _check_composition,
    _mark_decorator,
    _merge_toolkit_metadata,
)
from .out import DbOut
from .validation import (
    _apply_input_model,
    _build_host_signature,
    _resolve_callable,
    _validate_arg_name,
    _validate_model_type,
    _validate_resolver,
)
from .wrapper import _finalize_wrapper, _wrap_handler


class DbBindings:
    """Azure Functions-style decorator API for database integration.

    Provides ``trigger``, ``input``, ``output``, ``inject_reader``,
    and ``inject_writer`` decorator methods that wrap the imperative API
    (PollTrigger, DbReader, DbWriter) in an Azure Functions-native
    decorator experience.

    **Data injection** (``input`` / ``output``):
        ``input`` injects query results into handler parameters.
        ``output`` injects a :class:`DbOut` instance; call ``.set()``
        to write data explicitly.

    **Client injection** (``inject_reader`` / ``inject_writer``):
        Handlers receive ``DbReader`` / ``DbWriter`` instances for
        imperative control.

    Decorator order contract:
        Decorator composition rules:
            - Azure decorators outermost, db decorators closest to the function
            - ``trigger`` + ``output`` can be combined (process events and write results)
            - ``trigger`` + ``inject_writer`` can be combined (imperative write in trigger handler)
            - ``input`` + ``output`` can be combined (read data, write results)
            - ``input`` and ``inject_reader`` are mutually exclusive
            - ``output`` and ``inject_writer`` are mutually exclusive
            - No decorator can be applied twice to the same handler

        Valid combinations::

            @app.schedule(...)
            @db.trigger(...)        # Azure trigger outermost
            @db.output("out", ...)  # db output innermost
            def handler(events, out: DbOut) -> None:
                out.set([...])

            @db.input("user", ...)
            @db.output("out", ...)
            def handler(user, out: DbOut) -> dict:
                out.set({...})
                return user

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

        .. note::
            Only synchronous handlers are supported. Async handlers will raise
            ``ConfigurationError`` at decoration time. This is because
            ``PollTrigger.run`` is synchronous.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _check_composition(fn, "trigger")
            _validate_arg_name(arg_name, fn, "trigger")

            # Reject async handlers: PollTrigger.run is synchronous and
            # calling an async function without await would silently return
            # an unawaited coroutine.
            if inspect.iscoroutinefunction(fn):
                msg = (
                    "trigger does not support async handlers because PollTrigger.run() "
                    "is synchronous. Use a sync handler instead."
                )
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
            _mark_decorator(wrapper, "trigger")
            _merge_toolkit_metadata(
                wrapper,
                "db",
                {
                    "version": 1,
                    "bindings": [
                        {
                            "kind": "trigger",
                            "parameter": arg_name,
                        }
                    ],
                    "injections": [],
                },
            )

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

        Supports both sync and async handlers. For async handlers, blocking
        database I/O is automatically offloaded via ``asyncio.to_thread()``.
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
            _check_composition(fn, "input")
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

            binding_info: dict[str, Any] = {
                "kind": "input",
                "parameter": arg_name,
                "connection_setting": url,
                "resource": {"table": table} if table else {},
                "query_kind": "pk" if use_pk else "text",
            }
            if model is not None:
                binding_info["model_ref"] = f"{model.__module__}:{model.__qualname__}"

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
                            from ..core.errors import NotFoundError

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

            input_metadata: dict[str, Any] = {
                "version": 1,
                "bindings": [binding_info],
                "injections": [],
            }

            if is_async:

                @functools.wraps(fn)
                async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                    r_pk, r_params = _resolve_read_args(kwargs)
                    data = await asyncio.to_thread(_execute_read, r_pk, r_params)
                    kwargs[arg_name] = _apply_input_model(data, model)
                    return await fn(*args, **kwargs)

                return _finalize_wrapper(
                    async_wrapper,
                    fn=fn,
                    arg_name=arg_name,
                    kind="input",
                    metadata=input_metadata,
                )

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                r_pk, r_params = _resolve_read_args(kwargs)
                data = _execute_read(r_pk, r_params)
                kwargs[arg_name] = _apply_input_model(data, model)
                return fn(*args, **kwargs)

            return _finalize_wrapper(
                wrapper,
                fn=fn,
                arg_name=arg_name,
                kind="input",
                metadata=input_metadata,
            )

        return decorator

    def output(
        self,
        arg_name: str,
        *,
        url: str,
        table: str,
        schema: str | None = None,
        action: Literal["insert", "upsert"] = "insert",
        conflict_columns: list[str] | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Decorator that injects a :class:`DbOut` instance into the handler.

        Follows the native Azure Functions output binding pattern
        (``func.Out[T]`` with ``.set()``).  The handler parameter named
        ``arg_name`` will receive a ``DbOut`` instance for sync handlers
        or an ``_AsyncDbOutProxy`` for async handlers.

        The handler's return value is **not** intercepted — use
        ``out.set(data)`` to write explicitly.

        Parameters
        ----------
        arg_name:
            Name of the handler parameter that receives the ``DbOut``.
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

        Supports both sync and async handlers. For async handlers, blocking
        database I/O is automatically offloaded via ``asyncio.to_thread()``.
        """
        if action not in ("insert", "upsert"):
            msg = f"output action must be 'insert' or 'upsert', got '{action}'"
            raise ConfigurationError(msg)
        if action == "upsert" and not conflict_columns:
            msg = "output with action='upsert' requires 'conflict_columns'"
            raise ConfigurationError(msg)

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _check_composition(fn, "output")
            _validate_arg_name(arg_name, fn, "output")
            out = DbOut(
                url=url,
                table=table,
                schema=schema,
                action=action,
                conflict_columns=conflict_columns,
                engine_provider=engine_provider,
            )
            proxy = _AsyncDbOutProxy(out)

            return _wrap_handler(
                fn,
                kind="output",
                arg_name=arg_name,
                metadata={
                    "version": 1,
                    "bindings": [
                        {
                            "kind": "output",
                            "parameter": arg_name,
                            "connection_setting": url,
                            "resource": {"table": table},
                        }
                    ],
                    "injections": [],
                },
                acquire=lambda: None,
                to_async_arg=lambda _resource: proxy,
                to_sync_arg=lambda _resource: out,
            )

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

        Supports both sync and async handlers. For async handlers, blocking
        database I/O is automatically offloaded via ``asyncio.to_thread()``.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _check_composition(fn, "inject_reader")
            _validate_arg_name(arg_name, fn, "inject_reader")

            def _acquire() -> DbReader:
                return DbReader(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )

            return _wrap_handler(
                fn,
                kind="inject_reader",
                arg_name=arg_name,
                metadata={
                    "version": 1,
                    "bindings": [],
                    "injections": [{"kind": "reader", "parameter": arg_name}],
                },
                acquire=_acquire,
                to_async_arg=lambda reader: _AsyncDbReaderProxy(reader),
                to_sync_arg=lambda reader: reader,
                release=lambda reader: reader.close(),
            )

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

        Supports both sync and async handlers. For async handlers, blocking
        database I/O is automatically offloaded via ``asyncio.to_thread()``.
        """

        def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
            _check_composition(fn, "inject_writer")
            _validate_arg_name(arg_name, fn, "inject_writer")

            def _acquire() -> DbWriter:
                return DbWriter(
                    url=url,
                    table=table,
                    schema=schema,
                    engine_provider=engine_provider,
                )

            return _wrap_handler(
                fn,
                kind="inject_writer",
                arg_name=arg_name,
                metadata={
                    "version": 1,
                    "bindings": [],
                    "injections": [{"kind": "writer", "parameter": arg_name}],
                },
                acquire=_acquire,
                to_async_arg=lambda writer: _AsyncDbWriterProxy(writer),
                to_sync_arg=lambda writer: writer,
                release=lambda writer: writer.close(),
            )

        return decorator


__all__ = ["DbBindings"]
