"""Shared sync/async handler-wrapping helpers used by every decorator."""

from __future__ import annotations

from collections.abc import Callable
import functools
import inspect
from typing import Any

from .composition import _mark_decorator, _merge_toolkit_metadata
from .validation import _build_host_signature


def _finalize_wrapper(
    wrapper: Callable[..., Any],
    *,
    fn: Callable[..., Any],
    arg_name: str,
    kind: str,
    metadata: dict[str, Any],
) -> Callable[..., Any]:
    """Attach the host signature and toolkit metadata shared by every decorator."""
    setattr(wrapper, "__signature__", _build_host_signature(fn, {arg_name}))
    _mark_decorator(wrapper, kind)
    _merge_toolkit_metadata(wrapper, "db", metadata)
    return wrapper


def _wrap_handler(
    fn: Callable[..., Any],
    *,
    kind: str,
    arg_name: str,
    metadata: dict[str, Any],
    acquire: Callable[[], Any],
    to_async_arg: Callable[[Any], Any],
    to_sync_arg: Callable[[Any], Any],
    release: Callable[[Any], None] | None = None,
) -> Callable[..., Any]:
    """Build a sync or async handler wrapper with uniform resource injection.

    Detects ``inspect.iscoroutinefunction`` once and returns the matching
    wrapper.  ``acquire`` runs per invocation to obtain the resource,
    ``to_async_arg`` / ``to_sync_arg`` map it to the injected argument, and
    ``release`` (when given) tears it down in a ``finally`` block.  When
    ``release is None`` the ``try/finally`` is omitted so exception tracebacks
    stay identical to a plain wrapper.
    """
    is_async = inspect.iscoroutinefunction(fn)

    if release is None:
        if is_async:

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                kwargs[arg_name] = to_async_arg(acquire())
                return await fn(*args, **kwargs)

            return _finalize_wrapper(
                async_wrapper, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata
            )

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            kwargs[arg_name] = to_sync_arg(acquire())
            return fn(*args, **kwargs)

        return _finalize_wrapper(wrapper, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata)

    if is_async:

        @functools.wraps(fn)
        async def async_wrapper_rel(*args: Any, **kwargs: Any) -> Any:
            resource = acquire()
            try:
                kwargs[arg_name] = to_async_arg(resource)
                return await fn(*args, **kwargs)
            finally:
                release(resource)

        return _finalize_wrapper(
            async_wrapper_rel, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata
        )

    @functools.wraps(fn)
    def wrapper_rel(*args: Any, **kwargs: Any) -> Any:
        resource = acquire()
        try:
            kwargs[arg_name] = to_sync_arg(resource)
            return fn(*args, **kwargs)
        finally:
            release(resource)

    return _finalize_wrapper(wrapper_rel, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata)


__all__ = ["_finalize_wrapper", "_wrap_handler"]
