"""Shared sync/async handler-wrapping helpers used by every decorator."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
import functools
import inspect
from typing import Any

from .composition import _mark_decorator, _merge_toolkit_metadata
from .validation import _bind_host_args, _build_host_signature, _finalize_injected_call


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
    ``release is None`` no resource cleanup is performed. Binding happens before
    acquisition for unmanaged wrappers and after acquisition for managed ones.
    """
    is_async = inspect.iscoroutinefunction(fn)
    host_sig = _build_host_signature(fn, {arg_name})

    @contextmanager
    def injected_call(
        args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> Iterator[tuple[tuple[Any, ...], dict[str, Any]]]:
        convert = to_async_arg if is_async else to_sync_arg
        if release is None:
            host_bound = _bind_host_args(host_sig, args, kwargs)
            yield _finalize_injected_call(fn, host_bound, arg_name, convert(acquire()))
        else:
            resource = acquire()
            try:
                host_bound = _bind_host_args(host_sig, args, kwargs)
                yield _finalize_injected_call(fn, host_bound, arg_name, convert(resource))
            finally:
                release(resource)

    if is_async:

        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            with injected_call(args, kwargs) as (call_args, call_kwargs):
                return await fn(*call_args, **call_kwargs)

        return _finalize_wrapper(
            async_wrapper, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata
        )

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with injected_call(args, kwargs) as (call_args, call_kwargs):
            return fn(*call_args, **call_kwargs)

    return _finalize_wrapper(wrapper, fn=fn, arg_name=arg_name, kind=kind, metadata=metadata)


__all__ = ["_finalize_wrapper", "_wrap_handler"]
