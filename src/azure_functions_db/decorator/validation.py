"""Argument, resolver, and model validation helpers for the decorator API."""

from __future__ import annotations

from collections.abc import Callable
import inspect
from typing import Any

from pydantic import BaseModel

from ..core.errors import ConfigurationError
from .constants import _RESERVED_ARGS


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


__all__ = [
    "_validate_arg_name",
    "_build_host_signature",
    "_validate_resolver",
    "_resolve_callable",
    "_validate_model_type",
    "_apply_input_model",
    "_normalize_output_row",
]
