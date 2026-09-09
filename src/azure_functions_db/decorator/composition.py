"""Decorator composition tracking and toolkit-metadata merging."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from .._metadata import METADATA_ATTR, NAMESPACE, DbMetadata, merge_db_metadata
from ..core.errors import ConfigurationError
from .constants import _DB_DECORATOR_ATTR


def _get_db_decorators(fn: Callable[..., Any]) -> frozenset[str]:
    existing: object = getattr(fn, _DB_DECORATOR_ATTR, frozenset())
    if not isinstance(existing, frozenset):
        return frozenset()
    return existing


def _mark_decorator(fn: Callable[..., Any], name: str) -> None:
    setattr(fn, _DB_DECORATOR_ATTR, _get_db_decorators(fn) | {name})


def _merge_toolkit_metadata(
    fn: Callable[..., Any],
    namespace: str,
    payload: dict[str, Any],
) -> None:
    """Merge toolkit metadata into the convention attribute, preserving other namespaces.

    Backward-compatible shim delegating to the typed :func:`merge_db_metadata`
    for the ``db`` namespace.
    """
    if namespace == NAMESPACE:
        merge_db_metadata(fn, cast(DbMetadata, payload))
        return

    existing: dict[str, Any] = getattr(fn, METADATA_ATTR, {})
    if not isinstance(existing, dict):
        existing = {}
    existing = {**existing, namespace: payload}
    setattr(fn, METADATA_ATTR, existing)


def _check_composition(fn: Callable[..., Any], name: str) -> None:
    existing = _get_db_decorators(fn)

    if name in existing:
        msg = f"Decorator '{name}' cannot be applied twice to the same handler"
        raise ConfigurationError(msg)

    if name == "input" and "inject_reader" in existing:
        msg = (
            "Cannot combine 'input' and 'inject_reader' on the same handler — use one or the other"
        )
        raise ConfigurationError(msg)
    if name == "inject_reader" and "input" in existing:
        msg = (
            "Cannot combine 'inject_reader' and 'input' on the same handler — use one or the other"
        )
        raise ConfigurationError(msg)

    if name == "output" and "inject_writer" in existing:
        msg = (
            "Cannot combine 'output' and 'inject_writer' on the same handler — use one or the other"
        )
        raise ConfigurationError(msg)
    if name == "inject_writer" and "output" in existing:
        msg = (
            "Cannot combine 'inject_writer' and 'output' on the same handler — use one or the other"
        )
        raise ConfigurationError(msg)


__all__ = [
    "_get_db_decorators",
    "_mark_decorator",
    "_merge_toolkit_metadata",
    "_check_composition",
]
