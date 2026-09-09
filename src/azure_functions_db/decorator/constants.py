"""Shared constants for the decorator package."""

from __future__ import annotations

from .._metadata import METADATA_ATTR

# Parameter names reserved by Azure Functions runtime.
_RESERVED_ARGS = frozenset({"timer", "req", "context", "msg", "input", "output"})
_DB_DECORATOR_ATTR = "_db_decorators"
_TOOLKIT_META_ATTR = METADATA_ATTR

__all__ = ["_RESERVED_ARGS", "_DB_DECORATOR_ATTR", "_TOOLKIT_META_ATTR"]
