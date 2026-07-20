"""Typed cross-package metadata contract for the ``db`` namespace.

This module defines the shape of the ``_azure_functions_metadata`` convention
attribute that decorators attach to Azure Functions handlers. Consumers (such as
the OpenAPI bridge) read this attribute to discover database bindings and
injections without importing this package.

The contract is behavior-preserving: the payload shape (``version``,
``bindings``, ``injections``) is unchanged from the historical ad-hoc dict.
"""

from __future__ import annotations

from typing import Any, TypedDict, cast

# Convention attribute name shared across every Azure Functions toolkit package.
METADATA_ATTR = "_azure_functions_metadata"

# Namespace owned by this package inside the convention attribute.
NAMESPACE = "db"

# Version of the ``db`` namespace payload. Consumers should read this and
# degrade gracefully when it exceeds the version they support.
DB_METADATA_VERSION = 1


class _BindingRequired(TypedDict):
    """Keys present on every binding entry."""

    kind: str
    parameter: str


class BindingEntry(_BindingRequired, total=False):
    """A single database binding (trigger/input/output).

    ``connection_setting`` and ``resource`` are only present for output-style
    bindings.
    """

    connection_setting: str
    resource: dict[str, Any]


class InjectionEntry(TypedDict):
    """A single reader/writer injection into a handler parameter."""

    kind: str
    parameter: str


class DbMetadata(TypedDict):
    """The ``db`` namespace payload stored under ``METADATA_ATTR``."""

    version: int
    bindings: list[BindingEntry]
    injections: list[InjectionEntry]


def merge_db_metadata(fn: Any, payload: DbMetadata) -> None:
    """Merge a ``db`` payload into the convention attribute on ``fn``.

    Preserves other namespaces and concatenates ``bindings``/``injections``
    when the handler already carries ``db`` metadata (multiple decorators).
    """
    existing: Any = getattr(fn, METADATA_ATTR, {})
    if not isinstance(existing, dict):
        existing = {}

    ns_meta = existing.get(NAMESPACE)
    if isinstance(ns_meta, dict):
        merged: dict[str, Any] = {**payload}
        merged["bindings"] = list(ns_meta.get("bindings", [])) + list(payload["bindings"])
        merged["injections"] = list(ns_meta.get("injections", [])) + list(payload["injections"])
        existing = {**existing, NAMESPACE: merged}
    else:
        existing = {**existing, NAMESPACE: dict(payload)}

    setattr(fn, METADATA_ATTR, existing)


def read_db_metadata(func: Any) -> DbMetadata | None:
    """Return the typed ``db`` metadata attached to ``func``, or ``None``."""
    meta = getattr(func, METADATA_ATTR, None)
    if not isinstance(meta, dict):
        return None
    entry = meta.get(NAMESPACE)
    if not isinstance(entry, dict):
        return None
    return cast("DbMetadata", entry)
