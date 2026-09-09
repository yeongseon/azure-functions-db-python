"""Public metadata-introspection helper for decorated handlers."""

from __future__ import annotations

from typing import Any

from .._metadata import read_db_metadata


def get_db_metadata(func: Any) -> dict[str, Any] | None:
    """Return db metadata if the function was decorated with DbBindings decorators.

    Returns None if the function has no db metadata attached.
    """
    meta = read_db_metadata(func)
    return dict(meta) if meta is not None else None


__all__ = ["get_db_metadata"]
