from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from .errors import CursorSerializationError
from .types import CursorPart, CursorValue


def serialize_cursor_part(value: object) -> CursorPart:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    msg = f"Unsupported cursor value type: {type(value).__name__}"
    raise TypeError(msg)


def _is_valid_cursor_part(value: object) -> bool:
    return isinstance(value, (str, int, float, bool)) or value is None


def parse_checkpoint_cursor(raw: object) -> CursorValue | None:
    if raw is None or isinstance(raw, (str, int, float, bool)):
        return raw
    if isinstance(raw, (tuple, list)):
        parts = tuple(raw)
        for i, part in enumerate(parts):
            if not _is_valid_cursor_part(part):
                msg = f"Unsupported cursor part type at index {i}: {type(part).__name__}"
                raise CursorSerializationError(msg)
        return parts
    msg = f"Unsupported cursor type in checkpoint: {type(raw).__name__}"
    raise CursorSerializationError(msg)
