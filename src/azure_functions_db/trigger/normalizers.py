from __future__ import annotations

from collections.abc import Callable
import hashlib
import logging

from ..core.errors import ConfigurationError
from ..core.serializers import serialize_cursor_part
from ..core.types import CursorPart, CursorValue, RawRecord, SourceDescriptor
from .events import RowChange

logger = logging.getLogger(__name__)

EventNormalizer = Callable[[RawRecord, SourceDescriptor], RowChange]


def _cursor_part(value: object) -> CursorPart:
    return serialize_cursor_part(value)


def _compute_event_id(
    *,
    source_name: str,
    fingerprint: str,
    op: str,
    cursor: CursorValue,
    pk: dict[str, object],
) -> str:
    parts = [source_name, fingerprint, op, repr(cursor), repr(sorted(pk.items()))]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def default_normalizer(record: RawRecord, descriptor: SourceDescriptor) -> RowChange:
    """Select source-aware normalization in PollTrigger; direct calls require a factory."""
    del record, descriptor
    raise ConfigurationError(
        "default_normalizer requires source cursor metadata. "
        "Use make_normalizer(cursor_column=..., pk_columns=...) for direct calls "
        "or pass default_normalizer to PollTrigger with a source exposing those attributes."
    )


def make_normalizer(*, cursor_column: str, pk_columns: list[str]) -> EventNormalizer:
    def normalizer(record: RawRecord, descriptor: SourceDescriptor) -> RowChange:
        cursor_val = _cursor_part(record[cursor_column])
        pk = {column: record[column] for column in pk_columns}
        cursor_parts = [cursor_val]
        cursor_parts.extend(_cursor_part(record[column]) for column in pk_columns)
        cursor: CursorValue = tuple(cursor_parts)
        return RowChange(
            event_id=_compute_event_id(
                source_name=descriptor.name,
                fingerprint=descriptor.fingerprint,
                op="upsert",
                cursor=cursor,
                pk=pk,
            ),
            op="upsert",
            source=descriptor,
            cursor=cursor,
            pk=pk,
            before=None,
            after=dict(record),
            metadata={},
        )

    return normalizer
