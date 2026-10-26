"""Finite integer-cursor source demonstrating the experimental polling protocol."""

from collections.abc import Sequence
from dataclasses import dataclass

from azure_functions_db.core.types import CursorValue, RawRecord, SourceDescriptor


@dataclass(frozen=True, slots=True)
class NumberSource:
    @property
    def source_descriptor(self) -> SourceDescriptor:
        return SourceDescriptor(name="numbers", kind="example", fingerprint="numbers-v1")

    def fetch(self, cursor: CursorValue | None, batch_size: int) -> Sequence[RawRecord]:
        match cursor:
            case None:
                start = 1
            case int() as position:
                start = position + 1
            case _:
                msg = "NumberSource accepts only integer cursors"
                raise ValueError(msg)
        return [{"id": number} for number in range(start, min(start + batch_size, 6))]
