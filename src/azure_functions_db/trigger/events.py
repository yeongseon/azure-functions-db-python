from __future__ import annotations

from dataclasses import dataclass, field

from azure_functions_db.core.types import CursorValue, SourceDescriptor


@dataclass(frozen=True, slots=True)
class RowChange:
    event_id: str
    op: str
    source: SourceDescriptor
    cursor: CursorValue
    pk: dict[str, object]
    before: dict[str, object] | None
    after: dict[str, object] | None
    metadata: dict[str, object] = field(default_factory=dict)

    VALID_OPS: frozenset[str] = frozenset({"insert", "update", "upsert", "delete", "unknown"})  # noqa: RUF012

    def __post_init__(self) -> None:
        if self.op not in self.VALID_OPS:
            msg = f"Invalid op '{self.op}', must be one of {sorted(self.VALID_OPS)}"
            raise ValueError(msg)
        if not self.event_id:
            msg = "event_id must not be empty"
            raise ValueError(msg)
