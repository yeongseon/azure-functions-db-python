from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class PollContext:
    poller_name: str
    invocation_id: str
    batch_id: str
    lease_owner: str
    checkpoint_before: dict[str, object]
    checkpoint_after_candidate: dict[str, object] | None
    tick_started_at: datetime
    source_name: str
