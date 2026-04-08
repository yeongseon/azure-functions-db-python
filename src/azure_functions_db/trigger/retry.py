from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            msg = "max_retries must be non-negative"
            raise ValueError(msg)
        if self.base_delay_seconds <= 0:
            msg = "base_delay_seconds must be positive"
            raise ValueError(msg)
        if self.max_delay_seconds < self.base_delay_seconds:
            msg = "max_delay_seconds must be >= base_delay_seconds"
            raise ValueError(msg)
        if self.exponential_base < 1:
            msg = "exponential_base must be >= 1"
            raise ValueError(msg)

    def delay_for_attempt(self, attempt: int) -> float:
        delay = self.base_delay_seconds * (self.exponential_base ** attempt)
        return min(delay, self.max_delay_seconds)
