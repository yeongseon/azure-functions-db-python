from __future__ import annotations

from dataclasses import dataclass, field
import os
import re
from typing import Any

from .errors import ConfigurationError

_ENV_TOKEN = re.compile(r"%%|%(\w+)%")


def resolve_env_vars(value: str) -> str:
    def _replace(m: re.Match[str]) -> str:
        if m.group(0) == "%%":
            return "%"
        var_name = m.group(1)
        resolved = os.environ.get(var_name)
        if resolved is None:
            msg = f"Environment variable '{var_name}' is not set"
            raise ConfigurationError(msg)
        return resolved

    return _ENV_TOKEN.sub(_replace, value)


@dataclass(frozen=True, slots=True, kw_only=True)
class DbConfig:
    connection_url: str
    pool_size: int = 5
    pool_recycle: int = 3600
    echo: bool = False
    connect_args: dict[str, object] | None = None
    engine_kwargs: dict[str, Any] = field(default_factory=dict)
