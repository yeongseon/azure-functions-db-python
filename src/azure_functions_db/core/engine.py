from __future__ import annotations

import atexit
import json
import threading

from sqlalchemy.engine import Engine, create_engine

from .config import DbConfig, resolve_env_vars
from .errors import ConfigurationError


class EngineProvider:
    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._engines: dict[str, Engine] = {}
        # Best-effort pool cleanup on interpreter shutdown.  Azure Functions
        # workers stay alive between invocations; without this, connection-pool
        # resources are leaked when the process exits.
        atexit.register(self.dispose_all)

    def get_engine(self, config: DbConfig) -> Engine:
        cache_key = self._cache_key(config)
        with self._lock:
            engine = self._engines.get(cache_key)
            if engine is None:
                engine = self.create_isolated_engine(config)
                self._engines[cache_key] = engine
            return engine

    def create_isolated_engine(self, config: DbConfig) -> Engine:
        if "connect_args" in config.engine_kwargs:
            msg = (
                "Do not pass 'connect_args' inside engine_kwargs; "
                "use DbConfig.connect_args instead. Mixing the two would "
                "silently let engine_kwargs override DbConfig.connect_args."
            )
            raise ConfigurationError(msg)

        kwargs: dict[str, object] = {
            "pool_size": config.pool_size,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo,
        }
        if config.connect_args is not None:
            kwargs["connect_args"] = dict(config.connect_args)
        kwargs.update(config.engine_kwargs)
        return create_engine(resolve_env_vars(config.connection_url), **kwargs)

    def dispose_all(self) -> None:
        with self._lock:
            engines = list(self._engines.values())
            self._engines.clear()

        for engine in engines:
            engine.dispose()

    @staticmethod
    def _cache_key(config: DbConfig) -> str:
        normalized = {
            "connection_url": resolve_env_vars(config.connection_url),
            "pool_size": config.pool_size,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo,
            "connect_args": config.connect_args,
            "engine_kwargs": config.engine_kwargs,
        }

        def _encode(obj: object) -> str:
            # Include the fully-qualified type name so that two non-JSON-
            # serializable objects whose str() representations are identical
            # but whose types differ still produce distinct cache keys.
            qualified = f"{type(obj).__module__}.{type(obj).__qualname__}"
            return f"<{qualified}:{str(obj)!r}>"

        return json.dumps(normalized, sort_keys=True, default=_encode)
