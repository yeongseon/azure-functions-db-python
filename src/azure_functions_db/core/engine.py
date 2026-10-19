from __future__ import annotations

import atexit
import json
import threading

from sqlalchemy.engine import Engine, create_engine

from .config import DbConfig
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
        # config.connection_url is already fully resolved -- every DbConfig
        # reaching this provider was constructed through resolve_db_config(),
        # which resolves %VAR% tokens exactly once. Re-resolving here (#438)
        # would reinterpret the ALREADY-EXPANDED connection string (which may
        # itself legitimately contain %-looking substrings, e.g. a
        # percent-encoded password) as if it still needed expansion.
        return create_engine(config.connection_url, **kwargs)

    def dispose_all(self) -> None:
        with self._lock:
            engines = list(self._engines.values())
            self._engines.clear()

        for engine in engines:
            engine.dispose()

    @staticmethod
    def _cache_key(config: DbConfig) -> str:
        # config.connection_url is already resolved -- see the matching note
        # in create_isolated_engine (#438).
        normalized = {
            "connection_url": config.connection_url,
            "pool_size": config.pool_size,
            "pool_recycle": config.pool_recycle,
            "echo": config.echo,
            "connect_args": config.connect_args,
            "engine_kwargs": config.engine_kwargs,
        }

        def _encode(obj: object) -> str:
            # A type name plus str()/repr() is not enough: two different
            # instances of the SAME class can legitimately produce the same
            # str() (e.g. a custom SQLAlchemy `creator` callable that embeds
            # per-instance state without overriding __str__), and such
            # instances can resolve to entirely different physical
            # databases (#156). Fall back to object identity instead. This
            # stays safe here specifically because the cache only maps a key
            # to a *live* Engine, and SQLAlchemy's connection pool retains
            # its own reference to a `creator` for as long as the Engine it
            # belongs to exists -- so the id() of a cached non-JSON value
            # cannot be reused by an unrelated object while its cache entry
            # is still present.
            qualified = f"{type(obj).__module__}.{type(obj).__qualname__}"
            return f"<{qualified}:{id(obj)}>"

        return json.dumps(normalized, sort_keys=True, default=_encode)
