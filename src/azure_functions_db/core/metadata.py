from __future__ import annotations

import threading
import weakref

from sqlalchemy.engine import Engine
from sqlalchemy.schema import MetaData, Table

from .config import resolve_env_vars


def reflect_table(
    *,
    engine: Engine,
    url: str,
    schema: str | None,
    table_name: str,
    error_cls: type[Exception],
) -> Table:
    """Reflect *table_name* through the shared metadata cache and return it.

    Shared by ``DbReader``, ``DbWriter``, and ``SqlAlchemySource``.  Raises an
    instance of *error_cls* when reflection fails or the table is not found so
    each caller keeps its domain-specific error type.
    """
    cache = get_metadata_cache()
    try:
        table = cache.get_or_reflect(
            engine=engine,
            url=url,
            schema=schema,
            table_name=table_name,
        )
    except Exception as exc:
        msg = f"Failed to reflect table '{table_name}'"
        raise error_cls(msg) from exc

    key = f"{schema}.{table_name}" if schema else table_name
    if table is None:
        msg = f"Table '{key}' not found in database"
        raise error_cls(msg)
    return table


_SubKey = tuple[str, "str | None", str]
_CacheKey = tuple[int, str, "str | None", str]


class MetadataCache:
    """Process-wide reflected-table cache, scoped by engine identity.

    Keying by URL/schema/table alone is unsound (#436): two independent
    physical databases can share the exact same textual URL (e.g. two
    separate ``sqlite:///:memory:`` engines), so a second reader would
    silently receive the first database's reflected columns. Every cache
    entry is therefore additionally scoped by ``id(engine)``.

    ``id()`` is safe here specifically because entries for a given engine
    are purged via a ``weakref.finalize`` callback that runs when that
    *exact* engine object is garbage collected -- so a later, unrelated
    object can never reuse that id while stale entries for it still exist.
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        self._cache: dict[_CacheKey, Table] = {}
        self._inflight: dict[_CacheKey, threading.Event] = {}
        self._engine_keys: dict[int, set[_CacheKey]] = {}

    def _track_key(self, engine: Engine, cache_key: _CacheKey) -> None:
        engine_id = id(engine)
        keys = self._engine_keys.get(engine_id)
        if keys is None:
            keys = set()
            self._engine_keys[engine_id] = keys
            weakref.finalize(engine, self._purge_engine, engine_id)
        keys.add(cache_key)

    def _purge_engine(self, engine_id: int) -> None:
        with self._lock:
            keys = self._engine_keys.pop(engine_id, None)
            if not keys:
                return
            for key in keys:
                self._cache.pop(key, None)
                self._inflight.pop(key, None)

    def get_or_reflect(
        self,
        *,
        engine: Engine,
        url: str,
        schema: str | None,
        table_name: str,
    ) -> Table | None:
        resolved_url = resolve_env_vars(url)
        cache_key: _CacheKey = (id(engine), resolved_url, schema, table_name)

        should_reflect = False
        wait_event: threading.Event | None = None

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

            wait_event = self._inflight.get(cache_key)
            if wait_event is None:
                wait_event = threading.Event()
                self._inflight[cache_key] = wait_event
                self._track_key(engine, cache_key)
                should_reflect = True

        if not should_reflect:
            _ = wait_event.wait()
            with self._lock:
                return self._cache.get(cache_key)

        table: Table | None = None
        try:
            metadata = MetaData()
            metadata.reflect(bind=engine, schema=schema, only=[table_name])

            key = f"{schema}.{table_name}" if schema else table_name
            reflected = metadata.tables.get(key)
            if isinstance(reflected, Table):
                table = reflected

            if table is not None:
                with self._lock:
                    self._cache[cache_key] = table

            return table
        finally:
            with self._lock:
                event = self._inflight.pop(cache_key, None)
                if event is not None:
                    event.set()

    def invalidate(self, *, engine: Engine, url: str, schema: str | None, table_name: str) -> None:
        resolved_url = resolve_env_vars(url)
        cache_key: _CacheKey = (id(engine), resolved_url, schema, table_name)
        with self._lock:
            _ = self._cache.pop(cache_key, None)
            keys = self._engine_keys.get(id(engine))
            if keys is not None:
                keys.discard(cache_key)

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._inflight.clear()
            self._engine_keys.clear()


_metadata_cache = MetadataCache()


def get_metadata_cache() -> MetadataCache:
    return _metadata_cache
