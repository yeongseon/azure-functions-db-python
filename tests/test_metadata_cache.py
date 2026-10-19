from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import threading
from typing import cast
from unittest.mock import Mock, patch

from sqlalchemy import MetaData, Table
from sqlalchemy.engine import Engine

from azure_functions_db.core.metadata import MetadataCache


def _make_table(name: str = "users") -> Table:
    return Table(name, MetaData())


class TestMetadataCache:
    def test_cache_miss_reflects_and_caches(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        table = _make_table()
        metadata = Mock()
        metadata.tables = {"users": table}

        with patch("azure_functions_db.core.metadata.MetaData", return_value=metadata):
            result = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )

        assert result is table
        cast(Mock, metadata.reflect).assert_called_once_with(
            bind=engine,
            schema=None,
            only=["users"],
        )

    def test_cache_hit_skips_second_reflection(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        table = _make_table()
        metadata = Mock()
        metadata.tables = {"users": table}

        with patch("azure_functions_db.core.metadata.MetaData", return_value=metadata):
            first = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )
            second = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )

        assert first is table
        assert second is table
        cast(Mock, metadata.reflect).assert_called_once()

    def test_concurrent_access_reflects_once_per_key(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        table = _make_table()
        entered_reflect = threading.Event()
        release_reflect = threading.Event()
        metadata_instances: list[Mock] = []

        def metadata_factory() -> Mock:
            metadata = Mock()
            metadata.tables = {"users": table}

            def reflect(*, bind: Engine, schema: str | None, only: list[str]) -> None:
                del bind, schema, only
                entered_reflect.set()
                _ = release_reflect.wait(timeout=1)

            cast(Mock, metadata.reflect).side_effect = reflect
            metadata_instances.append(metadata)
            return metadata

        def get_table() -> Table | None:
            return cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )

        with patch("azure_functions_db.core.metadata.MetaData", side_effect=metadata_factory):
            with ThreadPoolExecutor(max_workers=5) as executor:
                first_future = executor.submit(get_table)
                assert entered_reflect.wait(timeout=1)
                futures = [executor.submit(get_table) for _ in range(4)]
                release_reflect.set()

                first = first_future.result(timeout=1)
                results = [future.result(timeout=1) for future in futures]

        assert first is table
        assert results == [table, table, table, table]
        assert len(metadata_instances) == 1
        cast(Mock, metadata_instances[0].reflect).assert_called_once_with(
            bind=engine,
            schema=None,
            only=["users"],
        )

    def test_invalidate_removes_specific_entry(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        tables = [_make_table(), _make_table()]
        metadata_instances: list[Mock] = []

        def metadata_factory() -> Mock:
            metadata = Mock()
            metadata.tables = {"users": tables[len(metadata_instances)]}
            metadata_instances.append(metadata)
            return metadata

        with patch("azure_functions_db.core.metadata.MetaData", side_effect=metadata_factory):
            first = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )
            cache.invalidate(
                engine=engine, url="sqlite:///:memory:", schema=None, table_name="users"
            )
            second = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )

        assert first is tables[0]
        assert second is tables[1]
        assert len(metadata_instances) == 2

    def test_purge_callback_reentering_lock_does_not_deadlock(self) -> None:
        """Regression (#436): weakref.finalize callbacks can run
        synchronously, in whatever thread happens to drop the last
        reference to the finalized object -- including a thread that
        already holds this cache's lock (e.g. inside get_or_reflect's
        critical section, if an unrelated engine's refcount drops to zero
        as a side effect of a dict mutation there). A plain threading.Lock
        would deadlock the moment _purge_engine tried to re-acquire it from
        that nested call; this reproduces exactly that nesting directly."""
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        table = _make_table("users")
        metadata = Mock()
        metadata.tables = {"users": table}

        with patch("azure_functions_db.core.metadata.MetaData", return_value=metadata):
            cache.get_or_reflect(
                engine=engine, url="sqlite:///:memory:", schema=None, table_name="users"
            )

        engine_id = id(engine)

        def reenter_while_holding_lock() -> None:
            with cache._lock:  # noqa: SLF001
                # Simulates a finalizer firing mid-critical-section, in the
                # same thread, for an unrelated engine.
                cache._purge_engine(engine_id)  # noqa: SLF001

        thread = threading.Thread(target=reenter_while_holding_lock)
        thread.start()
        thread.join(timeout=2)

        assert not thread.is_alive(), "lock re-entry from the same thread deadlocked"

    def test_clear_removes_all_entries(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        tables = [_make_table("users"), _make_table("orders")]
        metadata_instances: list[Mock] = []

        def metadata_factory() -> Mock:
            metadata = Mock()
            metadata.tables = {
                "users": tables[0],
                "orders": tables[1],
            }
            metadata_instances.append(metadata)
            return metadata

        with patch("azure_functions_db.core.metadata.MetaData", side_effect=metadata_factory):
            _ = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )
            _ = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="orders",
            )
            cache.clear()
            first = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )
            second = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="orders",
            )

        assert first is tables[0]
        assert second is tables[1]
        assert len(metadata_instances) == 4

    def test_different_engines_with_same_url_use_distinct_entries(self) -> None:
        """Regression (#436): two independent physical databases sharing the
        same textual URL (e.g. separate `sqlite:///:memory:` engines) must
        not share a reflected Table -- keying by URL/schema/table alone let
        the second reader silently receive the first database's columns."""
        cache = MetadataCache()
        engine_a = Mock(spec=Engine)
        engine_b = Mock(spec=Engine)
        table_a = _make_table("users")
        table_b = _make_table("users")
        reflected_tables = [table_a, table_b]
        created_metadata: list[Mock] = []

        def metadata_factory() -> Mock:
            metadata = Mock()
            metadata.tables = {"users": reflected_tables[len(created_metadata)]}
            created_metadata.append(metadata)
            return metadata

        with patch("azure_functions_db.core.metadata.MetaData", side_effect=metadata_factory):
            result_a = cache.get_or_reflect(
                engine=engine_a, url="sqlite:///:memory:", schema=None, table_name="users"
            )
            result_b = cache.get_or_reflect(
                engine=engine_b, url="sqlite:///:memory:", schema=None, table_name="users"
            )

        assert result_a is table_a
        assert result_b is table_b
        assert len(created_metadata) == 2

    def test_same_engine_with_same_url_still_reuses_cache(self) -> None:
        """Control for #436: the common case (one engine, repeated lookups)
        must still hit the cache -- the fix must not disable caching
        entirely, only scope it correctly by engine identity."""
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        table = _make_table("users")
        metadata = Mock()
        metadata.tables = {"users": table}

        with patch("azure_functions_db.core.metadata.MetaData", return_value=metadata):
            first = cache.get_or_reflect(
                engine=engine, url="sqlite:///:memory:", schema=None, table_name="users"
            )
            second = cache.get_or_reflect(
                engine=engine, url="sqlite:///:memory:", schema=None, table_name="users"
            )

        assert first is table
        assert second is table
        cast(Mock, metadata.reflect).assert_called_once()

    def test_different_url_schema_and_table_use_distinct_entries(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        plain_table = _make_table("users")
        schema_table = _make_table("users")
        other_url_table = _make_table("users")
        other_table = _make_table("orders")
        reflected_tables = [plain_table, schema_table, other_url_table, other_table]
        reflected_keys = ["users", "public.users", "users", "orders"]

        def metadata_factory() -> Mock:
            metadata = Mock()
            index = len(created_metadata)
            metadata.tables = {reflected_keys[index]: reflected_tables[index]}
            created_metadata.append(metadata)
            return metadata

        created_metadata: list[Mock] = []

        with patch(
            "azure_functions_db.core.metadata.MetaData", side_effect=metadata_factory
        ) as patched:
            result_plain = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///one.db",
                schema=None,
                table_name="users",
            )
            result_schema = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///one.db",
                schema="public",
                table_name="users",
            )
            result_other_url = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///two.db",
                schema=None,
                table_name="users",
            )
            result_other_table = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///one.db",
                schema=None,
                table_name="orders",
            )

        assert result_plain is plain_table
        assert result_schema is schema_table
        assert result_other_url is other_url_table
        assert result_other_table is other_table
        assert patched.call_count == 4

    def test_returns_none_when_table_not_found(self) -> None:
        cache = MetadataCache()
        engine = Mock(spec=Engine)
        metadata = Mock()
        metadata.tables = {}

        with patch("azure_functions_db.core.metadata.MetaData", return_value=metadata):
            result = cache.get_or_reflect(
                engine=engine,
                url="sqlite:///:memory:",
                schema=None,
                table_name="users",
            )

        assert result is None
