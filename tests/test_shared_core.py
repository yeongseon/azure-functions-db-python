from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import threading
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert, text
from sqlalchemy.engine import Engine

from azure_functions_db import SqlAlchemySource
from azure_functions_db.core.config import DbConfig, resolve_env_vars
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.core.errors import ConfigurationError, CursorSerializationError, DbError
from azure_functions_db.core.serializers import parse_checkpoint_cursor, serialize_cursor_part
from azure_functions_db.core.types import RawRecord, Row, RowDict
from azure_functions_db.state.errors import StateStoreError
from azure_functions_db.trigger.errors import PollerError


def _create_orders_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("updated_at", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["orders"]),
            [
                {"id": 1, "name": "Alice", "updated_at": 100},
                {"id": 2, "name": "Bob", "updated_at": 200},
            ],
        )
    engine.dispose()
    return url


class TestDbConfig:
    def test_creation(self) -> None:
        config = DbConfig(
            connection_url="sqlite:///test.db",
            pool_size=7,
            pool_recycle=120,
            echo=True,
            connect_args={"timeout": 5},
        )

        assert config.connection_url == "sqlite:///test.db"
        assert config.pool_size == 7
        assert config.pool_recycle == 120
        assert config.echo is True
        assert config.connect_args == {"timeout": 5}

    def test_env_var_resolution(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PHASE8_DB_URL", "sqlite:///resolved.db")
        assert resolve_env_vars("%PHASE8_DB_URL%") == "sqlite:///resolved.db"

    def test_env_var_partial_substitution(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DB_PASS", "s3cret")
        assert (
            resolve_env_vars("postgresql://user:%DB_PASS%@host/db")
            == "postgresql://user:s3cret@host/db"
        )

    def test_env_var_multiple_tokens(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("HOST", "myhost")
        monkeypatch.setenv("PORT", "5432")
        assert resolve_env_vars("postgresql://%HOST%:%PORT%/db") == "postgresql://myhost:5432/db"

    def test_env_var_literal_percent_escape(self) -> None:
        assert resolve_env_vars("100%% done") == "100% done"

    def test_env_var_missing_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="not set"):
            resolve_env_vars("%NONEXISTENT_VAR_XYZ%")

    def test_env_var_no_tokens_returns_unchanged(self) -> None:
        assert resolve_env_vars("sqlite:///plain.db") == "sqlite:///plain.db"

    def test_frozen_immutable(self) -> None:
        config = DbConfig(connection_url="sqlite:///test.db")

        with pytest.raises(FrozenInstanceError):
            config.connection_url = "sqlite:///other.db"  # type: ignore[misc]

    def test_engine_kwargs_default_empty(self) -> None:
        config = DbConfig(connection_url="sqlite:///test.db")
        assert config.engine_kwargs == {}

    def test_engine_kwargs_passthrough(self) -> None:
        config = DbConfig(
            connection_url="sqlite:///test.db",
            engine_kwargs={"pool_pre_ping": True, "max_overflow": 5},
        )
        assert config.engine_kwargs == {"pool_pre_ping": True, "max_overflow": 5}


class TestEngineProvider:
    def test_get_engine_caches_by_full_config(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "cache.db")
        provider = EngineProvider()
        config = DbConfig(connection_url=url)

        first = provider.get_engine(config)
        second = provider.get_engine(config)

        assert first is second
        provider.dispose_all()

    def test_create_isolated_engine_returns_distinct_engines(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "isolated.db")
        provider = EngineProvider()
        config = DbConfig(connection_url=url)

        first = provider.create_isolated_engine(config)
        second = provider.create_isolated_engine(config)

        assert first is not second
        first.dispose()
        second.dispose()

    def test_dispose_all_clears_cache(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "dispose_all.db")
        provider = EngineProvider()
        config = DbConfig(connection_url=url)

        first = provider.get_engine(config)
        provider.dispose_all()
        second = provider.get_engine(config)

        assert first is not second
        provider.dispose_all()

    def test_get_engine_is_thread_safe(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "threadsafe.db")
        provider = EngineProvider()
        config = DbConfig(connection_url=url)
        barrier = threading.Barrier(8)
        engines: list[Engine] = []
        lock = threading.Lock()

        def worker() -> None:
            barrier.wait()
            engine = provider.get_engine(config)
            with lock:
                engines.append(engine)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(engines) == 8
        assert len({id(engine) for engine in engines}) == 1
        provider.dispose_all()

    def test_engine_kwargs_forwarded(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "kwargs.db")
        provider = EngineProvider()
        config = DbConfig(connection_url=url, engine_kwargs={"pool_pre_ping": True})

        engine = provider.get_engine(config)
        assert engine.pool._pre_ping is True
        provider.dispose_all()

    def test_different_engine_kwargs_create_separate_engines(self, tmp_path: Path) -> None:
        url = _create_orders_db(tmp_path / "kwargs_diff.db")
        provider = EngineProvider()
        config_a = DbConfig(connection_url=url, engine_kwargs={"pool_pre_ping": True})
        config_b = DbConfig(connection_url=url, engine_kwargs={"pool_pre_ping": False})

        engine_a = provider.get_engine(config_a)
        engine_b = provider.get_engine(config_b)

        assert engine_a is not engine_b
        provider.dispose_all()


class TestSerializers:
    def test_serialize_cursor_part_datetime(self) -> None:
        value = datetime(2026, 4, 8, 1, 2, 3, 456789, tzinfo=timezone.utc)
        assert serialize_cursor_part(value) == "2026-04-08T01:02:03.456789+00:00"

    def test_serialize_cursor_part_decimal(self) -> None:
        assert serialize_cursor_part(Decimal("12.34")) == "12.34"

    def test_serialize_cursor_part_uuid(self) -> None:
        value = uuid4()
        assert serialize_cursor_part(value) == str(value)

    @pytest.mark.parametrize("value", [None, "x", 1, 1.5, True])
    def test_serialize_cursor_part_primitives(self, value: object) -> None:
        assert serialize_cursor_part(value) == value

    def test_serialize_cursor_part_unsupported(self) -> None:
        with pytest.raises(TypeError, match="Unsupported cursor value type"):
            serialize_cursor_part(object())

    def test_parse_checkpoint_cursor_none(self) -> None:
        assert parse_checkpoint_cursor(None) is None

    @pytest.mark.parametrize("value", ["x", 1, 1.5, True])
    def test_parse_checkpoint_cursor_primitive(self, value: object) -> None:
        assert parse_checkpoint_cursor(value) == value

    def test_parse_checkpoint_cursor_list_to_tuple(self) -> None:
        assert parse_checkpoint_cursor([1, "a"]) == (1, "a")

    def test_parse_checkpoint_cursor_tuple(self) -> None:
        assert parse_checkpoint_cursor((1, "a")) == (1, "a")

    def test_parse_checkpoint_cursor_invalid(self) -> None:
        with pytest.raises(CursorSerializationError, match="Unsupported cursor type"):
            parse_checkpoint_cursor({"cursor": 1})

    def test_parse_checkpoint_cursor_rejects_invalid_composite_member(self) -> None:
        with pytest.raises(
            CursorSerializationError, match="Unsupported cursor part type at index 0"
        ):
            parse_checkpoint_cursor([{"bad": 1}, 2])

    def test_parse_checkpoint_cursor_rejects_nested_list(self) -> None:
        with pytest.raises(
            CursorSerializationError, match="Unsupported cursor part type at index 1"
        ):
            parse_checkpoint_cursor([1, [2, 3]])


class TestSharedTypes:
    def test_aliases_exist(self) -> None:
        raw: RawRecord = {"id": 1}
        row: Row = raw
        payload: RowDict = {"id": 1}

        assert raw == {"id": 1}
        assert row == raw
        assert payload == {"id": 1}
        assert RawRecord == dict[str, object]
        assert Row == RawRecord
        assert RowDict == dict[str, object]


class TestErrorHierarchy:
    def test_poller_error_is_db_error(self) -> None:
        assert issubclass(PollerError, DbError)

    def test_state_store_error_is_db_error(self) -> None:
        assert issubclass(StateStoreError, DbError)


def test_sqlalchemy_source_uses_engine_provider(tmp_path: Path) -> None:
    url = _create_orders_db(tmp_path / "provider.db")
    provider = EngineProvider()
    source = SqlAlchemySource(
        url=url,
        table="orders",
        cursor_column="updated_at",
        pk_columns=["id"],
        engine_provider=provider,
    )

    rows = source.fetch(cursor=None, batch_size=10)
    source.dispose()

    assert [cast(int, row["id"]) for row in rows] == [1, 2]

    shared_engine = provider.get_engine(DbConfig(connection_url=url))
    with shared_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one()

    assert count == 2
    provider.dispose_all()
