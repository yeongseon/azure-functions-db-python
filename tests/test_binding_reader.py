from __future__ import annotations

import os
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert

from azure_functions_db.binding.reader import DbReader
from azure_functions_db.core.config import DbConfig
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.core.errors import ConfigurationError, DbConnectionError, QueryError


def _create_users_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("email", String(100)),
        Column("active", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["users"]),
            [
                {"id": 1, "name": "Alice", "email": "alice@example.com", "active": 1},
                {"id": 2, "name": "Bob", "email": "bob@example.com", "active": 1},
                {"id": 3, "name": "Charlie", "email": "charlie@example.com", "active": 0},
            ],
        )
    engine.dispose()
    return url


def _create_order_items_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "order_items",
        metadata,
        Column("order_id", Integer, primary_key=True),
        Column("item_id", Integer, primary_key=True),
        Column("qty", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["order_items"]),
            [
                {"order_id": 1, "item_id": 1, "qty": 2},
                {"order_id": 1, "item_id": 2, "qty": 3},
                {"order_id": 2, "item_id": 1, "qty": 1},
            ],
        )
    engine.dispose()
    return url


def _create_duplicate_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "events",
        metadata,
        Column("id", Integer),
        Column("kind", String(50)),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["events"]),
            [
                {"id": 1, "kind": "click"},
                {"id": 1, "kind": "click"},
            ],
        )
    engine.dispose()
    return url


@pytest.fixture()
def users_url(tmp_path: Path) -> str:
    return _create_users_db(tmp_path / "users.db")


@pytest.fixture()
def composite_pk_url(tmp_path: Path) -> str:
    return _create_order_items_db(tmp_path / "order_items.db")


@pytest.fixture()
def duplicate_url(tmp_path: Path) -> str:
    return _create_duplicate_db(tmp_path / "duplicates.db")


class TestDbReaderConstructor:
    def test_valid_with_table(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        assert reader._table_name == "users"
        reader.close()

    def test_valid_without_table(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        assert reader._table_name is None
        reader.close()

    def test_valid_with_schema(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users", schema="main")
        assert reader._schema == "main"
        reader.close()

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="url must not be empty"):
            DbReader(url="")

    def test_kw_only(self) -> None:
        with pytest.raises(TypeError):
            DbReader("sqlite:///:memory:")  # type: ignore[misc]

    def test_env_var_resolution(self) -> None:
        with patch.dict(os.environ, {"TEST_READER_URL": "sqlite:///:memory:"}):
            reader = DbReader(url="%TEST_READER_URL%")
            assert reader._url == "sqlite:///:memory:"
            reader.close()

    def test_env_var_missing_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="not set"):
            DbReader(url="%NONEXISTENT_READER_VAR%")

    def test_lazy_initialization(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        assert not reader._initialized
        reader.close()

    def test_engine_provider_accepted(self, users_url: str) -> None:
        provider = EngineProvider()
        reader = DbReader(url=users_url, table="users", engine_provider=provider)
        assert reader._engine_provider is provider
        reader.close()
        provider.dispose_all()

    def test_db_config_created(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        assert reader._db_config.connection_url == users_url
        reader.close()


class TestDbReaderGet:
    def test_get_existing_row(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        row = reader.get(pk={"id": 1})
        assert row is not None
        assert row["name"] == "Alice"
        assert row["email"] == "alice@example.com"
        reader.close()

    def test_get_missing_row_returns_none(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        row = reader.get(pk={"id": 999})
        assert row is None
        reader.close()

    def test_get_composite_pk(self, composite_pk_url: str) -> None:
        reader = DbReader(url=composite_pk_url, table="order_items")
        row = reader.get(pk={"order_id": 1, "item_id": 2})
        assert row is not None
        assert cast(int, row["qty"]) == 3
        reader.close()

    def test_get_composite_pk_missing(self, composite_pk_url: str) -> None:
        reader = DbReader(url=composite_pk_url, table="order_items")
        row = reader.get(pk={"order_id": 99, "item_id": 99})
        assert row is None
        reader.close()

    def test_get_without_table_raises(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        with pytest.raises(ConfigurationError, match="requires 'table'"):
            reader.get(pk={"id": 1})
        reader.close()

    def test_get_unknown_column_raises(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with pytest.raises(ConfigurationError, match="not part of the primary key"):
            reader.get(pk={"nonexistent": 1})
        reader.close()

    def test_get_non_pk_column_raises(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with pytest.raises(ConfigurationError, match="not part of the primary key"):
            reader.get(pk={"email": "alice@example.com"})
        reader.close()

    def test_get_partial_composite_pk(self, composite_pk_url: str) -> None:
        reader = DbReader(url=composite_pk_url, table="order_items")
        row = reader.get(pk={"order_id": 2})
        assert row is not None
        assert cast(int, row["qty"]) == 1
        reader.close()

    def test_get_empty_pk_raises(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with pytest.raises(ConfigurationError, match="pk must not be empty"):
            reader.get(pk={})
        reader.close()

    def test_get_triggers_initialization(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        assert not reader._initialized
        reader.get(pk={"id": 1})
        assert reader._initialized
        reader.close()

    def test_get_on_table_without_pk_raises(self, duplicate_url: str) -> None:
        reader = DbReader(url=duplicate_url, table="events")
        with pytest.raises(ConfigurationError, match="not part of the primary key"):
            reader.get(pk={"id": 1})
        reader.close()

    def test_get_partial_pk_multiple_matches_raises(
        self, composite_pk_url: str
    ) -> None:
        reader = DbReader(url=composite_pk_url, table="order_items")
        with pytest.raises(QueryError, match="expected at most 1 row"):
            reader.get(pk={"order_id": 1})
        reader.close()

    def test_get_with_partial_pk(self, composite_pk_url: str) -> None:
        reader = DbReader(url=composite_pk_url, table="order_items")
        row = reader.get(pk={"order_id": 2, "item_id": 1})
        assert row is not None
        assert cast(int, row["qty"]) == 1
        reader.close()


class TestDbReaderQuery:
    def test_query_all_rows(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        rows = reader.query("SELECT * FROM users")
        assert len(rows) == 3
        reader.close()

    def test_query_with_params(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        rows = reader.query(
            "SELECT * FROM users WHERE active = :active",
            params={"active": 1},
        )
        assert len(rows) == 2
        names = [r["name"] for r in rows]
        assert "Alice" in names
        assert "Bob" in names
        reader.close()

    def test_query_empty_result(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        rows = reader.query("SELECT * FROM users WHERE id = :id", params={"id": 999})
        assert rows == []
        reader.close()

    def test_query_without_table(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        rows = reader.query("SELECT * FROM users")
        assert len(rows) == 3
        reader.close()

    def test_query_with_table_set(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        rows = reader.query("SELECT name FROM users WHERE id = 1")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        reader.close()

    def test_query_triggers_initialization(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        assert not reader._initialized
        reader.query("SELECT 1")
        assert reader._initialized
        reader.close()

    def test_query_invalid_sql_raises(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        with pytest.raises(QueryError, match="Failed to execute query"):
            reader.query("INVALID SQL STATEMENT")
        reader.close()

    def test_query_without_params(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        rows = reader.query("SELECT COUNT(*) AS cnt FROM users")
        assert len(rows) == 1
        assert cast(int, rows[0]["cnt"]) == 3
        reader.close()


class TestDbReaderLifecycle:
    def test_close_resets_state(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        reader.get(pk={"id": 1})
        assert reader._initialized
        reader.close()
        assert not reader._initialized
        assert reader._engine is None
        assert reader._table is None

    def test_close_without_init_is_noop(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        reader.close()
        assert not reader._initialized

    def test_close_twice_is_idempotent(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        reader.get(pk={"id": 1})
        reader.close()
        reader.close()
        assert not reader._initialized

    def test_get_after_close_reinitializes(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        reader.get(pk={"id": 1})
        reader.close()
        row = reader.get(pk={"id": 2})
        assert row is not None
        assert row["name"] == "Bob"
        assert reader._initialized
        reader.close()

    def test_query_after_close_reinitializes(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        reader.query("SELECT 1")
        reader.close()
        rows = reader.query("SELECT * FROM users")
        assert len(rows) == 3
        assert reader._initialized
        reader.close()

    def test_context_manager_enter_returns_self(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with reader as ctx:
            assert ctx is reader

    def test_context_manager_exit_calls_close(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with reader:
            reader.get(pk={"id": 1})
            assert reader._initialized
        assert not reader._initialized

    def test_context_manager_with_exception_still_closes(self, users_url: str) -> None:
        reader = DbReader(url=users_url, table="users")
        with pytest.raises(RuntimeError, match="test error"):
            with reader:
                reader.get(pk={"id": 1})
                raise RuntimeError("test error")
        assert not reader._initialized

    def test_non_owned_engine_not_disposed(self, users_url: str) -> None:
        provider = EngineProvider()
        reader = DbReader(url=users_url, table="users", engine_provider=provider)
        reader.get(pk={"id": 1})
        assert not reader._owns_engine
        reader.close()
        engine = provider.get_engine(DbConfig(connection_url=users_url))
        assert engine is not None
        provider.dispose_all()

    def test_engine_provider_uses_get_engine(self, users_url: str) -> None:
        provider = EngineProvider()
        reader = DbReader(url=users_url, table="users", engine_provider=provider)
        reader.get(pk={"id": 1})
        config = DbConfig(connection_url=users_url)
        shared_engine = provider.get_engine(config)
        assert reader._engine is shared_engine
        reader.close()
        provider.dispose_all()


class TestDbReaderErrorMapping:
    def test_engine_creation_failure_raises_connection_error(self) -> None:
        reader = DbReader(url="sqlite:///nonexistent/path/db.sqlite", table="users")
        with patch(
            "azure_functions_db.binding.reader.create_engine",
            side_effect=Exception("connection failed"),
        ):
            with pytest.raises(DbConnectionError, match="Failed to create database engine"):
                reader.get(pk={"id": 1})

    def test_table_reflection_failure_raises_configuration_error(
        self, users_url: str
    ) -> None:
        reader = DbReader(url=users_url, table="nonexistent_table")
        with pytest.raises(ConfigurationError, match="Failed to reflect table"):
            reader.get(pk={"id": 1})

    def test_engine_disposes_on_reflection_failure(self) -> None:
        reader = DbReader(url="sqlite:///:memory:", table="users")
        engine_mock = Mock()
        with patch(
            "azure_functions_db.binding.reader.create_engine",
            return_value=engine_mock,
        ):
            with pytest.raises(ConfigurationError):
                reader._ensure_initialized()
        engine_mock.dispose.assert_called_once()
        assert reader._engine is None
        assert not reader._initialized

    def test_query_execution_error_raises_query_error(self, users_url: str) -> None:
        reader = DbReader(url=users_url)
        with pytest.raises(QueryError, match="Failed to execute query"):
            reader.query("SELECT * FROM nonexistent_table_xyz")
        reader.close()

    def test_get_on_nonexistent_table_raises_configuration_error(
        self, users_url: str
    ) -> None:
        reader = DbReader(url=users_url, table="missing_table")
        with pytest.raises(ConfigurationError, match="Failed to reflect table"):
            reader.get(pk={"id": 1})
        reader.close()
