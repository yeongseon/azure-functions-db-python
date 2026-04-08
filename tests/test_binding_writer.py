from __future__ import annotations

import os
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select

from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.config import DbConfig
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.core.errors import ConfigurationError, DbConnectionError, WriteError


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
    )
    metadata.create_all(engine)
    engine.dispose()
    return url


def _create_orders_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "orders",
        metadata,
        Column("order_id", Integer, primary_key=True),
        Column("item_id", Integer, primary_key=True),
        Column("qty", Integer),
    )
    metadata.create_all(engine)
    engine.dispose()
    return url


def _create_no_pk_db(db_path: Path) -> str:
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
    engine.dispose()
    return url


def _create_strict_users_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50), nullable=False),
        Column("email", String(100), nullable=False),
    )
    metadata.create_all(engine)
    engine.dispose()
    return url


def _read_all(url: str, table_name: str) -> list[dict[str, object]]:
    engine = create_engine(url)
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    tbl = metadata.tables[table_name]
    with engine.connect() as conn:
        result = conn.execute(select(tbl))
        rows = [dict(row._mapping) for row in result]
    engine.dispose()
    return rows


@pytest.fixture()
def users_url(tmp_path: Path) -> str:
    return _create_users_db(tmp_path / "users.db")


@pytest.fixture()
def orders_url(tmp_path: Path) -> str:
    return _create_orders_db(tmp_path / "orders.db")


@pytest.fixture()
def no_pk_url(tmp_path: Path) -> str:
    return _create_no_pk_db(tmp_path / "no_pk.db")


@pytest.fixture()
def strict_users_url(tmp_path: Path) -> str:
    return _create_strict_users_db(tmp_path / "strict_users.db")


class TestDbWriterConstructor:
    def test_valid(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        assert writer._table_name == "users"
        writer.close()

    def test_with_schema(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users", schema="main")
        assert writer._schema == "main"
        writer.close()

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="url must not be empty"):
            DbWriter(url="", table="users")

    def test_empty_table_raises(self, users_url: str) -> None:
        with pytest.raises(ConfigurationError, match="table must not be empty"):
            DbWriter(url=users_url, table="")

    def test_kw_only(self) -> None:
        with pytest.raises(TypeError):
            DbWriter("sqlite:///:memory:", "users")  # type: ignore[misc]

    def test_env_var_resolution(self) -> None:
        with patch.dict(os.environ, {"TEST_WRITER_URL": "sqlite:///:memory:"}):
            writer = DbWriter(url="%TEST_WRITER_URL%", table="t")
            assert writer._url == "sqlite:///:memory:"
            writer.close()

    def test_env_var_missing_raises(self) -> None:
        with pytest.raises(ConfigurationError, match="not set"):
            DbWriter(url="%NONEXISTENT_WRITER_VAR%", table="t")

    def test_lazy_initialization(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        assert not writer._initialized
        writer.close()

    def test_engine_provider_accepted(self, users_url: str) -> None:
        provider = EngineProvider()
        writer = DbWriter(url=users_url, table="users", engine_provider=provider)
        assert writer._engine_provider is provider
        writer.close()
        provider.dispose_all()

    def test_db_config_created(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        assert writer._db_config.connection_url == users_url
        writer.close()


class TestDbWriterInsert:
    def test_insert_single_row(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
        rows = _read_all(users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_insert_triggers_initialization(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        assert not writer._initialized
        writer.insert(data={"id": 1, "name": "Test", "email": "t@t.com"})
        assert writer._initialized
        writer.close()

    def test_insert_duplicate_pk_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            with pytest.raises(WriteError, match="Failed to insert row"):
                writer.insert(data={"id": 1, "name": "Bob", "email": "b@b.com"})

    def test_insert_unknown_column_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="Unknown columns in data"):
                writer.insert(data={"id": 1, "nonexistent": "x"})

    def test_insert_empty_data_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="data must not be empty"):
                writer.insert(data={})


class TestDbWriterInsertMany:
    def test_insert_many_rows(self, users_url: str) -> None:
        rows_data = [
            {"id": 1, "name": "Alice", "email": "a@b.com"},
            {"id": 2, "name": "Bob", "email": "b@b.com"},
            {"id": 3, "name": "Charlie", "email": "c@b.com"},
        ]
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert_many(rows=rows_data)
        rows = _read_all(users_url, "users")
        assert len(rows) == 3

    def test_insert_many_empty_is_noop(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert_many(rows=[])
        rows = _read_all(users_url, "users")
        assert len(rows) == 0

    def test_insert_many_rollback_on_failure(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Existing", "email": "e@e.com"})
            with pytest.raises(WriteError, match="Failed to insert rows"):
                writer.insert_many(rows=[
                    {"id": 2, "name": "New", "email": "n@n.com"},
                    {"id": 1, "name": "Dup", "email": "d@d.com"},
                ])
        rows = _read_all(users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Existing"


class TestDbWriterUpsert:
    def test_upsert_insert_new(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.upsert(
                data={"id": 1, "name": "Alice", "email": "a@b.com"},
                conflict_columns=["id"],
            )
        rows = _read_all(users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_upsert_update_existing(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            writer.upsert(
                data={"id": 1, "name": "Alice Updated", "email": "new@b.com"},
                conflict_columns=["id"],
            )
        rows = _read_all(users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice Updated"
        assert rows[0]["email"] == "new@b.com"

    def test_upsert_is_idempotent(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            for _ in range(3):
                writer.upsert(
                    data={"id": 1, "name": "Alice", "email": "a@b.com"},
                    conflict_columns=["id"],
                )
        rows = _read_all(users_url, "users")
        assert len(rows) == 1

    def test_upsert_unknown_conflict_column_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="Unknown conflict columns"):
                writer.upsert(
                    data={"id": 1, "name": "Alice"},
                    conflict_columns=["nonexistent"],
                )

    def test_upsert_empty_conflict_columns_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="conflict_columns must not be empty"):
                writer.upsert(
                    data={"id": 1, "name": "Alice"},
                    conflict_columns=[],
                )

    def test_upsert_only_conflict_columns_no_update(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            writer.upsert(
                data={"id": 1},
                conflict_columns=["id"],
            )
        rows = _read_all(users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"


class TestDbWriterUpsertMany:
    def test_upsert_many_mixed(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Old", "email": "old@b.com"})
            writer.upsert_many(
                rows=[
                    {"id": 1, "name": "Updated", "email": "u@b.com"},
                    {"id": 2, "name": "New", "email": "n@b.com"},
                ],
                conflict_columns=["id"],
            )
        rows = _read_all(users_url, "users")
        assert len(rows) == 2
        by_id = {r["id"]: r for r in rows}
        assert by_id[1]["name"] == "Updated"
        assert by_id[2]["name"] == "New"

    def test_upsert_many_empty_is_noop(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.upsert_many(rows=[], conflict_columns=["id"])
        rows = _read_all(users_url, "users")
        assert len(rows) == 0

    def test_upsert_many_rollback_on_failure(self, strict_users_url: str) -> None:
        with DbWriter(url=strict_users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Keep", "email": "k@k.com"})

            with pytest.raises(WriteError, match="Failed to upsert rows"):
                writer.upsert_many(
                    rows=[
                        {"id": 2, "name": "OK", "email": "ok@ok.com"},
                        {"id": 3, "name": None, "email": "bad@b.com"},
                    ],
                    conflict_columns=["id"],
                )

        rows = _read_all(strict_users_url, "users")
        assert len(rows) == 1
        assert rows[0]["name"] == "Keep"


class TestDbWriterUpdate:
    def test_update_existing_row(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            writer.update(data={"name": "Bob"}, pk={"id": 1})
        rows = _read_all(users_url, "users")
        assert rows[0]["name"] == "Bob"
        assert rows[0]["email"] == "a@b.com"

    def test_update_nonexistent_is_noop(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.update(data={"name": "Ghost"}, pk={"id": 999})
        rows = _read_all(users_url, "users")
        assert len(rows) == 0

    def test_update_non_pk_column_in_pk_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            with pytest.raises(ConfigurationError, match="not part of the primary key"):
                writer.update(data={"name": "Bob"}, pk={"email": "a@b.com"})

    def test_update_empty_pk_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="pk must not be empty"):
                writer.update(data={"name": "Bob"}, pk={})

    def test_update_empty_data_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="data must not be empty"):
                writer.update(data={}, pk={"id": 1})

    def test_update_composite_pk(self, orders_url: str) -> None:
        with DbWriter(url=orders_url, table="orders") as writer:
            writer.insert(data={"order_id": 1, "item_id": 1, "qty": 5})
            writer.update(data={"qty": 10}, pk={"order_id": 1, "item_id": 1})
        rows = _read_all(orders_url, "orders")
        assert cast(int, rows[0]["qty"]) == 10

    def test_update_partial_composite_pk_raises(self, orders_url: str) -> None:
        with DbWriter(url=orders_url, table="orders") as writer:
            writer.insert(data={"order_id": 1, "item_id": 1, "qty": 5})
            with pytest.raises(ConfigurationError, match="Incomplete primary key"):
                writer.update(data={"qty": 10}, pk={"order_id": 1})


class TestDbWriterDelete:
    def test_delete_existing_row(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.insert(data={"id": 1, "name": "Alice", "email": "a@b.com"})
            writer.delete(pk={"id": 1})
        rows = _read_all(users_url, "users")
        assert len(rows) == 0

    def test_delete_nonexistent_is_noop(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            writer.delete(pk={"id": 999})
        rows = _read_all(users_url, "users")
        assert len(rows) == 0

    def test_delete_non_pk_column_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="not part of the primary key"):
                writer.delete(pk={"name": "Alice"})

    def test_delete_empty_pk_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(ConfigurationError, match="pk must not be empty"):
                writer.delete(pk={})

    def test_delete_composite_pk(self, orders_url: str) -> None:
        with DbWriter(url=orders_url, table="orders") as writer:
            writer.insert(data={"order_id": 1, "item_id": 1, "qty": 5})
            writer.insert(data={"order_id": 1, "item_id": 2, "qty": 3})
            writer.delete(pk={"order_id": 1, "item_id": 1})
        rows = _read_all(orders_url, "orders")
        assert len(rows) == 1
        assert cast(int, rows[0]["item_id"]) == 2

    def test_delete_partial_composite_pk_raises(self, orders_url: str) -> None:
        with DbWriter(url=orders_url, table="orders") as writer:
            writer.insert(data={"order_id": 1, "item_id": 1, "qty": 5})
            with pytest.raises(ConfigurationError, match="Incomplete primary key"):
                writer.delete(pk={"order_id": 1})

    def test_delete_on_no_pk_table_raises(self, no_pk_url: str) -> None:
        with DbWriter(url=no_pk_url, table="events") as writer:
            with pytest.raises(ConfigurationError, match="has no primary key"):
                writer.delete(pk={"id": 1})


class TestDbWriterLifecycle:
    def test_close_resets_state(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
        assert writer._initialized
        writer.close()
        assert not writer._initialized
        assert writer._engine is None
        assert writer._table is None

    def test_close_without_init_is_noop(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.close()
        assert not writer._initialized

    def test_close_twice_is_idempotent(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
        writer.close()
        writer.close()
        assert not writer._initialized

    def test_insert_after_close_reinitializes(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
        writer.close()
        writer.insert(data={"id": 2, "name": "B", "email": "b@b.com"})
        assert writer._initialized
        writer.close()
        rows = _read_all(users_url, "users")
        assert len(rows) == 2

    def test_context_manager_enter_returns_self(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        with writer as ctx:
            assert ctx is writer

    def test_context_manager_exit_calls_close(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        with writer:
            writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
            assert writer._initialized
        assert not writer._initialized

    def test_context_manager_with_exception_still_closes(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        with pytest.raises(RuntimeError, match="test error"):
            with writer:
                writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
                raise RuntimeError("test error")
        assert not writer._initialized

    def test_non_owned_engine_not_disposed(self, users_url: str) -> None:
        provider = EngineProvider()
        writer = DbWriter(url=users_url, table="users", engine_provider=provider)
        writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
        assert not writer._owns_engine
        writer.close()
        engine = provider.get_engine(DbConfig(connection_url=users_url))
        assert engine is not None
        provider.dispose_all()

    def test_engine_provider_uses_get_engine(self, users_url: str) -> None:
        provider = EngineProvider()
        writer = DbWriter(url=users_url, table="users", engine_provider=provider)
        writer.insert(data={"id": 1, "name": "A", "email": "a@a.com"})
        config = DbConfig(connection_url=users_url)
        shared_engine = provider.get_engine(config)
        assert writer._engine is shared_engine
        writer.close()
        provider.dispose_all()


class TestDbWriterErrorMapping:
    def test_engine_creation_failure_raises_connection_error(self) -> None:
        writer = DbWriter(url="sqlite:///nonexistent/path/db.sqlite", table="users")
        with patch(
            "azure_functions_db.binding.writer.create_engine",
            side_effect=Exception("connection failed"),
        ):
            with pytest.raises(DbConnectionError, match="Failed to create database engine"):
                writer.insert(data={"id": 1})

    def test_table_reflection_failure_raises_configuration_error(
        self, users_url: str,
    ) -> None:
        writer = DbWriter(url=users_url, table="nonexistent_table")
        with pytest.raises(ConfigurationError, match="Failed to reflect table"):
            writer.insert(data={"id": 1})

    def test_engine_disposes_on_reflection_failure(self) -> None:
        writer = DbWriter(url="sqlite:///:memory:", table="users")
        engine_mock = Mock()
        with patch(
            "azure_functions_db.binding.writer.create_engine",
            return_value=engine_mock,
        ):
            with pytest.raises(ConfigurationError):
                writer._ensure_initialized()
        engine_mock.dispose.assert_called_once()
        assert writer._engine is None
        assert not writer._initialized

    def test_unsupported_upsert_dialect_raises(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer._ensure_initialized()
        assert writer._engine is not None
        with patch.object(
            writer._engine.dialect,
            "name",
            "mssql",
        ):
            with pytest.raises(ConfigurationError, match="not supported for dialect"):
                writer.upsert(
                    data={"id": 1, "name": "A"},
                    conflict_columns=["id"],
                )
        writer.close()
