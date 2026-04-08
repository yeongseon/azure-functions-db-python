from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Column, Index, Integer, MetaData, String, Table, create_engine, insert

from azure_functions_db.adapter.sqlalchemy import SqlAlchemySource, _resolve_env_vars
from azure_functions_db.core.types import SourceDescriptor
from azure_functions_db.trigger.errors import FetchError, SourceConfigurationError
from azure_functions_db.trigger.runner import SourceAdapter


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
                {"id": 2, "name": "Bob", "updated_at": 100},
                {"id": 3, "name": "Charlie", "updated_at": 200},
                {"id": 4, "name": "Diana", "updated_at": 200},
                {"id": 5, "name": "Eve", "updated_at": 300},
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
        Column("updated_at", Integer),
        Column("qty", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["order_items"]),
            [
                {"order_id": 1, "item_id": 1, "updated_at": 100, "qty": 2},
                {"order_id": 1, "item_id": 2, "updated_at": 100, "qty": 3},
                {"order_id": 2, "item_id": 1, "updated_at": 200, "qty": 1},
                {"order_id": 2, "item_id": 2, "updated_at": 200, "qty": 5},
                {"order_id": 3, "item_id": 1, "updated_at": 300, "qty": 4},
            ],
        )
    engine.dispose()
    return url


def _create_orders_db_with_cursor_index(db_path: Path, *, indexed: bool) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    orders = Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("updated_at", Integer),
    )
    if indexed:
        Index("ix_orders_updated_at", orders.c.updated_at)
    metadata.create_all(engine)
    engine.dispose()
    return url


@pytest.fixture()
def orders_url(tmp_path: Path) -> str:
    return _create_orders_db(tmp_path / "orders.db")


@pytest.fixture()
def composite_pk_url(tmp_path: Path) -> str:
    return _create_order_items_db(tmp_path / "order_items.db")


def _make_source(
    url: str = "sqlite:///:memory:",
    *,
    table: str | None = "orders",
    query: str | None = None,
    cursor_column: str = "updated_at",
    pk_columns: list[str] | None = None,
    **kwargs: Any,
) -> SqlAlchemySource:
    return SqlAlchemySource(
        url=url,
        table=table,
        query=query,
        cursor_column=cursor_column,
        pk_columns=["id"] if pk_columns is None else pk_columns,
        **kwargs,
    )


class TestSqlAlchemySourceConstructor:
    def test_valid_table_mode(self) -> None:
        src = _make_source()
        assert src.source_descriptor.kind == "sqlalchemy"
        assert src.source_descriptor.name == "orders"

    def test_valid_query_mode(self) -> None:
        src = _make_source(table=None, query="SELECT * FROM orders")
        assert src.source_descriptor.kind == "sqlalchemy"
        assert src.source_descriptor.name.startswith("query_")

    def test_empty_url_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="url must not be empty"):
            _make_source(url="")

    def test_both_table_and_query_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="not both"):
            _make_source(table="orders", query="SELECT 1")

    def test_neither_table_nor_query_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="must be provided"):
            _make_source(table=None, query=None)

    def test_empty_cursor_column_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="cursor_column"):
            _make_source(cursor_column="")

    def test_empty_pk_columns_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="pk_columns"):
            _make_source(pk_columns=[])

    def test_unsupported_strategy_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="Unsupported strategy"):
            _make_source(strategy="snapshot")

    def test_unsupported_operation_mode_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="Unsupported operation_mode"):
            _make_source(operation_mode="delete_only")

    def test_kw_only(self) -> None:
        with pytest.raises(TypeError):
            SqlAlchemySource(  # type: ignore[misc]
                "sqlite:///:memory:",
                "orders",
                None,
                None,
                "updated_at",
                ["id"],
            )

    def test_env_var_resolution(self) -> None:
        with patch.dict(os.environ, {"TEST_DB_URL": "sqlite:///:memory:"}):
            src = _make_source(url="%TEST_DB_URL%")
            assert src.source_descriptor.kind == "sqlalchemy"

    def test_env_var_missing_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="not set"):
            _make_source(url="%NONEXISTENT_VAR_12345%")

    def test_env_var_partial_pattern_not_resolved(self) -> None:
        src = _make_source(url="sqlite:///:memory:")
        assert src.source_descriptor is not None

    def test_fingerprint_deterministic(self) -> None:
        src1 = _make_source()
        src2 = _make_source()
        assert src1.source_descriptor.fingerprint == src2.source_descriptor.fingerprint

    def test_fingerprint_changes_with_config(self) -> None:
        src1 = _make_source()
        src2 = _make_source(cursor_column="id")
        assert src1.source_descriptor.fingerprint != src2.source_descriptor.fingerprint

    def test_fingerprint_strips_password(self) -> None:
        src1 = _make_source(url="sqlite:///:memory:", table="orders")
        src2 = _make_source(url="sqlite:///:memory:", table="orders")
        assert src1.source_descriptor.fingerprint == src2.source_descriptor.fingerprint

    def test_source_descriptor_returns_source_descriptor(self) -> None:
        src = _make_source()
        desc = src.source_descriptor
        assert isinstance(desc, SourceDescriptor)
        assert desc.kind == "sqlalchemy"
        assert desc.name == "orders"
        assert len(desc.fingerprint) == 64

    def test_cursor_column_property(self) -> None:
        src = _make_source(cursor_column="version")
        assert src.cursor_column == "version"

    def test_pk_columns_property(self) -> None:
        src = _make_source(pk_columns=["id", "tenant_id"])
        assert src.pk_columns == ["id", "tenant_id"]

    def test_pk_columns_returns_copy(self) -> None:
        src = _make_source(pk_columns=["id", "tenant_id"])
        pk_columns = src.pk_columns
        pk_columns.append("other")
        assert src.pk_columns == ["id", "tenant_id"]

    def test_query_mode_name_uses_hash(self) -> None:
        src = _make_source(table=None, query="SELECT id, updated_at FROM orders")
        assert src.source_descriptor.name.startswith("query_")
        assert len(src.source_descriptor.name) == len("query_") + 12

    def test_invalid_url_raises_source_config_error(self) -> None:
        with pytest.raises(SourceConfigurationError, match="Invalid database URL"):
            _make_source(url="not-a-valid-url-at-all")


class TestResolveEnvVars:
    def test_no_pattern_returns_value(self) -> None:
        assert _resolve_env_vars("sqlite:///:memory:") == "sqlite:///:memory:"

    def test_resolves_env_var(self) -> None:
        with patch.dict(os.environ, {"MY_URL": "postgres://host/db"}):
            assert _resolve_env_vars("%MY_URL%") == "postgres://host/db"

    def test_missing_env_var_raises(self) -> None:
        with pytest.raises(SourceConfigurationError, match="not set"):
            _resolve_env_vars("%MISSING_VAR_XYZ%")

    def test_partial_pattern_not_resolved(self) -> None:
        assert _resolve_env_vars("prefix_%VAR%") == "prefix_%VAR%"

    def test_embedded_pattern_not_resolved(self) -> None:
        assert _resolve_env_vars("sqlite:///%DB%/extra") == "sqlite:///%DB%/extra"


class TestSqlAlchemySourceFetch:
    def test_fetch_all_no_cursor(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=None, batch_size=10)
        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[-1]["name"] == "Eve"

    def test_fetch_with_batch_size(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=None, batch_size=3)
        assert len(rows) == 3

    def test_fetch_with_cursor_single_pk(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=(100, 2), batch_size=10)
        assert len(rows) == 3
        assert rows[0]["name"] == "Charlie"

    def test_fetch_returns_ordered_by_cursor_then_pk(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=None, batch_size=10)
        updated_ats = [(r["updated_at"], r["id"]) for r in rows]
        assert updated_ats == sorted(updated_ats)

    def test_fetch_empty_when_past_end(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=(300, 5), batch_size=10)
        assert len(rows) == 0

    def test_fetch_cursor_skips_equal_rows(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=(100, 1), batch_size=10)
        ids = [r["id"] for r in rows]
        assert 1 not in ids
        assert 2 in ids

    def test_fetch_composite_pk_no_cursor(self, composite_pk_url: str) -> None:
        src = SqlAlchemySource(
            url=composite_pk_url,
            table="order_items",
            cursor_column="updated_at",
            pk_columns=["order_id", "item_id"],
        )
        rows = src.fetch(cursor=None, batch_size=10)
        assert len(rows) == 5

    def test_fetch_composite_pk_with_cursor(self, composite_pk_url: str) -> None:
        src = SqlAlchemySource(
            url=composite_pk_url,
            table="order_items",
            cursor_column="updated_at",
            pk_columns=["order_id", "item_id"],
        )
        rows = src.fetch(cursor=(100, 1, 1), batch_size=10)
        assert len(rows) == 4
        first = rows[0]
        assert first["order_id"] == 1
        assert first["item_id"] == 2

    def test_fetch_cursor_wrong_length_raises(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        with pytest.raises(FetchError, match="expected 2"):
            src.fetch(cursor=(1, 2, 3), batch_size=10)

    def test_fetch_with_where_clause(self, orders_url: str) -> None:
        src = _make_source(url=orders_url, where="name != 'Eve'")
        rows = src.fetch(cursor=None, batch_size=10)
        names = [r["name"] for r in rows]
        assert "Eve" not in names
        assert len(rows) == 4

    def test_fetch_with_parameterized_where(self, orders_url: str) -> None:
        src = _make_source(
            url=orders_url,
            where="name != :excluded_name",
            parameters={"excluded_name": "Eve"},
        )
        rows = src.fetch(cursor=None, batch_size=10)
        names = [r["name"] for r in rows]
        assert "Eve" not in names
        assert len(rows) == 4

    def test_fetch_query_mode(self, orders_url: str) -> None:
        src = SqlAlchemySource(
            url=orders_url,
            table=None,
            query="SELECT id, name, updated_at FROM orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        rows = src.fetch(cursor=None, batch_size=10)
        assert len(rows) == 5

    def test_fetch_query_mode_preserves_all_columns(self, orders_url: str) -> None:
        src = SqlAlchemySource(
            url=orders_url,
            table=None,
            query="SELECT id, name, updated_at FROM orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        rows = src.fetch(cursor=None, batch_size=10)
        assert "name" in rows[0]
        assert "id" in rows[0]
        assert "updated_at" in rows[0]
        assert rows[0]["name"] == "Alice"

    def test_fetch_query_mode_with_cursor(self, orders_url: str) -> None:
        src = SqlAlchemySource(
            url=orders_url,
            table=None,
            query="SELECT id, name, updated_at FROM orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )
        rows = src.fetch(cursor=(200, 3), batch_size=10)
        assert len(rows) == 2
        assert rows[0]["id"] == 4

    def test_fetch_query_mode_with_parameters(self, orders_url: str) -> None:
        src = SqlAlchemySource(
            url=orders_url,
            table=None,
            query="SELECT id, name, updated_at FROM orders WHERE name != :excluded",
            cursor_column="updated_at",
            pk_columns=["id"],
            parameters={"excluded": "Eve"},
        )
        rows = src.fetch(cursor=None, batch_size=10)
        assert len(rows) == 4
        names = [r["name"] for r in rows]
        assert "Eve" not in names

    def test_fetch_lazy_initialization(self) -> None:
        src = _make_source()
        assert not src._initialized

    def test_fetch_triggers_initialization(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        assert not src._initialized
        src.fetch(cursor=None, batch_size=1)
        assert src._initialized

    def test_fetch_scalar_cursor_single_pk(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        rows = src.fetch(cursor=(200, 3), batch_size=10)
        assert len(rows) == 2

    def test_fetch_missing_column_raises(self, orders_url: str) -> None:
        src = _make_source(url=orders_url, cursor_column="nonexistent")
        with pytest.raises(SourceConfigurationError, match="Columns not found"):
            src.fetch(cursor=None, batch_size=10)

    def test_cursor_column_index_warning_when_no_index(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        url = _create_orders_db_with_cursor_index(tmp_path / "orders-no-index.db", indexed=False)
        src = _make_source(url=url)

        with caplog.at_level(logging.WARNING, logger="azure_functions_db.adapter.sqlalchemy"):
            src.fetch(cursor=None, batch_size=10)

        assert "cursor_column 'updated_at' on table 'orders' is not indexed" in caplog.text

    def test_no_cursor_column_index_warning_when_indexed(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        url = _create_orders_db_with_cursor_index(tmp_path / "orders-indexed.db", indexed=True)
        src = _make_source(url=url)

        with caplog.at_level(logging.WARNING, logger="azure_functions_db.adapter.sqlalchemy"):
            src.fetch(cursor=None, batch_size=10)

        assert "cursor_column 'updated_at' on table 'orders' is not indexed" not in caplog.text


class TestSqlAlchemySourceProtocol:
    def test_implements_source_adapter(self) -> None:
        src = _make_source()
        assert isinstance(src, SourceAdapter)

    def test_source_adapter_structural(self) -> None:
        src = _make_source()
        assert hasattr(src, "source_descriptor")
        assert hasattr(src, "fetch")
        desc = src.source_descriptor
        assert isinstance(desc, SourceDescriptor)


class TestSqlAlchemySourceDispose:
    def test_dispose_resets_state(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        src.fetch(cursor=None, batch_size=1)
        assert src._initialized
        src.dispose()
        assert not src._initialized
        assert src._engine is None
        assert src._table is None

    def test_dispose_without_init_is_noop(self) -> None:
        src = _make_source()
        src.dispose()
        assert not src._initialized

    def test_can_fetch_after_dispose(self, orders_url: str) -> None:
        src = _make_source(url=orders_url)
        src.fetch(cursor=None, batch_size=1)
        src.dispose()
        rows = src.fetch(cursor=None, batch_size=10)
        assert len(rows) == 5

    def test_ensure_initialized_disposes_engine_after_reflect_failure(self) -> None:
        src = _make_source(url="sqlite:///tmp/test.db")
        engine_first = Mock()
        engine_second = Mock()

        with (
            patch(
                "azure_functions_db.adapter.sqlalchemy.create_engine",
                side_effect=[engine_first, engine_second],
            ),
            patch.object(
                SqlAlchemySource,
                "_reflect_table",
                side_effect=[SourceConfigurationError("boom"), None],
            ),
        ):
            with pytest.raises(SourceConfigurationError, match="boom"):
                src._ensure_initialized()

            engine_first.dispose.assert_called_once_with()
            assert src._engine is None
            assert not src._initialized

            src._ensure_initialized()

        assert src._engine is engine_second
        assert src._initialized
