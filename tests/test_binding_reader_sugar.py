from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, insert

from azure_functions_db.binding.reader import DbReader
from azure_functions_db.core.errors import QueryError


def _create_users_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("active", Integer),
    )
    metadata.create_all(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(metadata.tables["users"]),
            [
                {"id": 1, "name": "Alice", "active": 1},
                {"id": 2, "name": "Bob", "active": 1},
                {"id": 3, "name": "Charlie", "active": 0},
            ],
        )
    engine.dispose()
    return url


@pytest.fixture
def users_url(tmp_path: Path) -> str:
    return _create_users_db(tmp_path / "users.db")


class TestScalar:
    def test_scalar_count(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            result = reader.scalar("SELECT COUNT(*) FROM users")
        assert result == 3

    def test_scalar_with_params(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            result = reader.scalar(
                "SELECT COUNT(*) FROM users WHERE active = :active",
                params={"active": 1},
            )
        assert result == 2

    def test_scalar_returns_first_column_only(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            result = reader.scalar("SELECT name, id FROM users WHERE id = 1")
        assert result == "Alice"

    def test_scalar_duplicate_column_labels_returns_positional_first(self, users_url: str) -> None:
        """Regression (#444): scalar() converted the row to a dict before
        choosing the first value, so a duplicate SQL column label silently
        collapsed onto the later value instead of the documented first
        positional column -- a join can produce duplicate labels without an
        invalid SQL statement."""
        with DbReader(url=users_url) as reader:
            assert reader.scalar("SELECT 11 AS value, 22 AS other") == 11
            assert reader.scalar("SELECT 11 AS value, 22 AS value") == 11
            assert reader.scalar("SELECT NULL AS value, 22 AS value") is None

    def test_scalar_no_match_returns_none(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            result = reader.scalar("SELECT name FROM users WHERE id = 999")
        assert result is None

    def test_scalar_multiple_rows_raises(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError, match="multiple"):
            reader.scalar("SELECT name FROM users")

    def test_scalar_invalid_sql_raises_query_error(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError, match="scalar query"):
            reader.scalar("SELECT * FROM nonexistent_table")


class TestOne:
    def test_one_returns_single_row(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            row = reader.one("SELECT * FROM users WHERE id = 1")
        assert row["name"] == "Alice"
        assert row["id"] == 1

    def test_one_with_params(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            row = reader.one("SELECT * FROM users WHERE id = :id", params={"id": 2})
        assert row["name"] == "Bob"

    def test_one_zero_rows_raises(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError, match="no rows"):
            reader.one("SELECT * FROM users WHERE id = 999")

    def test_one_multiple_rows_raises(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError, match="multiple"):
            reader.one("SELECT * FROM users WHERE active = :active", params={"active": 1})


class TestOneOrNone:
    def test_one_or_none_returns_single_row(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            row = reader.one_or_none("SELECT * FROM users WHERE id = 1")
        assert row is not None
        assert row["name"] == "Alice"

    def test_one_or_none_zero_rows_returns_none(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader:
            row = reader.one_or_none("SELECT * FROM users WHERE id = 999")
        assert row is None

    def test_one_or_none_multiple_rows_raises(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError, match="multiple"):
            reader.one_or_none("SELECT * FROM users WHERE active = 1")

    def test_one_or_none_invalid_sql_raises(self, users_url: str) -> None:
        with DbReader(url=users_url) as reader, pytest.raises(QueryError):
            reader.one_or_none("SELECT * FROM nonexistent")
