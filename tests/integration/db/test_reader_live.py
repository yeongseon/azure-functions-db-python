from __future__ import annotations

import os
from pathlib import Path

import pytest

from azure_functions_db.binding.reader import DbReader

_DB_KEYS = ("sqlite", "postgres", "mysql", "mssql")
_DB_PARAMS = [pytest.param(key, marks=getattr(pytest.mark, key), id=key) for key in _DB_KEYS]
_URL_ENV = {
    "postgres": "TEST_POSTGRES_URL",
    "mysql": "TEST_MYSQL_URL",
    "mssql": "TEST_MSSQL_URL",
}


@pytest.fixture()
def db_url(db_key: str, tmp_path: Path) -> str:
    if db_key == "sqlite":
        return f"sqlite:///{tmp_path / 'reader-live.db'}"

    env_var = _URL_ENV[db_key]
    url = os.environ.get(env_var)
    if url is None:
        pytest.skip(f"{env_var} not set")
    assert url is not None
    return url


@pytest.mark.parametrize("db_key", _DB_PARAMS)
class TestDbReaderLive:
    @pytest.mark.integration
    def test_get_single_pk(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")

        row = reader.get(pk={"id": 1})

        assert row is not None
        assert row["id"] == 1
        assert row["name"] == "Alice"

        reader.close()

    @pytest.mark.integration
    def test_get_composite_pk(self, order_items_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="order_items")

        row = reader.get(pk={"order_id": 1, "item_id": 1})

        assert row is not None
        assert row["qty"] == 2

        reader.close()

    @pytest.mark.integration
    def test_get_non_existent_pk(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")

        row = reader.get(pk={"id": 999})

        assert row is None

        reader.close()

    @pytest.mark.integration
    def test_query_text_sql(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")

        rows = reader.query("SELECT * FROM users WHERE id = :id", params={"id": 1})

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

        reader.close()

    @pytest.mark.integration
    def test_query_all_rows(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")

        rows = reader.query("SELECT * FROM users")

        assert [row["id"] for row in rows] == [1, 2, 3]

        reader.close()

    @pytest.mark.integration
    def test_query_empty_result(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")

        rows = reader.query("SELECT * FROM users WHERE id = :id", params={"id": 999})

        assert rows == []

        reader.close()

    @pytest.mark.integration
    def test_close_and_context_manager(self, users_table: tuple[object, str], db_url: str) -> None:
        reader = DbReader(url=db_url, table="users")
        reader.query("SELECT * FROM users")
        reader.close()

        rows = reader.query("SELECT * FROM users WHERE id = :id", params={"id": 2})
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

        with DbReader(url=db_url, table="users") as scoped:
            scoped_rows = scoped.query("SELECT * FROM users WHERE id = :id", params={"id": 3})
            assert len(scoped_rows) == 1
            assert scoped_rows[0]["name"] == "Charlie"
