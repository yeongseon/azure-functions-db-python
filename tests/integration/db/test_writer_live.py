from __future__ import annotations

from collections.abc import Generator
import os
from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.engine import Engine

from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.errors import ConfigurationError, WriteError


def _db_param(db: str) -> object:
    return pytest.param(
        db,
        marks=[pytest.mark.integration, getattr(pytest.mark, db)],
        id=db,
    )


_WRITER_DBS = [_db_param("sqlite"), _db_param("postgres"), _db_param("mysql"), _db_param("mssql")]
_UPSERT_DBS = [_db_param("sqlite"), _db_param("postgres"), _db_param("mysql")]


def _create_users_table(metadata: MetaData) -> None:
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("email", String(100)),
    )


def _resolve_url(db_key: str, tmp_path: Path, filename: str) -> str:
    if db_key == "sqlite":
        return f"sqlite:///{tmp_path / filename}"
    env_var = f"TEST_{db_key.upper()}_URL"
    url = os.environ.get(env_var)
    if url is None:
        pytest.skip(f"{env_var} not set")
    return url


def _read_all(engine: Engine, table_name: str) -> list[dict[str, object]]:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))  # noqa: S608
        return [dict(row._mapping) for row in result]


def _rows_by_id(engine: Engine) -> dict[int, dict[str, object]]:
    return {int(str(row["id"])): row for row in _read_all(engine, "users")}


@pytest.fixture(params=_WRITER_DBS)
def writer_db(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Generator[tuple[str, Engine, str], None, None]:
    db_key = request.param
    url = _resolve_url(db_key, tmp_path, "test_writer.db")

    engine = create_engine(url)
    metadata = MetaData()
    _create_users_table(metadata)
    metadata.create_all(engine)

    try:
        yield url, engine, db_key
    finally:
        metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture(params=_UPSERT_DBS)
def upsert_db(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Generator[tuple[str, Engine, str], None, None]:
    db_key = request.param
    url = _resolve_url(db_key, tmp_path, "test_writer_upsert.db")

    engine = create_engine(url)
    metadata = MetaData()
    _create_users_table(metadata)
    metadata.create_all(engine)

    try:
        yield url, engine, db_key
    finally:
        metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture()
def mssql_db(tmp_path: Path) -> Generator[tuple[str, Engine], None, None]:
    url = _resolve_url("mssql", tmp_path, "test_writer_mssql.db")

    engine = create_engine(url)
    metadata = MetaData()
    _create_users_table(metadata)
    metadata.create_all(engine)

    try:
        yield url, engine
    finally:
        metadata.drop_all(engine)
        engine.dispose()


@pytest.mark.integration
def test_insert_single_row(writer_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = writer_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 10, "name": "Test", "email": "t@t.com"})

    rows = _read_all(engine, "users")
    assert len(rows) == 1
    assert rows[0]["id"] == 10
    assert rows[0]["name"] == "Test"
    assert rows[0]["email"] == "t@t.com"


@pytest.mark.integration
def test_insert_many(writer_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = writer_db
    rows_to_insert = [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
    ]

    with DbWriter(url=url, table="users") as writer:
        writer.insert_many(rows=rows_to_insert)

    by_id = _rows_by_id(engine)
    assert set(by_id) == {1, 2, 3}
    assert by_id[1]["name"] == "Alice"
    assert by_id[2]["name"] == "Bob"
    assert by_id[3]["name"] == "Charlie"


@pytest.mark.integration
def test_update(writer_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = writer_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 10, "name": "Before", "email": "before@example.com"})
        writer.update(data={"name": "After"}, pk={"id": 10})

    rows = _read_all(engine, "users")
    assert len(rows) == 1
    assert rows[0]["name"] == "After"
    assert rows[0]["email"] == "before@example.com"


@pytest.mark.integration
def test_delete(writer_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = writer_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 10, "name": "DeleteMe", "email": "delete@example.com"})
        writer.delete(pk={"id": 10})

    assert _read_all(engine, "users") == []


@pytest.mark.integration
def test_delete_nonexistent(writer_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = writer_db
    with DbWriter(url=url, table="users") as writer:
        writer.delete(pk={"id": 9999})

    assert _read_all(engine, "users") == []


@pytest.mark.integration
def test_insert_duplicate_pk(writer_db: tuple[str, Engine, str]) -> None:
    url, _engine, _db_key = writer_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 10, "name": "First", "email": "first@example.com"})
        with pytest.raises(WriteError, match="Failed to insert row"):
            writer.insert(data={"id": 10, "name": "Second", "email": "second@example.com"})


@pytest.mark.integration
def test_upsert_insert(upsert_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = upsert_db
    with DbWriter(url=url, table="users") as writer:
        writer.upsert(
            data={"id": 20, "name": "Inserted", "email": "inserted@example.com"},
            conflict_columns=["id"],
        )

    rows = _read_all(engine, "users")
    assert len(rows) == 1
    assert rows[0]["id"] == 20
    assert rows[0]["name"] == "Inserted"


@pytest.mark.integration
def test_upsert_update(upsert_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = upsert_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 20, "name": "Before", "email": "before@example.com"})
        writer.upsert(
            data={"id": 20, "name": "After", "email": "after@example.com"},
            conflict_columns=["id"],
        )

    rows = _read_all(engine, "users")
    assert len(rows) == 1
    assert rows[0]["name"] == "After"
    assert rows[0]["email"] == "after@example.com"


@pytest.mark.integration
def test_upsert_many(upsert_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = upsert_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 1, "name": "Old One", "email": "old1@example.com"})
        writer.insert(data={"id": 2, "name": "Old Two", "email": "old2@example.com"})
        writer.upsert_many(
            rows=[
                {"id": 1, "name": "New One", "email": "new1@example.com"},
                {"id": 2, "name": "New Two", "email": "new2@example.com"},
                {"id": 3, "name": "New Three", "email": "new3@example.com"},
            ],
            conflict_columns=["id"],
        )

    by_id = _rows_by_id(engine)
    assert set(by_id) == {1, 2, 3}
    assert by_id[1]["name"] == "New One"
    assert by_id[2]["email"] == "new2@example.com"
    assert by_id[3]["name"] == "New Three"


@pytest.mark.integration
def test_upsert_partial_columns(upsert_db: tuple[str, Engine, str]) -> None:
    url, engine, _db_key = upsert_db
    with DbWriter(url=url, table="users") as writer:
        writer.insert(data={"id": 7, "name": "Initial", "email": "keep@example.com"})
        writer.upsert(
            data={"id": 7, "name": "Changed"},
            conflict_columns=["id"],
        )

    rows = _read_all(engine, "users")
    assert len(rows) == 1
    assert rows[0]["name"] == "Changed"
    assert rows[0]["email"] == "keep@example.com"


@pytest.mark.integration
@pytest.mark.mssql
def test_upsert_mssql_raises_write_error(mssql_db: tuple[str, Engine]) -> None:
    url, _engine = mssql_db
    with DbWriter(url=url, table="users") as writer:
        with pytest.raises(WriteError, match="Upsert is not supported for dialect 'mssql'"):
            writer.upsert(
                data={"id": 1, "name": "Nope", "email": "nope@example.com"},
                conflict_columns=["id"],
            )
