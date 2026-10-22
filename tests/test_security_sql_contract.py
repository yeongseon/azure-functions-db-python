"""Offline contracts: binding values is safe; raw SQL is deliberately not a parser."""

from collections.abc import Iterator
import logging
import sqlite3

import pytest
from sqlalchemy.engine import Engine

from azure_functions_db.binding.reader import DbReader
from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.config import DbConfig
from azure_functions_db.core.engine import EngineProvider
from azure_functions_db.core.errors import DbConnectionError, QueryError

CANARY_URL = "postgresql+psycopg://user:CANARY_SECRET_pw@host/db"


@pytest.fixture
def provider() -> Iterator[EngineProvider]:
    provider = EngineProvider()
    yield provider
    provider.dispose_all()


@pytest.mark.parametrize("value", ["'; DROP TABLE t;--", "한글😀é", "nul\x00byte", "x" * 100_000])
@pytest.mark.parametrize("method", ["query", "scalar"])
def test_bound_values_remain_inert(value: str, method: str, provider: EngineProvider) -> None:
    # Given an actual SQLite table and a malicious/unusual value.
    config = DbConfig(connection_url="sqlite:///:memory:")
    engine = provider.get_engine(config)
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (value TEXT)")
    with DbWriter(db_config=config, engine_provider=provider, table="t") as writer:
        writer.insert(data={"value": value})
    with DbReader(db_config=config, engine_provider=provider) as reader:
        # When the value is bound rather than interpolated.
        result = getattr(reader, method)(
            "SELECT value FROM t WHERE value = :value", params={"value": value}
        )
    # Then the exact bytes round-trip as data, not executable SQL.
    assert result == ([{"value": value}] if method == "query" else value)


@pytest.mark.parametrize("method", ["query", "scalar"])
def test_missing_parameters_still_warn(method: str) -> None:
    # Given raw SQL without params; When executed; Then the safety warning remains.
    with DbReader(url="sqlite:///:memory:") as reader:
        with pytest.warns(UserWarning, match=rf"{method}\(\) called without params"):
            getattr(reader, method)("SELECT 1")


def test_binding_an_identifier_shaped_value_does_not_turn_it_into_sql() -> None:
    # Given an identifier-shaped attack; When bound; Then it stays a value, not a column name.
    identifier = "value FROM t; DROP TABLE t;--"
    with DbReader(url="sqlite:///:memory:") as reader:
        result = reader.scalar("SELECT :column_name", params={"column_name": identifier})
    assert result == identifier


def test_query_allows_writes_because_arbitrary_sql_is_not_a_read_only_parser(
    provider: EngineProvider,
) -> None:
    """Preserve the escape hatch: database permissions, not a SQL parser, own read-only."""
    # Given an unrestricted SQLite connection; When INSERT RETURNING uses query.
    config = DbConfig(connection_url="sqlite:///:memory:")
    with provider.get_engine(config).begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (value TEXT)")
    with DbReader(db_config=config, engine_provider=provider) as reader:
        rows = reader.query("INSERT INTO t VALUES (:value) RETURNING value", params={"value": "ok"})
    # Then the library executes the write; no new commit/autocommit promise is made.
    assert rows == [{"value": "ok"}]


@pytest.mark.parametrize("method", ["query", "scalar"])
def test_bad_sql_library_errors_hide_credentials(
    method: str, caplog: pytest.LogCaptureFixture
) -> None:
    # Given invalid SQL carrying a canary in a driver diagnostic.
    caplog.set_level(logging.DEBUG, logger="azure_functions_db")
    with DbReader(url="sqlite:///:memory:") as reader:
        # When SQL execution fails; Then library error representations/logs are safe.
        with pytest.raises(QueryError) as error:
            getattr(reader, method)(f'SELECT * FROM "{CANARY_URL}"', params={})
    assert "CANARY_SECRET_pw" not in str(error.value) + repr(error.value) + caplog.text
    assert CANARY_URL not in str(error.value) + repr(error.value) + caplog.text


@pytest.mark.parametrize("method", ["query", "scalar"])
def test_failed_auth_library_errors_hide_credentials(
    method: str, caplog: pytest.LogCaptureFixture, provider: EngineProvider
) -> None:
    # Given a narrow DBAPI connection seam (no PostgreSQL server/driver required).
    caplog.set_level(logging.DEBUG, logger="azure_functions_db")

    def failed_auth() -> sqlite3.Connection:
        raise sqlite3.OperationalError(f"authentication failed: {CANARY_URL}")

    config = DbConfig(connection_url="sqlite://", engine_kwargs={"creator": failed_auth})
    with DbReader(db_config=config, engine_provider=provider) as reader:
        # When connection acquisition fails; Then the wrapper/logs contain no credentials.
        with pytest.raises(QueryError) as error:
            getattr(reader, method)("SELECT 1", params={})
    assert "CANARY_SECRET_pw" not in str(error.value) + repr(error.value) + caplog.text
    assert CANARY_URL not in str(error.value) + repr(error.value) + caplog.text


def test_engine_creation_failure_has_safe_library_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Given a driver failure that includes the original URL.
    caplog.set_level(logging.DEBUG, logger="azure_functions_db")

    def fail(url: str) -> Engine:
        raise sqlite3.OperationalError(f"driver error: {url}")

    monkeypatch.setattr("azure_functions_db.binding.reader.create_engine", fail)
    with DbReader(url=CANARY_URL) as reader:
        # When engine creation fails; Then the library wraps it without logging the URL.
        with pytest.raises(DbConnectionError) as error:
            reader.query("SELECT 1", params={})
    assert "CANARY_SECRET_pw" not in str(error.value) + repr(error.value) + caplog.text
