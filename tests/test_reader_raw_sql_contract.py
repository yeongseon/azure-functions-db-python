"""Contracts for ``DbReader`` raw SQL (docs/security.md, "Raw SQL").

``query()`` and ``scalar()`` hand the SQL to the database as written: no SELECT-only rule and no
parsing. Safety rests on bound parameters and read-only database roles. These tests pin that
choice, that bound values stay data, and that the library's own errors and logs never echo
credentials (fake canaries stand in for the password, URL, SQL, parameters and driver text).
"""

from __future__ import annotations

from collections.abc import Iterator
import logging
import sqlite3
from typing import NoReturn
import warnings

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from azure_functions_db import DbConnectionError, DbReader, EngineProvider, QueryError
from azure_functions_db.core.config import DbConfig

CANARY_PASSWORD = "CANARY_SECRET_pw"
CANARY_URL = f"postgresql+psycopg://user:{CANARY_PASSWORD}@host/db"
CANARY_SQL = "CANARY_SQL_TEXT"
CANARY_PARAM = "CANARY_PARAM_VALUE"

both_methods = pytest.mark.parametrize("method", ["query", "scalar"])


@pytest.fixture
def provider() -> Iterator[EngineProvider]:
    provider = EngineProvider()
    yield provider
    provider.dispose_all()


@pytest.fixture
def library_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger="azure_functions_db")
    return caplog


def reader_over(provider: EngineProvider, *values: str) -> DbReader:
    """A reader over a fresh in-memory SQLite table ``t(value)`` holding *values*."""
    config = DbConfig(connection_url="sqlite://")
    with provider.get_engine(config).begin() as conn:
        conn.exec_driver_sql("CREATE TABLE t (value TEXT)")
        for value in values:
            conn.exec_driver_sql("INSERT INTO t VALUES (?)", (value,))
    return DbReader(db_config=config, engine_provider=provider)


def one_row(method: str, column: str, value: str) -> object:
    """The one-row result: ``query()`` returns the rows, ``scalar()`` the first column."""
    return [{column: value}] if method == "query" else value


class FailingProvider(EngineProvider):
    """Engine creation fails and the driver's message echoes the connection URL."""

    def get_engine(self, config: DbConfig) -> Engine:
        raise sqlite3.OperationalError(f"driver error: {config.connection_url}")


class RejectingProvider(EngineProvider):
    """The engine builds but every connection attempt fails, echoing the URL as some drivers do."""

    def get_engine(self, config: DbConfig) -> Engine:
        def creator() -> NoReturn:
            raise sqlite3.OperationalError(f"authentication failed: {config.connection_url}")

        return create_engine("sqlite://", creator=creator)


def assert_not_exposed(error: Exception, logs: pytest.LogCaptureFixture, *canaries: str) -> None:
    """Each canary reached the chained driver error but not the library's own text or logs."""
    driver_text = str(error.__cause__)
    exposed = f"{error} {error!r} {error.args} {logs.text}"
    for canary in canaries:
        assert canary in driver_text, "precondition: the chained driver error carries the canary"
        assert canary not in exposed


@pytest.mark.skipif(sqlite3.sqlite_version_info < (3, 35), reason="needs SQLite RETURNING")
@both_methods
def test_writes_are_not_blocked(provider: EngineProvider, method: str) -> None:
    # Given an unrestricted reader; When a write goes through the read API
    with reader_over(provider) as reader:
        result = getattr(reader, method)(
            "INSERT INTO t VALUES (:value) RETURNING value", params={"value": "written"}
        )
    # Then the database ran it: RETURNING echoes the stored row
    assert result == one_row(method, "value", "written")


@pytest.mark.parametrize(("params", "warns"), [(None, True), ({}, False)])
@both_methods
def test_warning_is_emitted_only_when_params_are_omitted(
    method: str, params: dict[str, object] | None, warns: bool
) -> None:
    # Given a reader; When SQL runs with params omitted or explicitly empty
    with DbReader(url="sqlite://") as reader, warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        getattr(reader, method)("SELECT 1", params=params)
    # Then only the omitted case is reminded to bind values
    assert any(f"{method}() called without params" in str(w.message) for w in caught) is warns


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("x' OR '1'='1", id="tautology"),
        pytest.param("'; DROP TABLE t;--", id="stacked-statement"),
        pytest.param("日本語😀é", id="non-ascii"),
        pytest.param("nul\x00byte", id="embedded-nul"),
    ],
)
@both_methods
def test_bound_values_stay_data(provider: EngineProvider, method: str, value: str) -> None:
    # Given a table holding the value and one unrelated row; When the value is bound
    with reader_over(provider, value, "other") as reader:
        result = getattr(reader, method)(
            "SELECT value FROM t WHERE value = :value", params={"value": value}
        )
    # Then it matches exactly its own row, byte for byte
    assert result == one_row(method, "value", value)


@pytest.mark.parametrize("identifier", ["value", "t; DROP TABLE t"])
@both_methods
def test_bound_identifier_shaped_values_stay_literals(
    provider: EngineProvider, method: str, identifier: str
) -> None:
    # Given a one-row table; When a value naming a column (or smuggling a statement) is bound
    with reader_over(provider, "row-data") as reader:
        result = getattr(reader, method)("SELECT :name AS name FROM t", params={"name": identifier})
    # Then it is the string itself: not the column it names, not extra SQL
    assert result == one_row(method, "name", identifier)


@both_methods
def test_sql_failure_error_omits_sql_parameters_and_driver_text(
    provider: EngineProvider, library_logs: pytest.LogCaptureFixture, method: str
) -> None:
    # Given SQL that fails in the driver (SQLAlchemy's own message carries the SQL and params)
    with reader_over(provider) as reader, pytest.raises(QueryError) as caught:
        # When it is executed
        getattr(reader, method)(
            f"SELECT * FROM {CANARY_SQL} WHERE value = :value", params={"value": CANARY_PARAM}
        )
    # Then the library's error carries neither
    assert_not_exposed(caught.value, library_logs, CANARY_SQL, CANARY_PARAM)


@both_methods
def test_connection_failure_error_omits_url_and_driver_text(
    library_logs: pytest.LogCaptureFixture, method: str
) -> None:
    # Given a URL carrying a password and a driver that echoes it when the connection fails
    with DbReader(url=CANARY_URL, engine_provider=RejectingProvider()) as reader:
        # When the connection fails during execution
        with pytest.raises(QueryError) as caught:
            getattr(reader, method)("SELECT 1", params={})
    # Then neither the password nor the URL appears in the library's error or logs
    assert_not_exposed(caught.value, library_logs, CANARY_PASSWORD, CANARY_URL)


def test_engine_creation_failure_error_omits_url_and_driver_text(
    library_logs: pytest.LogCaptureFixture,
) -> None:
    # Given a URL carrying a password and a driver that echoes it when the engine fails
    with DbReader(url=CANARY_URL, engine_provider=FailingProvider()) as reader:
        # When the first query needs the engine
        with pytest.raises(DbConnectionError) as caught:
            reader.query("SELECT 1", params={})
    # Then neither the password nor the URL appears in the library's error or logs
    assert_not_exposed(caught.value, library_logs, CANARY_PASSWORD, CANARY_URL)
