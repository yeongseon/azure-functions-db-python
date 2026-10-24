"""Shared fixtures for live database integration tests.

Environment variables
---------------------
Each database backend is gated by an env var containing a SQLAlchemy URL:

* ``TEST_SQLITE_URL``   – e.g. ``sqlite:///tmp/test.db`` (defaults to in-memory)
* ``TEST_POSTGRES_URL`` – e.g. ``postgresql+psycopg://postgres:postgres@localhost:5432/testdb``
* ``TEST_MYSQL_URL``    – e.g. ``mysql+pymysql://root:root@localhost:3306/testdb``
* ``TEST_MSSQL_URL``    – e.g. ``mssql+pyodbc://sa:Password1!@localhost:1433/testdb?driver=...``

If the env var is not set the corresponding tests are skipped automatically,
which is convenient for local runs. A CI lane that claims to verify a backend
must set ``INTEGRATION_FAIL_ON_SKIP=1`` so any skip or xfail makes the pytest run fail.
A test marked with a backend also fails if that backend's URL names a different
SQLAlchemy dialect.
"""

from __future__ import annotations

from collections.abc import Generator
import os
from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

# ---------------------------------------------------------------------------
# Dialect capability model
# ---------------------------------------------------------------------------

_UPSERT_DIALECTS = frozenset({"postgresql", "sqlite", "mysql"})


def supports_upsert(engine: Engine) -> bool:
    """Return ``True`` if the engine's dialect supports upsert."""
    return engine.dialect.name in _UPSERT_DIALECTS


def dialect_name(engine: Engine) -> str:
    """Return the dialect name (e.g. ``'postgresql'``, ``'sqlite'``)."""
    return engine.dialect.name


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------

_ENV_VARS: dict[str, str] = {
    "sqlite": "TEST_SQLITE_URL",
    "postgres": "TEST_POSTGRES_URL",
    "mysql": "TEST_MYSQL_URL",
    "mssql": "TEST_MSSQL_URL",
}

_SKIP_REASONS: dict[str, str] = {
    "sqlite": "TEST_SQLITE_URL not set",
    "postgres": "TEST_POSTGRES_URL not set",
    "mysql": "TEST_MYSQL_URL not set",
    "mssql": "TEST_MSSQL_URL not set",
}


def _get_db_url(db: str) -> str | None:
    """Return the URL for *db* or ``None`` if env var is not set."""
    if db == "sqlite":
        # SQLite always available — default to in-memory
        return os.environ.get(_ENV_VARS["sqlite"], "sqlite:///:memory:")
    return os.environ.get(_ENV_VARS[db])


# ---------------------------------------------------------------------------
# Fail-closed guards
# ---------------------------------------------------------------------------

_BACKEND_DIALECTS: dict[str, str] = {
    "sqlite": "sqlite",
    "postgres": "postgresql",
    "mysql": "mysql",
    "mssql": "mssql",
}

_skipped: list[str] = []


@pytest.fixture(autouse=True)
def _backend_url_matches_marker(request: pytest.FixtureRequest) -> None:
    """Fail if a backend-marked test would run against another dialect."""
    for backend, dialect in _BACKEND_DIALECTS.items():
        if request.node.get_closest_marker(backend) is None:
            continue
        url = os.environ.get(_ENV_VARS[backend])
        if not url:
            continue
        # Never echo the URL or the parser error: both may contain credentials.
        # Fail outside the except block so the parser error is not chained in.
        actual: str | None
        try:
            actual = make_url(url).get_backend_name()
        except Exception:  # noqa: BLE001
            actual = None
        if actual is None:
            pytest.fail(f"{_ENV_VARS[backend]} is not a valid SQLAlchemy URL", pytrace=False)
        if actual != dialect:
            pytest.fail(f"{_ENV_VARS[backend]} does not use the {dialect!r} dialect", pytrace=False)


def _record_skip(report: Any) -> None:
    # An expected failure (xfail) is reported as skipped and never proves the
    # test body passed, so a required lane treats it like any other skip.
    if report.skipped:
        _skipped.append(report.nodeid)


def pytest_collectreport(report: pytest.CollectReport) -> None:
    _record_skip(report)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    _record_skip(report)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if os.environ.get("INTEGRATION_FAIL_ON_SKIP") == "1" and _skipped and exitstatus == 0:
        print(f"\nINTEGRATION_FAIL_ON_SKIP=1: {len(_skipped)} skipped or xfailed; failing the run")
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


# ---------------------------------------------------------------------------
# Parametrize helpers
# ---------------------------------------------------------------------------

# All 4 DB keys used for parametrization.
ALL_DBS = ("sqlite", "postgres", "mysql", "mssql")


def db_params(dbs: tuple[str, ...] = ALL_DBS) -> list[Any]:
    """Build ``pytest.param`` list for multi-DB parametrization.

    Each param carries:
    * id   – dialect name
    * marks – ``pytest.mark.{dialect}`` + ``pytest.mark.integration``

    Tests using this helper are **automatically skipped** when the
    corresponding env var is not set.
    """
    params: list[Any] = []
    for db in dbs:
        url = _get_db_url(db)
        marks = [
            pytest.mark.integration,
            getattr(pytest.mark, db),
        ]
        if url is None:
            marks.append(pytest.mark.skip(reason=_SKIP_REASONS[db]))
        params.append(pytest.param(url or "skip://", db, marks=marks, id=db))
    return params


# Only DBs that support upsert.
UPSERT_DBS = ("sqlite", "postgres", "mysql")


def upsert_db_params() -> list[Any]:
    """Like :func:`db_params` but limited to upsert-capable dialects."""
    return db_params(UPSERT_DBS)


# ---------------------------------------------------------------------------
# Schema fixtures
# ---------------------------------------------------------------------------

# Standard ``users`` table used across reader/writer/source tests.
_USERS_SEED = [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"},
    {"id": 3, "name": "Charlie", "email": "charlie@example.com"},
]

# Composite PK table used for multi-column key tests.
_ORDER_ITEMS_SEED = [
    {"order_id": 1, "item_id": 1, "qty": 2},
    {"order_id": 1, "item_id": 2, "qty": 3},
    {"order_id": 2, "item_id": 1, "qty": 1},
]

# Orders table used for source / cursor tests.
_ORDERS_SEED = [
    {"id": 1, "name": "Alice", "updated_at": 100},
    {"id": 2, "name": "Bob", "updated_at": 100},
    {"id": 3, "name": "Charlie", "updated_at": 200},
    {"id": 4, "name": "Diana", "updated_at": 200},
    {"id": 5, "name": "Eve", "updated_at": 300},
]


def _create_users_table(metadata: MetaData) -> Table:
    return Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("email", String(100)),
    )


def _create_order_items_table(metadata: MetaData) -> Table:
    return Table(
        "order_items",
        metadata,
        Column("order_id", Integer, primary_key=True),
        Column("item_id", Integer, primary_key=True),
        Column("qty", Integer),
    )


def _create_orders_table(metadata: MetaData) -> Table:
    return Table(
        "orders",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
        Column("updated_at", Integer),
    )


@pytest.fixture()
def db_engine(request: pytest.FixtureRequest) -> Generator[Engine, None, None]:
    """Create a SQLAlchemy engine for the parametrized DB URL.

    Usage::

        @pytest.mark.parametrize("db_url,db_key", db_params())
        def test_something(db_engine, db_url, db_key):
            ...
    """
    db_url: str = request.getfixturevalue("db_url")
    engine = create_engine(db_url)
    yield engine
    engine.dispose()


@pytest.fixture()
def users_table(
    db_engine: Engine,
) -> Generator[tuple[Engine, str], None, None]:
    """Create, seed, and teardown the ``users`` table.

    Yields ``(engine, table_name)`` — the engine has the table ready for use.
    """
    metadata = MetaData()
    table = _create_users_table(metadata)
    metadata.create_all(db_engine)

    with db_engine.begin() as conn:
        conn.execute(insert(table), _USERS_SEED)

    yield db_engine, "users"

    metadata.drop_all(db_engine)


@pytest.fixture()
def empty_users_table(
    db_engine: Engine,
) -> Generator[tuple[Engine, str], None, None]:
    """Create the ``users`` table without seed data.

    Yields ``(engine, table_name)``.
    """
    metadata = MetaData()
    _create_users_table(metadata)
    metadata.create_all(db_engine)

    yield db_engine, "users"

    metadata.drop_all(db_engine)


@pytest.fixture()
def order_items_table(
    db_engine: Engine,
) -> Generator[tuple[Engine, str], None, None]:
    """Create, seed, and teardown the ``order_items`` (composite PK) table.

    Yields ``(engine, table_name)``.
    """
    metadata = MetaData()
    table = _create_order_items_table(metadata)
    metadata.create_all(db_engine)

    with db_engine.begin() as conn:
        conn.execute(insert(table), _ORDER_ITEMS_SEED)

    yield db_engine, "order_items"

    metadata.drop_all(db_engine)


@pytest.fixture()
def orders_table(
    db_engine: Engine,
) -> Generator[tuple[Engine, str], None, None]:
    """Create, seed, and teardown the ``orders`` table (with cursor column).

    Yields ``(engine, table_name)``.
    """
    metadata = MetaData()
    table = _create_orders_table(metadata)
    metadata.create_all(db_engine)

    with db_engine.begin() as conn:
        conn.execute(insert(table), _ORDERS_SEED)

    yield db_engine, "orders"

    metadata.drop_all(db_engine)


# ---------------------------------------------------------------------------
# Helper to read all rows back for verification
# ---------------------------------------------------------------------------


def read_all_rows(engine: Engine, table_name: str) -> list[dict[str, Any]]:
    """Read all rows from *table_name* and return as dicts."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name}"))  # noqa: S608
        return [dict(row._mapping) for row in result]
