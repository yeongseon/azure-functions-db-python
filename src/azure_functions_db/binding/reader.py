from __future__ import annotations

import logging
from types import TracebackType
from typing import Any
import warnings

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.schema import Table
from sqlalchemy.sql import and_, select, text

from ..core.config import DbConfig, resolve_env_vars
from ..core.engine import EngineProvider
from ..core.errors import ConfigurationError, DbConnectionError, QueryError
from ..core.metadata import get_metadata_cache

logger = logging.getLogger(__name__)


class DbReader:
    """Imperative input binding for reading database rows.

    Provides ``get()`` for single-row lookup by primary key and ``query()``
    for arbitrary SQL queries.  Uses SQLAlchemy Core under the hood and
    integrates with :class:`EngineProvider` for shared connection pooling.

    Thread Safety
    -------------
    Instances are **not** safe to share across concurrent threads or
    async invocations.  Create a separate ``DbReader`` per function
    invocation, or use a ``with`` block to scope the lifecycle.

    Parameters
    ----------
    url:
        SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
    table:
        Table name for ``get()`` operations.  Optional if only ``query()``
        is used.
    schema:
        Optional schema qualifier for *table*.
    engine_provider:
        Optional shared :class:`EngineProvider`.  When provided, the reader
        uses a pooled engine instead of creating its own.
    """

    def __init__(
        self,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> None:
        if not url:
            msg = "url must not be empty"
            raise ConfigurationError(msg)

        try:
            self._url = resolve_env_vars(url)
        except ConfigurationError:
            raise

        self._table_name = table
        self._schema = schema
        self._engine_provider = engine_provider
        self._db_config = DbConfig(connection_url=self._url)

        self._engine: Engine | None = None
        self._table: Table | None = None
        self._owns_engine = False
        self._initialized = False

    def get(self, *, pk: dict[str, object]) -> dict[str, object] | None:
        """Look up a single row by primary key.

        Requires *table* to have been set in the constructor.

        Parameters
        ----------
        pk:
            Mapping of primary-key column name to value.  All keys must be
            actual primary key columns of the table.

        Returns
        -------
        dict or None
            The matching row as a dict, or ``None`` if no row matches.

        Raises
        ------
        ConfigurationError
            If *table* was not set, or *pk* contains unknown columns.
        QueryError
            If more than one row matches the provided key.
        """
        if self._table_name is None:
            msg = "get() requires 'table' to be set in DbReader constructor"
            raise ConfigurationError(msg)

        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        self._validate_pk_columns(pk)

        try:
            conditions = [self._table.c[col] == val for col, val in pk.items()]
            stmt: Any = select(self._table).where(and_(*conditions)).limit(2)

            with self._engine.connect() as conn:
                result = conn.execute(stmt)
                rows = [dict(row._mapping) for row in result]
        except (ConfigurationError, QueryError):
            raise
        except Exception as exc:
            msg = "Failed to execute get() query"
            raise QueryError(msg) from exc

        if len(rows) == 0:
            return None

        if len(rows) > 1:
            msg = f"get() expected at most 1 row but found multiple matches for pk={pk}"
            raise QueryError(msg)

        return rows[0]

    def query(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        """Execute a raw SQL query and return all matching rows.

        Always use ``:name`` parameter placeholders and *params* instead of
        string formatting to prevent SQL injection.  True read-only
        enforcement should be done at the database role/permission level.

        Parameters
        ----------
        sql:
            SQL query string.  Use ``:name`` placeholders for parameters.
        params:
            Optional mapping of parameter names to values.

        Returns
        -------
        list[dict]
            List of rows, each as a dict.  Empty list if no rows match.

        Raises
        ------
        QueryError
            If the query execution fails.
        """
        if params is None:
            warnings.warn(
                "query() called without params: verify that sql does not"
                " concatenate user input. Use :name placeholders and pass"
                " params=\"{'name': value}\" to prevent SQL injection.",
                UserWarning,
                stacklevel=2,
            )
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101

        try:
            stmt = text(sql)
            with self._engine.connect() as conn:
                if params:
                    result = conn.execute(stmt, params)
                else:
                    result = conn.execute(stmt)
                return [dict(row._mapping) for row in result]
        except (ConfigurationError, QueryError):
            raise
        except Exception as exc:
            msg = "Failed to execute query"
            raise QueryError(msg) from exc

    def scalar(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> object | None:
        """Execute a SQL query and return a single scalar value.

        Convenience wrapper around :meth:`query` for queries such as
        ``SELECT COUNT(*) FROM ...`` or ``SELECT MAX(updated_at) FROM ...``.

        Returns the first column of the first row, or ``None`` if no rows
        match.  Raises :class:`QueryError` if the query returns more than
        one row.

        Parameters
        ----------
        sql:
            SQL query string.  Use ``:name`` placeholders for parameters.
        params:
            Optional mapping of parameter names to values.

        Returns
        -------
        object or None
            The single scalar value, or ``None`` if no rows match.

        Raises
        ------
        QueryError
            If the query execution fails or returns multiple rows.
        """
        if params is None:
            warnings.warn(
                "scalar() called without params: verify that sql does not"
                " concatenate user input. Use :name placeholders and pass"
                " params=\"{'name': value}\" to prevent SQL injection.",
                UserWarning,
                stacklevel=2,
            )
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101

        try:
            stmt = text(sql)
            with self._engine.connect() as conn:
                if params:
                    result = conn.execute(stmt, params)
                else:
                    result = conn.execute(stmt)
                rows = result.fetchmany(2)
        except (ConfigurationError, QueryError):
            raise
        except Exception as exc:
            msg = "Failed to execute scalar query"
            raise QueryError(msg) from exc

        if not rows:
            return None
        if len(rows) > 1:
            msg = "scalar() expected at most 1 row but the query returned multiple rows"
            raise QueryError(msg)
        # First column of the single row.
        first_row = rows[0]
        if hasattr(first_row, "_mapping"):
            mapping: dict[str, object] = dict(first_row._mapping)
            if not mapping:
                return None
            value: object = next(iter(mapping.values()))
            return value
        # Defensive fallback: row is a plain tuple-like.
        if len(first_row):
            fallback: object = first_row[0]
            return fallback
        return None

    def one(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Execute a SQL query and return exactly one row.

        Convenience wrapper around :meth:`query` for queries that must
        return a single row.

        Parameters
        ----------
        sql:
            SQL query string.  Use ``:name`` placeholders for parameters.
        params:
            Optional mapping of parameter names to values.

        Returns
        -------
        dict
            The single matching row as a dict.

        Raises
        ------
        QueryError
            If the query execution fails, returns zero rows, or returns
            more than one row.
        """
        rows = self._fetch_at_most_two(sql, params)
        if not rows:
            msg = "one() expected exactly 1 row but the query returned no rows"
            raise QueryError(msg)
        if len(rows) > 1:
            msg = "one() expected exactly 1 row but the query returned multiple rows"
            raise QueryError(msg)
        return rows[0]

    def one_or_none(
        self,
        sql: str,
        *,
        params: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        """Execute a SQL query and return one row or ``None``.

        Convenience wrapper around :meth:`query` for queries that must
        return zero or one row.

        Parameters
        ----------
        sql:
            SQL query string.  Use ``:name`` placeholders for parameters.
        params:
            Optional mapping of parameter names to values.

        Returns
        -------
        dict or None
            The single matching row as a dict, or ``None`` if no rows
            match.

        Raises
        ------
        QueryError
            If the query execution fails or returns more than one row.
        """
        rows = self._fetch_at_most_two(sql, params)
        if not rows:
            return None
        if len(rows) > 1:
            msg = "one_or_none() expected at most 1 row but the query returned multiple rows"
            raise QueryError(msg)
        return rows[0]

    def _fetch_at_most_two(
        self,
        sql: str,
        params: dict[str, object] | None,
    ) -> list[dict[str, object]]:
        """Execute *sql* and return up to two rows as dicts."""
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101

        try:
            stmt = text(sql)
            with self._engine.connect() as conn:
                if params:
                    result = conn.execute(stmt, params)
                else:
                    result = conn.execute(stmt)
                rows = result.fetchmany(2)
        except (ConfigurationError, QueryError):
            raise
        except Exception as exc:
            msg = "Failed to execute query"
            raise QueryError(msg) from exc

        return [dict(row._mapping) for row in rows]

    def close(self) -> None:
        """Release resources held by this reader.

        If the reader owns its engine (no ``engine_provider`` was given),
        the engine is disposed.  If using a shared engine via
        ``engine_provider``, only the reader's internal state is reset.
        """
        if self._engine is not None:
            if self._owns_engine:
                self._engine.dispose()
            self._engine = None
            self._table = None
            self._initialized = False
            self._owns_engine = False

    def __enter__(self) -> DbReader:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_initialized(self) -> None:
        """Create engine and optionally reflect table metadata on first use."""
        if self._initialized:
            return

        try:
            if self._engine_provider is None:
                self._engine = create_engine(self._url)
                self._owns_engine = True
            else:
                self._engine = self._engine_provider.get_engine(self._db_config)
                self._owns_engine = False
        except Exception as exc:
            msg = "Failed to create database engine"
            raise DbConnectionError(msg) from exc

        try:
            if self._table_name:
                self._reflect_table()
        except Exception:
            if self._engine is not None and self._owns_engine:
                self._engine.dispose()
            self._engine = None
            self._table = None
            self._owns_engine = False
            raise

        self._initialized = True

    def _reflect_table(self) -> None:
        """Reflect table metadata and cache the Table object."""
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table_name is not None  # noqa: S101  # nosec B101

        cache = get_metadata_cache()
        try:
            table = cache.get_or_reflect(
                engine=self._engine,
                url=self._url,
                schema=self._schema,
                table_name=self._table_name,
            )
        except Exception as exc:
            msg = f"Failed to reflect table '{self._table_name}'"
            raise ConfigurationError(msg) from exc

        key = f"{self._schema}.{self._table_name}" if self._schema else self._table_name
        if table is None:
            msg = f"Table '{key}' not found in database"
            raise ConfigurationError(msg)

        self._table = table

    def _validate_pk_columns(self, pk: dict[str, object]) -> None:
        """Validate that *pk* keys exactly match the table's primary key columns."""
        assert self._table is not None  # noqa: S101  # nosec B101

        if not pk:
            msg = "pk must not be empty"
            raise ConfigurationError(msg)

        pk_columns = {c.name for c in self._table.primary_key.columns}

        if not pk_columns:
            msg = f"Table '{self._table_name}' has no primary key defined"
            raise ConfigurationError(msg)

        provided = set(pk.keys())

        invalid = provided - pk_columns
        if invalid:
            msg = (
                f"Columns {sorted(invalid)} are not part of the primary key. "
                f"Primary key columns: {sorted(pk_columns)}"
            )
            raise ConfigurationError(msg)

        missing = pk_columns - provided
        if missing:
            msg = (
                f"Incomplete primary key: missing columns {sorted(missing)}. "
                f"All primary key columns required: {sorted(pk_columns)}"
            )
            raise ConfigurationError(msg)
