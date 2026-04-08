from __future__ import annotations

import logging
from types import TracebackType
from typing import Any

from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import and_, delete, update

from ..core.config import DbConfig, resolve_env_vars
from ..core.engine import EngineProvider
from ..core.errors import ConfigurationError, DbConnectionError, WriteError

logger = logging.getLogger(__name__)

_UPSERT_DIALECTS = frozenset({"postgresql", "sqlite", "mysql"})


class DbWriter:
    """Imperative output binding for writing database rows.

    Provides ``insert()``, ``upsert()``, ``update()``, ``delete()`` for
    single-row operations and ``insert_many()`` / ``upsert_many()`` for
    batch operations.  Uses SQLAlchemy Core under the hood.

    Thread Safety
    -------------
    Instances are **not** safe to share across concurrent threads or
    async invocations.  Create a separate ``DbWriter`` per function
    invocation, or use a ``with`` block to scope the lifecycle.
    """

    def __init__(
        self,
        *,
        url: str,
        table: str,
        schema: str | None = None,
        engine_provider: EngineProvider | None = None,
    ) -> None:
        if not url:
            msg = "url must not be empty"
            raise ConfigurationError(msg)

        if not table:
            msg = "table must not be empty"
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

    def insert(self, *, data: dict[str, object]) -> None:
        """Insert a single row.

        Raises :class:`WriteError` on constraint violations or other
        database errors.
        """
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        self._validate_data_columns(data)

        try:
            with self._engine.begin() as conn:
                conn.execute(self._table.insert().values(**data))
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to insert row"
            raise WriteError(msg) from exc

    def insert_many(self, *, rows: list[dict[str, object]]) -> None:
        """Insert multiple rows in a single transaction (all-or-nothing)."""
        if not rows:
            return

        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        for row in rows:
            self._validate_data_columns(row)

        try:
            with self._engine.begin() as conn:
                conn.execute(self._table.insert(), rows)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to insert rows"
            raise WriteError(msg) from exc

    def upsert(
        self,
        *,
        data: dict[str, object],
        conflict_columns: list[str],
    ) -> None:
        """Insert or update a single row using dialect-specific upsert.

        Supported dialects: PostgreSQL, SQLite, MySQL.  Other dialects
        raise :class:`ConfigurationError`.
        """
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        self._validate_data_columns(data)
        self._validate_conflict_columns(conflict_columns)

        try:
            stmt = self._build_upsert_stmt(data, conflict_columns)
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to upsert row"
            raise WriteError(msg) from exc

    def upsert_many(
        self,
        *,
        rows: list[dict[str, object]],
        conflict_columns: list[str],
    ) -> None:
        """Upsert multiple rows in a single transaction (all-or-nothing)."""
        if not rows:
            return

        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        for row in rows:
            self._validate_data_columns(row)
        self._validate_conflict_columns(conflict_columns)

        try:
            with self._engine.begin() as conn:
                for row in rows:
                    stmt = self._build_upsert_stmt(row, conflict_columns)
                    conn.execute(stmt)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to upsert rows"
            raise WriteError(msg) from exc

    def update(self, *, data: dict[str, object], pk: dict[str, object]) -> None:
        """Update a single row identified by primary key.

        This is a no-op if no row matches the given *pk* (idempotent).
        """
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        self._validate_data_columns(data)
        self._validate_pk_columns(pk)

        try:
            conditions = [self._table.c[col] == val for col, val in pk.items()]
            stmt: Any = update(self._table).where(and_(*conditions)).values(**data)
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to update row"
            raise WriteError(msg) from exc

    def delete(self, *, pk: dict[str, object]) -> None:
        """Delete a single row identified by primary key.

        This is a no-op if no row matches the given *pk* (idempotent).
        """
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        self._validate_pk_columns(pk)

        try:
            conditions = [self._table.c[col] == val for col, val in pk.items()]
            stmt: Any = delete(self._table).where(and_(*conditions))
            with self._engine.begin() as conn:
                conn.execute(stmt)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to delete row"
            raise WriteError(msg) from exc

    def close(self) -> None:
        """Release resources held by this writer."""
        if self._engine is not None:
            if self._owns_engine:
                self._engine.dispose()
            self._engine = None
            self._table = None
            self._initialized = False
            self._owns_engine = False

    def __enter__(self) -> DbWriter:
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
        assert self._engine is not None  # noqa: S101  # nosec B101

        metadata = MetaData()
        try:
            metadata.reflect(
                bind=self._engine,
                schema=self._schema,
                only=[self._table_name],
            )
        except Exception as exc:
            msg = f"Failed to reflect table '{self._table_name}'"
            raise ConfigurationError(msg) from exc

        key = (
            f"{self._schema}.{self._table_name}"
            if self._schema
            else self._table_name
        )
        if key not in metadata.tables:
            msg = f"Table '{key}' not found in database"
            raise ConfigurationError(msg)

        self._table = metadata.tables[key]

    def _validate_data_columns(self, data: dict[str, object]) -> None:
        assert self._table is not None  # noqa: S101  # nosec B101

        if not data:
            msg = "data must not be empty"
            raise ConfigurationError(msg)

        table_columns = {c.name for c in self._table.columns}
        unknown = set(data.keys()) - table_columns
        if unknown:
            msg = f"Unknown columns in data: {sorted(unknown)}"
            raise ConfigurationError(msg)

    def _validate_pk_columns(self, pk: dict[str, object]) -> None:
        """Validate that *pk* keys are actual primary key columns of the table."""
        assert self._table is not None  # noqa: S101  # nosec B101

        if not pk:
            msg = "pk must not be empty"
            raise ConfigurationError(msg)

        pk_columns = {c.name for c in self._table.primary_key.columns}

        if not pk_columns:
            msg = f"Table '{self._table_name}' has no primary key defined"
            raise ConfigurationError(msg)

        invalid = set(pk.keys()) - pk_columns
        if invalid:
            msg = (
                f"Columns {sorted(invalid)} are not part of the primary key. "
                f"Primary key columns: {sorted(pk_columns)}"
            )
            raise ConfigurationError(msg)

    def _validate_conflict_columns(self, conflict_columns: list[str]) -> None:
        assert self._table is not None  # noqa: S101  # nosec B101

        if not conflict_columns:
            msg = "conflict_columns must not be empty"
            raise ConfigurationError(msg)

        table_columns = {c.name for c in self._table.columns}
        unknown = set(conflict_columns) - table_columns
        if unknown:
            msg = f"Unknown conflict columns: {sorted(unknown)}"
            raise ConfigurationError(msg)

    def _build_upsert_stmt(
        self,
        data: dict[str, object],
        conflict_columns: list[str],
    ) -> Any:
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        dialect = self._engine.dialect.name
        update_columns = {
            k: v for k, v in data.items() if k not in conflict_columns
        }

        if dialect in ("postgresql", "sqlite"):
            return self._build_pg_sqlite_upsert(data, conflict_columns, update_columns)
        if dialect == "mysql":
            return self._build_mysql_upsert(data, update_columns)

        msg = (
            f"Upsert is not supported for dialect '{dialect}'. "
            f"Supported dialects: {sorted(_UPSERT_DIALECTS)}"
        )
        raise ConfigurationError(msg)

    def _build_pg_sqlite_upsert(
        self,
        data: dict[str, object],
        conflict_columns: list[str],
        update_columns: dict[str, object],
    ) -> Any:
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table is not None  # noqa: S101  # nosec B101

        dialect = self._engine.dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            stmt = pg_insert(self._table).values(**data)
            if update_columns:
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_={col: stmt.excluded[col] for col in update_columns},
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
            return stmt

        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = sqlite_insert(self._table).values(**data)
        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_={col: stmt.excluded[col] for col in update_columns},
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
        return stmt

    def _build_mysql_upsert(
        self,
        data: dict[str, object],
        update_columns: dict[str, object],
    ) -> Any:
        assert self._table is not None  # noqa: S101  # nosec B101

        from sqlalchemy.dialects.mysql import insert as mysql_insert

        stmt = mysql_insert(self._table).values(**data)
        if update_columns:
            stmt = stmt.on_duplicate_key_update(
                **{col: stmt.inserted[col] for col in update_columns}
            )
        return stmt
