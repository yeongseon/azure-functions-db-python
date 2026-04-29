from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import logging
from types import TracebackType
from typing import Any

from sqlalchemy.engine import Connection, Engine, create_engine
from sqlalchemy.engine.base import Transaction
from sqlalchemy.schema import Table
from sqlalchemy.sql import and_, delete, update

from ..core.config import DbConfig, resolve_env_vars
from ..core.engine import EngineProvider
from ..core.errors import ConfigurationError, DbConnectionError, WriteError
from ..core.metadata import get_metadata_cache

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
        self._tx_conn: Connection | None = None
        self._closed_in_active_tx = False
        self._tx: Transaction | None = None

    @contextmanager
    def transaction(self) -> Iterator[DbWriter]:
        """Group multiple write operations into a single SQL transaction.

        Inside the ``with`` block, every call to ``insert``, ``insert_many``,
        ``upsert``, ``upsert_many``, ``update``, and ``delete`` executes on
        the same connection and shares one transaction.  The transaction is
        committed on normal exit and rolled back if any exception leaves
        the block.

        Nested ``transaction()`` calls on the same writer are not supported
        and raise :class:`WriteError`.

        Example
        -------
        >>> with writer.transaction():
        ...     writer.insert(data={"id": 1, "status": "pending"})
        ...     writer.update(data={"status": "shipped"}, pk={"id": 1})

        Raises
        ------
        WriteError
            If a transaction is already active on this writer, if the
            connection cannot be acquired, or if commit fails.  The
            original exception is preserved as the cause when applicable.
        """
        if self._tx_conn is not None:
            msg = (
                "transaction() is already active on this writer; "
                "nested transactions are not supported"
            )
            raise WriteError(msg)

        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101

        try:
            conn = self._engine.connect()
        except Exception as exc:
            msg = "Failed to acquire connection for transaction"
            raise WriteError(msg) from exc

        tx = conn.begin()
        self._tx_conn = conn
        self._tx = tx
        self._closed_in_active_tx = False
        try:
            yield self
        except BaseException:
            if self._tx is None:
                raise
            try:
                tx.rollback()
            finally:
                self._tx = None
                self._tx_conn = None
                conn.close()
            raise
        else:
            if self._tx is None:
                return
            try:
                tx.commit()
            except Exception as exc:
                self._tx = None
                self._tx_conn = None
                conn.close()
                msg = "Failed to commit transaction"
                raise WriteError(msg) from exc
            self._tx = None
            self._tx_conn = None
            conn.close()

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
            with self._execution_scope() as conn:
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
            with self._execution_scope() as conn:
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
            with self._execution_scope() as conn:
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
            with self._execution_scope() as conn:
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
            with self._execution_scope() as conn:
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
            with self._execution_scope() as conn:
                conn.execute(stmt)
        except (ConfigurationError, WriteError):
            raise
        except Exception as exc:
            msg = "Failed to delete row"
            raise WriteError(msg) from exc

    def close(self) -> None:
        """Tear down resources held by this writer.

        If called while a ``transaction()`` ``with`` block is still active,
        the transaction is rolled back, the connection is released, and a
        sentinel is set so that any subsequent write call inside the same
        ``with`` block raises :class:`WriteError`. The surrounding context
        manager's ``__exit__`` observes the teardown and skips its own
        commit/rollback. Rollback failures are logged at WARNING and do
        not prevent connection close or engine disposal.

        After ``close()``, do not continue using the writer inside the
        original ``transaction()`` block — start a new ``with`` block on
        a fresh writer instance.
        """
        had_active_tx = self._tx is not None
        if self._tx is not None:
            tx = self._tx
            self._tx = None
            try:
                tx.rollback()
            except Exception:
                logger.warning(
                    "Failed to roll back active transaction on writer.close()",
                    exc_info=True,
                )
        if self._tx_conn is not None:
            tx_conn = self._tx_conn
            self._tx_conn = None
            try:
                tx_conn.close()
            except Exception:
                logger.warning(
                    "Failed to close active transaction connection on writer.close()",
                    exc_info=True,
                )
        if self._engine is not None:
            if self._owns_engine:
                self._engine.dispose()
            self._engine = None
            self._table = None
            self._initialized = False
            self._owns_engine = False
        if had_active_tx:
            self._closed_in_active_tx = True

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

    @contextmanager
    def _execution_scope(self) -> Iterator[Connection]:
        """Yield a connection for a single write operation.

        Inside ``transaction()`` the writer reuses the active transactional
        connection so all operations share one commit/rollback boundary.
        Outside, each operation gets its own short-lived ``engine.begin()``
        transaction, preserving the previous behavior.
        """
        assert self._engine is not None  # noqa: S101  # nosec B101

        if self._tx_conn is not None:
            yield self._tx_conn
            return

        with self._engine.begin() as conn:
            yield conn

    def _ensure_initialized(self) -> None:
        if self._closed_in_active_tx:
            msg = (
                "DbWriter was close()d while a transaction() block was still "
                "active; further writes inside that block are rejected. Exit "
                "the transaction() block and use a fresh writer instance."
            )
            raise WriteError(msg)
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
        update_columns = {k: v for k, v in data.items() if k not in conflict_columns}

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
            return self._build_pg_upsert(data, conflict_columns, update_columns)
        return self._build_sqlite_upsert(data, conflict_columns, update_columns)

    def _build_pg_upsert(
        self,
        data: dict[str, object],
        conflict_columns: list[str],
        update_columns: dict[str, object],
    ) -> Any:
        assert self._table is not None  # noqa: S101  # nosec B101

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

    def _build_sqlite_upsert(
        self,
        data: dict[str, object],
        conflict_columns: list[str],
        update_columns: dict[str, object],
    ) -> Any:
        assert self._table is not None  # noqa: S101  # nosec B101

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
        else:
            first_col = list(data.keys())[0]
            stmt = stmt.on_duplicate_key_update(**{first_col: stmt.inserted[first_col]})
        return stmt
