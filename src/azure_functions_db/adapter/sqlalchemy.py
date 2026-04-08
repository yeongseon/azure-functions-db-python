from __future__ import annotations

from collections.abc import Sequence
import hashlib
import json
import logging
import os
import re
from typing import Any

from sqlalchemy import MetaData, Table, and_, create_engine, literal_column, or_, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from azure_functions_db.core.types import CursorValue, SourceDescriptor
from azure_functions_db.trigger.errors import FetchError, SourceConfigurationError

RawRecord = dict[str, object]

logger = logging.getLogger(__name__)

_ENV_VAR_PATTERN = re.compile(r"^%(\w+)%$")


def _resolve_env_vars(value: str) -> str:
    """Resolve ``%VAR%`` whole-string pattern to environment variable value."""
    match = _ENV_VAR_PATTERN.match(value)
    if match is None:
        return value
    var_name = match.group(1)
    resolved = os.environ.get(var_name)
    if resolved is None:
        msg = f"Environment variable '{var_name}' is not set"
        raise SourceConfigurationError(msg)
    return resolved


class SqlAlchemySource:
    """SQLAlchemy-based source adapter for cursor-based change polling.

    Implements the ``SourceAdapter`` protocol defined in
    ``azure_functions_db.trigger.runner``.

    Parameters
    ----------
    url:
        SQLAlchemy connection URL.  Supports ``%VAR%`` env-var substitution.
    table:
        Table name to poll.  Mutually exclusive with *query*.
    schema:
        Optional schema qualifier for *table*.
    query:
        Raw SQL query to poll.  Mutually exclusive with *table*.
    cursor_column:
        Column used for cursor-based ordering.
    pk_columns:
        Primary-key column(s) for tie-breaking within the same cursor value.
    where:
        Optional extra SQL WHERE clause fragment (appended with AND).
    parameters:
        Optional bind parameters for *where* or *query*.
    strategy:
        Polling strategy.  Only ``"cursor"`` is supported.
    operation_mode:
        Operation mode.  Only ``"upsert_only"`` is supported.
    """

    def __init__(
        self,
        *,
        url: str,
        table: str | None = None,
        schema: str | None = None,
        query: str | None = None,
        cursor_column: str,
        pk_columns: list[str],
        where: str | None = None,
        parameters: dict[str, object] | None = None,
        strategy: str = "cursor",
        operation_mode: str = "upsert_only",
    ) -> None:
        if not url:
            msg = "url must not be empty"
            raise SourceConfigurationError(msg)
        if table and query:
            msg = "Exactly one of 'table' or 'query' must be provided, not both"
            raise SourceConfigurationError(msg)
        if not table and not query:
            msg = "Exactly one of 'table' or 'query' must be provided"
            raise SourceConfigurationError(msg)
        if not cursor_column:
            msg = "cursor_column must not be empty"
            raise SourceConfigurationError(msg)
        if not pk_columns:
            msg = "pk_columns must not be empty"
            raise SourceConfigurationError(msg)
        if strategy != "cursor":
            msg = f"Unsupported strategy: '{strategy}'. Only 'cursor' is supported."
            raise SourceConfigurationError(msg)
        if operation_mode != "upsert_only":
            msg = (
                f"Unsupported operation_mode: '{operation_mode}'. Only 'upsert_only' is supported."
            )
            raise SourceConfigurationError(msg)

        self._url = _resolve_env_vars(url)
        self._table_name = table
        self._schema = schema
        self._query = query
        self._cursor_column = cursor_column
        self._pk_columns = list(pk_columns)
        self._where = where
        self._parameters = dict(parameters) if parameters else {}
        self._strategy = strategy
        self._operation_mode = operation_mode

        self._engine: Engine | None = None
        self._table: Table | None = None
        self._initialized = False

        self._fingerprint = self._compute_fingerprint()
        self._descriptor = SourceDescriptor(
            name=self._compute_name(),
            kind="sqlalchemy",
            fingerprint=self._fingerprint,
        )

    def _compute_name(self) -> str:
        if self._table_name:
            return self._table_name
        query_hash = hashlib.sha256(self._query.encode()).hexdigest()[:12]  # type: ignore[union-attr]
        return f"query_{query_hash}"

    def _compute_fingerprint(self) -> str:
        try:
            parsed = make_url(self._url)
            safe_url = str(parsed.set(password=None))
        except Exception as exc:
            msg = "Invalid database URL"
            raise SourceConfigurationError(msg) from exc
        config: dict[str, object] = {
            "url": safe_url,
            "schema": self._schema,
            "table": self._table_name,
            "query": self._query,
            "cursor_column": self._cursor_column,
            "pk_columns": self._pk_columns,
            "where": self._where,
            "parameters": self._parameters,
            "strategy": self._strategy,
            "operation_mode": self._operation_mode,
        }
        serialized = json.dumps(config, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()

    @property
    def source_descriptor(self) -> SourceDescriptor:
        return self._descriptor

    def _ensure_initialized(self) -> None:
        """Create engine and reflect table metadata on first use."""
        if self._initialized:
            return

        try:
            self._engine = create_engine(self._url)
        except Exception as exc:
            msg = f"Failed to create engine for source '{self._compute_name()}'"
            raise SourceConfigurationError(msg) from exc

        try:
            if self._table_name:
                self._reflect_table()
        except Exception:
            self._engine.dispose()
            self._engine = None
            self._table = None
            raise

        self._initialized = True

    def _reflect_table(self) -> None:
        """Reflect table metadata and validate that required columns exist."""
        assert self._engine is not None  # noqa: S101  # nosec B101
        assert self._table_name is not None  # noqa: S101  # nosec B101

        metadata = MetaData()
        try:
            metadata.reflect(
                bind=self._engine,
                schema=self._schema,
                only=[self._table_name],
            )
        except Exception as exc:
            msg = f"Failed to reflect table '{self._table_name}'"
            raise SourceConfigurationError(msg) from exc

        key = f"{self._schema}.{self._table_name}" if self._schema else self._table_name
        if key not in metadata.tables:
            msg = f"Table '{key}' not found in database"
            raise SourceConfigurationError(msg)

        self._table = metadata.tables[key]

        table_columns = {c.name for c in self._table.columns}
        required = {self._cursor_column, *self._pk_columns}
        missing = required - table_columns
        if missing:
            msg = f"Columns not found in table '{key}': {sorted(missing)}"
            raise SourceConfigurationError(msg)

    def fetch(self, cursor: CursorValue | None, batch_size: int) -> Sequence[RawRecord]:
        """Fetch a batch of records newer than *cursor*.

        Returns an empty sequence when no new records are available.
        """
        self._ensure_initialized()
        assert self._engine is not None  # noqa: S101  # nosec B101

        try:
            stmt = self._build_query(cursor, batch_size)
            with self._engine.connect() as conn:
                result = conn.execute(stmt, self._parameters)
                return [dict(row._mapping) for row in result]
        except (SourceConfigurationError, FetchError):
            raise
        except Exception as exc:
            msg = "Failed to fetch records"
            raise FetchError(msg) from exc

    def _build_query(self, cursor: CursorValue | None, batch_size: int) -> Any:
        """Build the SELECT statement with cursor filter, ordering, and limit."""
        if self._table_name:
            return self._build_table_query(cursor, batch_size)
        return self._build_raw_query(cursor, batch_size)

    def _build_table_query(self, cursor: CursorValue | None, batch_size: int) -> Any:
        assert self._table is not None  # noqa: S101  # nosec B101

        stmt = select(self._table)

        conditions = []
        if cursor is not None:
            cursor_filter = self._build_cursor_filter_table(cursor)
            conditions.append(cursor_filter)

        if self._where:
            conditions.append(text(self._where))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        order_cols = [self._table.c[self._cursor_column]] + [
            self._table.c[pk] for pk in self._pk_columns
        ]
        stmt = stmt.order_by(*order_cols)
        stmt = stmt.limit(batch_size)
        return stmt

    def _build_raw_query(self, cursor: CursorValue | None, batch_size: int) -> Any:
        assert self._query is not None  # noqa: S101  # nosec B101

        subq = text(self._query).columns().subquery("source")

        stmt: Any = select(literal_column("*")).select_from(subq)

        conditions = []
        if cursor is not None:
            cursor_filter = self._build_cursor_filter_subquery(cursor)
            conditions.append(cursor_filter)

        if self._where:
            conditions.append(text(self._where))

        if conditions:
            stmt = stmt.where(and_(*conditions))

        order_cols: list[Any] = [literal_column(f"source.{self._cursor_column}")] + [
            literal_column(f"source.{pk}") for pk in self._pk_columns
        ]
        stmt = stmt.order_by(*order_cols)
        stmt = stmt.limit(batch_size)
        return stmt

    def _build_cursor_filter_table(self, cursor: CursorValue) -> Any:
        """Build lexicographic cursor predicate for table mode."""
        assert self._table is not None  # noqa: S101  # nosec B101

        cols_values = self._cursor_cols_values(cursor)
        col_exprs = [self._table.c[name] for name, _ in cols_values]
        vals = [val for _, val in cols_values]
        return self._build_or_and_expansion(col_exprs, vals)

    def _build_cursor_filter_subquery(self, cursor: CursorValue) -> Any:
        """Build lexicographic cursor predicate for raw-query mode."""
        cols_values = self._cursor_cols_values(cursor)
        col_exprs: list[Any] = [literal_column(f"source.{name}") for name, _ in cols_values]
        vals = [val for _, val in cols_values]
        return self._build_or_and_expansion(col_exprs, vals)

    def _cursor_cols_values(self, cursor: CursorValue) -> list[tuple[str, object]]:
        """Map cursor value(s) to (column_name, value) pairs.

        Checkpoint contract: cursor = ``(cursor_value, *pk_values)`` tuple.
        """
        if isinstance(cursor, tuple):
            values = list(cursor)
        else:
            values = [cursor]

        expected_len = 1 + len(self._pk_columns)
        if len(values) != expected_len:
            msg = (
                f"Cursor has {len(values)} parts but expected {expected_len} "
                f"(cursor_column + {len(self._pk_columns)} pk_columns)"
            )
            raise FetchError(msg)

        names = [self._cursor_column, *self._pk_columns]
        return list(zip(names, values))

    @staticmethod
    def _build_or_and_expansion(col_exprs: list[Any], vals: list[object]) -> Any:
        """Portable lexicographic ``>`` comparison via OR/AND expansion.

        For columns ``(a, b, c)`` and values ``(va, vb, vc)``::

            (a > va)
            OR (a = va AND b > vb)
            OR (a = va AND b = vb AND c > vc)
        """
        conditions = []
        for i in range(len(col_exprs)):
            eq_parts = [col_exprs[j] == vals[j] for j in range(i)]
            gt_part = col_exprs[i] > vals[i]
            if eq_parts:
                conditions.append(and_(*eq_parts, gt_part))
            else:
                conditions.append(gt_part)
        return or_(*conditions)

    def dispose(self) -> None:
        """Dispose the underlying engine and release connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._table = None
            self._initialized = False
