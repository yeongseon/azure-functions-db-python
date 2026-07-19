"""Shared validation helpers for database binding primary-key handling."""

from __future__ import annotations

from sqlalchemy.schema import Table

from .errors import ConfigurationError


def validate_pk_columns(table: Table, table_name: str | None, pk: dict[str, object]) -> None:
    """Validate that *pk* keys exactly match *table*'s primary key columns.

    Args:
        table: The reflected SQLAlchemy ``Table`` to validate against.
        table_name: The table name, used only for error messages.
        pk: The provided primary-key mapping to validate.

    Raises:
        ConfigurationError: If *pk* is empty, the table has no primary key, or
            the provided keys do not exactly match the primary-key columns.
    """
    if not pk:
        msg = "pk must not be empty"
        raise ConfigurationError(msg)

    pk_columns = {c.name for c in table.primary_key.columns}

    if not pk_columns:
        msg = f"Table '{table_name}' has no primary key defined"
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
