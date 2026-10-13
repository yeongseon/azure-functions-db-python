import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from azure_functions_db.core.errors import ConfigurationError
from azure_functions_db.core.validation import validate_pk_columns


def test_validate_pk_columns_rejects_empty_key() -> None:
    # Given
    table = Table("widgets", MetaData(), Column("id", Integer, primary_key=True))

    # When / Then
    with pytest.raises(ConfigurationError) as exc_info:
        validate_pk_columns(table, "widgets", {})
    assert str(exc_info.value) == "pk must not be empty"


def test_validate_pk_columns_rejects_table_without_primary_key() -> None:
    # Given
    table = Table("widgets", MetaData(), Column("name", String(20)))

    # When / Then
    with pytest.raises(ConfigurationError) as exc_info:
        validate_pk_columns(table, "widgets", {"name": "example"})
    assert str(exc_info.value) == "Table 'widgets' has no primary key defined"


def test_validate_pk_columns_reports_invalid_column_and_expected_key() -> None:
    # Given
    table = Table("widgets", MetaData(), Column("id", Integer, primary_key=True))

    # When / Then
    with pytest.raises(ConfigurationError) as exc_info:
        validate_pk_columns(table, "widgets", {"tenant_id": 7})
    assert str(exc_info.value) == (
        "Columns ['tenant_id'] are not part of the primary key. Primary key columns: ['id']"
    )


def test_validate_pk_columns_reports_missing_composite_key_column() -> None:
    # Given
    table = Table(
        "widgets",
        MetaData(),
        Column("tenant_id", Integer, primary_key=True),
        Column("widget_id", Integer, primary_key=True),
    )

    # When / Then
    with pytest.raises(ConfigurationError) as exc_info:
        validate_pk_columns(table, "widgets", {"tenant_id": 7})
    assert str(exc_info.value) == (
        "Incomplete primary key: missing columns ['widget_id']. "
        "All primary key columns required: ['tenant_id', 'widget_id']"
    )
