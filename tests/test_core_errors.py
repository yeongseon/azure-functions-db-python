import pytest

from azure_functions_db.core.errors import (
    DbConnectionError,
    DbError,
    NotFoundError,
    QueryError,
    WriteError,
)


class TestDbErrorHierarchy:
    def test_db_error_is_exception(self) -> None:
        assert issubclass(DbError, Exception)

    def test_connection_error_inherits_db_error(self) -> None:
        assert issubclass(DbConnectionError, DbError)

    def test_query_error_inherits_db_error(self) -> None:
        assert issubclass(QueryError, DbError)

    def test_write_error_inherits_db_error(self) -> None:
        assert issubclass(WriteError, DbError)

    def test_not_found_error_inherits_db_error(self) -> None:
        assert issubclass(NotFoundError, DbError)

    def test_catch_db_error_catches_subtypes(self) -> None:
        with pytest.raises(DbError):
            raise DbConnectionError("connection failed")

    def test_error_message(self) -> None:
        err = QueryError("bad query")
        assert str(err) == "bad query"
