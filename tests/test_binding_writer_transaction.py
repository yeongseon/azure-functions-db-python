from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, event, select
from sqlalchemy.engine import Connection

from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.errors import WriteError


def _create_users_db(db_path: Path) -> str:
    url = f"sqlite:///{db_path}"
    engine = create_engine(url)
    metadata = MetaData()
    Table(
        "users",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(50)),
    )
    metadata.create_all(engine)
    engine.dispose()
    return url


def _row_count(url: str) -> int:
    engine = create_engine(url)
    metadata = MetaData()
    table = Table("users", metadata, autoload_with=engine)
    with engine.connect() as conn:
        rows = conn.execute(select(table)).fetchall()
    engine.dispose()
    return len(rows)


@pytest.fixture
def users_url(tmp_path: Path) -> str:
    return _create_users_db(tmp_path / "users.db")


class TestTransactionCommit:
    def test_begin_failure_returns_connection_and_preserves_cause(self, users_url: str) -> None:
        """Failed transaction entry must return the connection without hiding its cause."""
        writer = DbWriter(url=users_url, table="users")
        writer._ensure_initialized()
        assert writer._engine is not None
        captured: list[Connection] = []

        def fail_begin(connection: Connection) -> None:
            captured.append(connection)
            raise RuntimeError("injected begin failure")

        event.listen(writer._engine, "begin", fail_begin)
        try:
            with pytest.raises(WriteError, match="begin transaction") as exc_info:
                with writer.transaction():
                    pytest.fail("body must not execute")
        finally:
            event.remove(writer._engine, "begin", fail_begin)
            writer.close()

        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert captured[0].closed
        assert writer._tx is None
        assert writer._tx_conn is None

    def test_begin_and_close_failure_preserves_begin_error(self, users_url: str) -> None:
        """A cleanup failure must not replace the transaction-entry failure."""
        writer = DbWriter(url=users_url, table="users")
        writer._ensure_initialized()
        assert writer._engine is not None
        connection = MagicMock()
        connection.begin.side_effect = RuntimeError("begin failed")
        connection.close.side_effect = RuntimeError("close failed")

        with (
            patch.object(writer._engine, "connect", return_value=connection),
            pytest.raises(WriteError, match="begin transaction") as exc_info,
        ):
            with writer.transaction():
                pytest.fail("body must not execute")

        assert str(exc_info.value.__cause__) == "begin failed"
        connection.close.assert_called_once_with()

    def test_transaction_commits_on_success(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.insert(data={"id": 2, "name": "Bob"})
        assert _row_count(users_url) == 2

    def test_transaction_yields_writer_instance(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction() as tx_writer:
                assert tx_writer is writer
                tx_writer.insert(data={"id": 1, "name": "Alice"})
        assert _row_count(users_url) == 1


class TestTransactionRollback:
    def test_transaction_rolls_back_on_exception(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(RuntimeError, match="boom"), writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                raise RuntimeError("boom")
        assert _row_count(users_url) == 0

    def test_transaction_rolls_back_on_write_error(self, users_url: str) -> None:
        """A later duplicate-key failure must undo the earlier successful insert."""
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(WriteError), writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.insert(data={"id": 1, "name": "Bob"})
        assert _row_count(users_url) == 0

    def test_original_exception_preserved_when_rollback_fails(
        self,
        users_url: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Original exception must surface even when rollback() itself raises."""
        import logging
        from unittest.mock import MagicMock, patch

        writer = DbWriter(url=users_url, table="users")
        writer._ensure_initialized()

        mock_tx = MagicMock()
        mock_tx.rollback.side_effect = RuntimeError("rollback forced to fail")
        mock_conn = MagicMock()
        mock_conn.begin.return_value = mock_tx

        with caplog.at_level(logging.WARNING, logger="azure_functions_db.binding.writer"):
            with patch.object(writer._engine, "connect", return_value=mock_conn):
                with pytest.raises(RuntimeError, match="the original error"):
                    with writer.transaction():
                        raise RuntimeError("the original error")

        assert any(
            "Failed to roll back transaction on exception" in r.message for r in caplog.records
        ), "Expected rollback-failure warning in log"


class TestTransactionNesting:
    def test_nested_transaction_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                with pytest.raises(WriteError, match="nested"):
                    with writer.transaction():
                        pass
        assert _row_count(users_url) == 0

    def test_can_open_new_transaction_after_previous_completes(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
            with writer.transaction():
                writer.insert(data={"id": 2, "name": "Bob"})
        assert _row_count(users_url) == 2


class TestTransactionWithMultipleOps:
    def test_insert_update_delete_in_transaction(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.insert(data={"id": 2, "name": "Bob"})
                writer.update(data={"name": "Alicia"}, pk={"id": 1})
                writer.delete(pk={"id": 2})
        assert _row_count(users_url) == 1


class TestCloseRollsBackActiveTransaction:
    def test_close_inside_with_block_rolls_back_inserts(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.insert(data={"id": 2, "name": "Bob"})
                writer.close()

        assert _row_count(users_url) == 0

    def test_close_inside_with_block_clears_transaction_handles(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        with writer.transaction():
            writer.insert(data={"id": 1, "name": "Alice"})
            writer.close()

            assert writer._tx is None
            assert writer._tx_conn is None
            assert writer._engine is None

    def test_close_inside_with_block_does_not_raise_on_exit(self, users_url: str) -> None:
        """Context exit must not commit a transaction already rolled back by close()."""
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.close()

        assert _row_count(users_url) == 0

    def test_close_without_active_transaction_is_unaffected(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.insert(data={"id": 1, "name": "Alice"})

        writer.close()

        assert writer._tx is None
        assert writer._tx_conn is None
        assert _row_count(users_url) == 1

    def test_write_after_close_inside_transaction_is_rejected(self, users_url: str) -> None:
        """Closing a transaction must not let later writes escape into autocommit."""
        writer = DbWriter(url=users_url, table="users")
        with writer.transaction():
            writer.insert(data={"id": 1, "name": "Alice"})
            writer.close()

            with pytest.raises(WriteError, match="close.*transaction"):
                writer.insert(data={"id": 2, "name": "Bob"})

        assert _row_count(users_url) == 0

    def test_close_outside_transaction_does_not_block_reuse(self, users_url: str) -> None:
        writer = DbWriter(url=users_url, table="users")
        writer.insert(data={"id": 1, "name": "Alice"})
        writer.close()

        writer.insert(data={"id": 2, "name": "Bob"})
        writer.close()

        assert _row_count(users_url) == 2

    def test_close_logs_and_continues_when_rollback_fails(
        self,
        users_url: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        import logging
        from unittest.mock import MagicMock

        writer = DbWriter(url=users_url, table="users")
        with writer.transaction():
            writer.insert(data={"id": 1, "name": "Alice"})

            real_tx = writer._tx
            real_conn = writer._tx_conn
            assert real_tx is not None
            assert real_conn is not None

            failing_tx = MagicMock(wraps=real_tx)
            failing_tx.rollback.side_effect = RuntimeError("rollback failed")
            writer._tx = failing_tx

            with caplog.at_level(logging.WARNING, logger="azure_functions_db.binding.writer"):
                writer.close()

            failing_tx.rollback.assert_called_once()
            assert real_conn.closed
            assert any(
                "Failed to roll back active transaction" in record.message
                for record in caplog.records
            )
            assert writer._tx is None
            assert writer._tx_conn is None
            assert writer._engine is None
