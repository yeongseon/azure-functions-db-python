from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, select

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
        with DbWriter(url=users_url, table="users") as writer:
            with pytest.raises(WriteError), writer.transaction():
                writer.insert(data={"id": 1, "name": "Alice"})
                writer.insert(data={"id": 1, "name": "Bob"})
        assert _row_count(users_url) == 0


class TestTransactionNesting:
    def test_nested_transaction_raises(self, users_url: str) -> None:
        with DbWriter(url=users_url, table="users") as writer:
            with writer.transaction():
                with pytest.raises(WriteError, match="nested"):
                    with writer.transaction():
                        pass
        assert _row_count(users_url) == 0

    def test_can_open_new_transaction_after_previous_completes(
        self, users_url: str
    ) -> None:
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
