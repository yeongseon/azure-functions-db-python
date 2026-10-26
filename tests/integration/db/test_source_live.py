from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path

import pytest
from sqlalchemy import Column, DateTime, Integer, MetaData, Table, create_engine, insert

from azure_functions_db.adapter.sqlalchemy import SqlAlchemySource
from azure_functions_db.core.types import SourceDescriptor
from azure_functions_db.trigger.errors import SourceConfigurationError
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.poll import PollTrigger
from tests.test_poll_trigger import FakeStateStore

_DB_KEYS = ("sqlite", "postgres", "mysql", "mssql")
_DB_PARAMS = [pytest.param(key, marks=getattr(pytest.mark, key), id=key) for key in _DB_KEYS]
_URL_ENV = {
    "postgres": "TEST_POSTGRES_URL",
    "mysql": "TEST_MYSQL_URL",
    "mssql": "TEST_MSSQL_URL",
}


@pytest.fixture()
def db_url(db_key: str, tmp_path: Path) -> str:
    if db_key == "sqlite":
        return f"sqlite:///{tmp_path / 'source-live.db'}"

    env_var = _URL_ENV[db_key]
    url = os.environ.get(env_var)
    if url is None:
        pytest.skip(f"{env_var} not set")
    assert url is not None
    return url


@pytest.mark.parametrize("db_key", _DB_PARAMS)
class TestSqlAlchemySourceLive:
    @pytest.mark.integration
    @pytest.mark.parametrize("query_mode", [False, True])
    def test_persisted_datetime_cursor_resumes_fresh_source(
        self, db_url: str, db_key: str, tmp_path: Path, query_mode: bool
    ) -> None:
        # Given tied typed timestamps and composite primary keys on the selected backend.
        engine = create_engine(db_url)
        metadata = MetaData()
        table = Table(
            "persisted_cursor_events",
            metadata,
            Column("tenant", Integer, primary_key=True),
            Column("id", Integer, primary_key=True),
            Column("occurred_at", DateTime, nullable=False),
        )
        metadata.create_all(engine)
        early = datetime(2026, 10, 16, 10)
        late = datetime(2026, 10, 16, 11)
        with engine.begin() as connection:
            connection.execute(
                insert(table),
                [
                    {"tenant": 1, "id": 1, "occurred_at": early},
                    {"tenant": 1, "id": 2, "occurred_at": early},
                    {"tenant": 2, "id": 1, "occurred_at": early},
                    {"tenant": 2, "id": 2, "occurred_at": late},
                ],
            )
        checkpoint_file = tmp_path / "checkpoint.json"
        delivered: list[RowChange] = []
        positions: list[list[object]] = []
        try:
            # When each one-row batch is persisted as JSON and reloaded into fresh objects.
            for _ in range(5):
                store = FakeStateStore()
                if checkpoint_file.exists():
                    store.checkpoints["events"] = json.loads(checkpoint_file.read_text())
                source = SqlAlchemySource(
                    url=db_url,
                    table=None if query_mode else table.name,
                    query=f"SELECT tenant, id, occurred_at FROM {table.name}"
                    if query_mode
                    else None,
                    cursor_column="occurred_at",
                    pk_columns=["tenant", "id"],
                    cursor_types=[datetime, int, int] if query_mode else None,
                )
                try:
                    trigger = PollTrigger(
                        name="events", source=source, checkpoint_store=store, batch_size=1
                    )
                    count = trigger.run(timer=None, handler=delivered.extend)
                    checkpoint_file.write_text(json.dumps(store.checkpoints["events"]))
                    cursor = json.loads(checkpoint_file.read_text())["cursor"]
                    positions.append([datetime.fromisoformat(cursor[0]), *cursor[1:]])
                    assert count == (1 if len(positions) <= 4 else 0)
                finally:
                    source.dispose()
            # Then each exact ID and persisted resume position survives restarts, including the tie.
            assert [(event.pk["tenant"], event.pk["id"]) for event in delivered] == [
                (1, 1),
                (1, 2),
                (2, 1),
                (2, 2),
            ]
            assert positions == [
                [early, 1, 1],
                [early, 1, 2],
                [early, 2, 1],
                [late, 2, 2],
                [late, 2, 2],
            ]
        finally:
            metadata.drop_all(engine)
            engine.dispose()

    @pytest.mark.integration
    def test_datetime_cursor_round_trip(self, db_url: str, db_key: str) -> None:
        engine = create_engine(db_url)
        metadata = MetaData()
        events = Table(
            "typed_cursor_events",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("occurred_at", DateTime, nullable=False),
        )
        metadata.drop_all(engine, tables=[events], checkfirst=True)
        metadata.create_all(engine, tables=[events])
        with engine.begin() as connection:
            connection.execute(
                insert(events),
                [
                    {"id": 1, "occurred_at": datetime(2026, 10, 16, 10)},
                    {"id": 2, "occurred_at": datetime(2026, 10, 16, 10)},
                    {"id": 3, "occurred_at": datetime(2026, 10, 16, 11)},
                ],
            )
        source = SqlAlchemySource(
            url=db_url,
            table="typed_cursor_events",
            cursor_column="occurred_at",
            pk_columns=["id"],
        )
        cursor = None
        seen: list[int] = []
        for _ in range(4):
            rows = source.fetch(cursor=cursor, batch_size=1)
            if not rows:
                break
            event_id = rows[0]["id"]
            occurred_at = rows[0]["occurred_at"]
            assert isinstance(event_id, int)
            assert isinstance(occurred_at, datetime)
            seen.append(event_id)
            cursor = (occurred_at.isoformat(), event_id)
        source.dispose()
        metadata.drop_all(engine, tables=[events])
        engine.dispose()

        assert seen == [1, 2, 3]

    @pytest.mark.integration
    def test_table_reflection(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        rows = source.fetch(cursor=None, batch_size=2)
        assert len(rows) == 2
        assert rows[0]["id"] == 1

    @pytest.mark.integration
    def test_composite_pk(self, order_items_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="order_items",
            cursor_column="qty",
            pk_columns=["order_id", "item_id"],
        )

        rows = source.fetch(cursor=None, batch_size=10)
        assert len(rows) == 3
        assert rows[0]["order_id"] == 2
        assert rows[0]["item_id"] == 1

    @pytest.mark.integration
    def test_fetch_all(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        rows = source.fetch(cursor=None, batch_size=10)
        assert [row["id"] for row in rows] == [1, 2, 3, 4, 5]

    @pytest.mark.integration
    def test_fetch_with_cursor(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        rows = source.fetch(cursor=(100, 2), batch_size=10)
        assert [row["id"] for row in rows] == [3, 4, 5]

    @pytest.mark.integration
    def test_source_descriptor(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        descriptor = source.source_descriptor
        assert isinstance(descriptor, SourceDescriptor)
        assert descriptor.kind == "sqlalchemy"
        assert descriptor.name == "orders"
        assert len(descriptor.fingerprint) == 64

    @pytest.mark.integration
    def test_dispose(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="orders",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        first = source.fetch(cursor=None, batch_size=1)
        source.dispose()
        second = source.fetch(cursor=None, batch_size=10)

        assert len(first) == 1
        assert len(second) == 5

    @pytest.mark.integration
    def test_non_existent_table(self, orders_table: tuple[object, str], db_url: str) -> None:
        source = SqlAlchemySource(
            url=db_url,
            table="missing_table",
            cursor_column="updated_at",
            pk_columns=["id"],
        )

        with pytest.raises(SourceConfigurationError):
            source.fetch(cursor=None, batch_size=10)
