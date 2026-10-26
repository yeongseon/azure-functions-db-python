from pathlib import Path

import pytest
from sqlalchemy import create_engine

from azure_functions_db import PollTrigger, RowChange, SqlAlchemySource
from tests.test_poll_trigger import FakeStateStore


@pytest.mark.parametrize("query_mode", [False, True])
def test_late_visible_lower_cursor_is_not_revisited(tmp_path: Path, query_mode: bool) -> None:
    # Given two logical transactions which allocate cursors before visibility.
    url = f"sqlite:///{tmp_path / 'visibility.db'}"
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE events (id INTEGER PRIMARY KEY, version INTEGER)")
    pending_a = (1, 10)
    pending_b = (2, 20)
    source = SqlAlchemySource(
        url=url,
        table=None if query_mode else "events",
        query="SELECT id, version FROM events" if query_mode else None,
        cursor_column="version",
        pk_columns=["id"],
    )
    trigger = PollTrigger(name="events", source=source, checkpoint_store=FakeStateStore())
    events: list[RowChange] = []
    try:
        # When B becomes visible and is acknowledged before A commits.
        with engine.begin() as connection:
            connection.exec_driver_sql("INSERT INTO events VALUES (?, ?)", pending_b)
        assert trigger.run(timer=None, handler=events.extend) == 1
        with engine.begin() as connection:
            connection.exec_driver_sql("INSERT INTO events VALUES (?, ?)", pending_a)
        assert trigger.run(timer=None, handler=events.extend) == 0
        # Then strict cursor polling misses A; PK tiebreaking cannot recover it.
        assert [event.pk for event in events] == [{"id": 2}]
    finally:
        source.dispose()
        engine.dispose()


def test_serialized_publication_assigns_cursor_in_visibility_order(tmp_path: Path) -> None:
    # Given a relay that publishes committed work with a serialized sequence.
    url = f"sqlite:///{tmp_path / 'published.db'}"
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE events (id INTEGER PRIMARY KEY, version INTEGER)")
    source = SqlAlchemySource(url=url, table="events", cursor_column="version", pk_columns=["id"])
    trigger = PollTrigger(name="events", source=source, checkpoint_store=FakeStateStore())
    events: list[RowChange] = []
    try:
        # When publication order is B then A, with each publication committed first.
        for row in [(2, 1), (1, 2)]:
            with engine.begin() as connection:
                connection.exec_driver_sql("INSERT INTO events VALUES (?, ?)", row)
            assert trigger.run(timer=None, handler=events.extend) == 1
        # Then both changes are delivered despite their opposite business-key order.
        assert [event.pk for event in events] == [{"id": 2}, {"id": 1}]
    finally:
        source.dispose()
        engine.dispose()
