from pathlib import Path

import pytest
from sqlalchemy import create_engine

from azure_functions_db import PollTrigger, RowChange, SqlAlchemySource
from azure_functions_db.core.errors import ConfigurationError
from azure_functions_db.core.types import SourceDescriptor
from azure_functions_db.trigger.normalizers import default_normalizer
from azure_functions_db.trigger.runner import PollRunner
from tests.test_poll_trigger import FakeSourceAdapterNoCursorPk, FakeStateStore


def test_explicit_default_advances_tied_composite_cursor(tmp_path: Path) -> None:
    # Given two tied rows with a composite primary key and one-row batches.
    url = f"sqlite:///{tmp_path / 'rows.db'}"
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE rows (tenant INTEGER, id INTEGER, version INTEGER, "
            "PRIMARY KEY (tenant, id))"
        )
        connection.exec_driver_sql("INSERT INTO rows VALUES (1, 1, 10), (1, 2, 10)")
    engine.dispose()
    source = SqlAlchemySource(
        url=url, table="rows", cursor_column="version", pk_columns=["tenant", "id"]
    )
    store = FakeStateStore()
    trigger = PollTrigger(
        name="rows",
        source=source,
        checkpoint_store=store,
        normalizer=default_normalizer,
        batch_size=1,
    )
    events: list[RowChange] = []
    try:
        # When separate ticks exhaust the source and another tick polls it again.
        counts = [trigger.run(timer=None, handler=events.extend) for _ in range(3)]
        # Then both rows are delivered once and the complete cursor is retained.
        assert counts == [1, 1, 0]
        assert [event.pk for event in events] == [{"tenant": 1, "id": 1}, {"tenant": 1, "id": 2}]
        assert store.checkpoints["rows"]["cursor"] == (10, 1, 2)
    finally:
        source.dispose()


def test_default_requires_source_metadata_at_construction() -> None:
    # Given a custom adapter without cursor metadata.
    source = FakeSourceAdapterNoCursorPk([])
    # When the public default is explicitly selected, then configuration fails early.
    with pytest.raises(ConfigurationError, match="make_normalizer"):
        PollTrigger(
            name="rows",
            source=source,
            checkpoint_store=FakeStateStore(),
            normalizer=default_normalizer,
        )


def test_direct_default_rejects_descriptor_without_cursor_metadata() -> None:
    # Given the public descriptor, which contains no cursor or primary-key columns.
    descriptor = SourceDescriptor(name="rows", kind="sqlalchemy", fingerprint="fp")
    # When called without source metadata, then never silently return an unusable cursor.
    with pytest.raises(ConfigurationError, match="make_normalizer"):
        default_normalizer({"id": 1}, descriptor)


def test_runner_rejects_unconfigured_default_at_construction() -> None:
    with pytest.raises(ConfigurationError, match="make_normalizer"):
        PollRunner(
            name="rows",
            source=FakeSourceAdapterNoCursorPk([]),
            state_store=FakeStateStore(),
            normalizer=default_normalizer,
            handler=lambda events: None,
        )
