from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from azure_functions_db import ConfigurationError, DbFunctionApp, NoOpCollector
from azure_functions_db.binding.reader import DbReader
from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.trigger.errors import FetchError
from azure_functions_db.trigger.events import RowChange
from tests.test_poll_trigger import FakeSourceAdapter, FakeStateStore


def _sqlite_url(tmp_path: Path, name: str) -> str:
    return f"sqlite:///{tmp_path / name}"


def _create_users_table(url: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
            conn.execute(
                text("INSERT INTO users (id, name) VALUES (:id, :name)"),
                {"id": 1, "name": "Alice"},
            )
    finally:
        engine.dispose()


def _create_orders_table(url: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text("CREATE TABLE processed_orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL)")
            )
    finally:
        engine.dispose()


def test_db_trigger_returns_callable() -> None:
    decorator = DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )

    assert callable(decorator)


def test_db_trigger_calls_handler_with_events() -> None:
    handled: list[list[RowChange]] = []

    @DbFunctionApp().db_trigger(
        arg_name="changes",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(changes: list[RowChange]) -> None:
        handled.append(changes)

    handler(object())

    assert len(handled) == 1
    assert len(handled[0]) == 1
    assert handled[0][0].pk == {"id": 1}


def test_db_trigger_returns_processed_count() -> None:
    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    result = handler(object())

    assert isinstance(result, int)
    assert result == 1


def test_db_trigger_async_handler_rejected() -> None:
    async def handler(events: list[RowChange]) -> None:
        del events

    with pytest.raises(ConfigurationError, match="does not support async"):
        DbFunctionApp().db_trigger(
            arg_name="events",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )(handler)


def test_db_trigger_swallows_lease_error() -> None:
    store = FakeStateStore()
    store.acquire_error = RuntimeError("lease conflict")

    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=store,
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler(object()) == 0


def test_db_trigger_propagates_other_errors() -> None:
    source = FakeSourceAdapter(batches=[])
    source.fetch_error = RuntimeError("db unavailable")

    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=source,
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    with pytest.raises(FetchError, match="Failed to fetch"):
        handler(object())


def test_db_trigger_preserves_function_name() -> None:
    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler.__name__ == "handler"


def test_db_trigger_default_name_from_function() -> None:
    store = FakeStateStore()

    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=store,
    )
    def orders_poll(events: list[RowChange]) -> None:
        del events

    orders_poll(object())

    assert "orders_poll" in store.checkpoints


def test_db_trigger_explicit_name() -> None:
    store = FakeStateStore()

    @DbFunctionApp().db_trigger(
        arg_name="events",
        name="custom_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=store,
    )
    def orders_poll(events: list[RowChange]) -> None:
        del events

    orders_poll(object())

    assert "custom_poller" in store.checkpoints


def test_db_trigger_accepts_metrics() -> None:
    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
        metrics=NoOpCollector(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler(object()) == 1


def test_db_input_injects_reader(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "reader.db")
    _create_users_table(url)

    @DbFunctionApp().db_input("reader", url=url, table="users")
    def handler(reader: DbReader) -> dict[str, object] | None:
        return reader.get(pk={"id": 1})

    assert handler() == {"id": 1, "name": "Alice"}


def test_db_input_auto_closes_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "reader-close.db")
    _create_users_table(url)
    closed: list[DbReader] = []
    original_close = DbReader.close

    def tracking_close(self: DbReader) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbReader, "close", tracking_close)

    @DbFunctionApp().db_input("reader", url=url, table="users")
    def handler(reader: DbReader) -> None:
        assert reader.get(pk={"id": 1}) == {"id": 1, "name": "Alice"}

    handler()

    assert len(closed) == 1


def test_db_input_invalid_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="db_input arg_name='reader' not found"):

        @DbFunctionApp().db_input("reader", url="sqlite:///unused.db", table="users")
        def handler() -> None:
            return None


def test_db_output_injects_writer(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "writer.db")
    _create_orders_table(url)

    @DbFunctionApp().db_output("writer", url=url, table="processed_orders")
    def handler(writer: DbWriter) -> None:
        writer.insert(data={"id": 1, "status": "processed"})

    handler()

    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, status FROM processed_orders ORDER BY id"))
            rows = [dict(row._mapping) for row in result]
    finally:
        engine.dispose()

    assert rows == [{"id": 1, "status": "processed"}]


def test_db_output_auto_closes_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "writer-close.db")
    _create_orders_table(url)
    closed: list[DbWriter] = []
    original_close = DbWriter.close

    def tracking_close(self: DbWriter) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbWriter, "close", tracking_close)

    @DbFunctionApp().db_output("writer", url=url, table="processed_orders")
    def handler(writer: DbWriter) -> None:
        writer.insert(data={"id": 1, "status": "processed"})

    handler()

    assert len(closed) == 1


def test_db_output_invalid_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="db_output arg_name='writer' not found"):

        @DbFunctionApp().db_output("writer", url="sqlite:///unused.db", table="processed")
        def handler() -> None:
            return None


def test_db_trigger_signature_preserves_host_params() -> None:
    """Host trigger params (e.g. timer) must stay visible in __signature__."""
    import inspect

    @DbFunctionApp().db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(timer: object, events: list[RowChange]) -> None:
        del timer, events

    sig = inspect.signature(handler)
    assert "timer" in sig.parameters
    assert "events" not in sig.parameters


def test_db_trigger_stacked_with_db_output(tmp_path: Path) -> None:
    """db_trigger + db_output stacking: events and writer both injected."""
    url = _sqlite_url(tmp_path, "stack.db")
    _create_orders_table(url)

    db = DbFunctionApp()
    captured: dict[str, object] = {}

    @db.db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    @db.db_output("writer", url=url, table="processed_orders")
    def handler(events: list[RowChange], writer: DbWriter) -> None:
        captured["events_count"] = len(events)
        captured["has_writer"] = writer is not None
        writer.insert(data={"id": 1, "status": "done"})

    result = handler(object())

    assert result == 1
    assert captured["events_count"] == 1
    assert captured["has_writer"] is True


def test_db_trigger_stacked_signature_hides_db_params() -> None:
    """When stacked, final __signature__ should only show host params."""
    import inspect

    db = DbFunctionApp()

    @db.db_trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    @db.db_output("writer", url="sqlite:///unused.db", table="t")
    def handler(timer: object, events: list[RowChange], writer: DbWriter) -> None:
        del timer, events, writer

    sig = inspect.signature(handler)
    param_names = list(sig.parameters.keys())
    assert "timer" in param_names
    assert "events" not in param_names
    assert "writer" not in param_names


def test_db_trigger_rejects_async_db_output_wrapper() -> None:
    """db_trigger must reject async-wrapped inner decorators."""
    db = DbFunctionApp()

    with pytest.raises(ConfigurationError, match="does not support async"):

        @db.db_trigger(
            arg_name="events",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )
        @db.db_output("writer", url="sqlite:///unused.db", table="t")
        async def handler(events: list[RowChange], writer: DbWriter) -> None:
            del events, writer


def test_db_trigger_reserved_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="conflicts with Azure Functions"):

        @DbFunctionApp().db_trigger(
            arg_name="timer",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )
        def handler(timer: object) -> None:
            del timer
