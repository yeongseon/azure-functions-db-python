from __future__ import annotations

import pytest

from azure_functions_db.decorator import db
from azure_functions_db.trigger.errors import FetchError
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.normalizers import default_normalizer
from tests.test_poll_trigger import FakeSourceAdapter, FakeStateStore


def test_db_poll_returns_callable() -> None:
    decorator = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    assert callable(decorator)  # noqa: S101


def test_decorated_function_accepts_timer() -> None:
    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )(lambda events: None)

    result = decorated(object())
    assert result == 1  # noqa: S101


def test_decorated_function_calls_handler_with_events() -> None:
    handled: list[list[RowChange]] = []

    def handler(events: list[RowChange]) -> None:
        handled.append(events)

    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )(handler)

    decorated(object())

    assert len(handled) == 1  # noqa: S101
    assert len(handled[0]) == 1  # noqa: S101
    assert handled[0][0].pk == {"id": 1}  # noqa: S101


def test_decorated_function_returns_processed_count() -> None:
    def handler(events: list[RowChange]) -> None:
        del events

    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )(handler)

    result = decorated(object())

    assert isinstance(result, int)  # noqa: S101
    assert result == 1  # noqa: S101


def test_db_poll_async_handler_rejected() -> None:
    async def handler(events: list[RowChange]) -> None:
        del events

    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )(handler)

    with pytest.raises(TypeError, match="Async handlers are not supported"):
        decorated(object())


def test_db_poll_swallows_lease_acquire_error() -> None:
    store = FakeStateStore()
    store.acquire_error = RuntimeError("lease conflict")

    def handler(events: list[RowChange]) -> None:
        del events

    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=store,
    )(handler)

    result = decorated(object())
    assert result == 0  # noqa: S101


def test_db_poll_propagates_other_errors() -> None:
    source = FakeSourceAdapter(batches=[])
    source.fetch_error = RuntimeError("db unavailable")

    def handler(events: list[RowChange]) -> None:
        del events

    decorated = db.poll(
        name="test_poller",
        source=source,
        checkpoint_store=FakeStateStore(),
    )(handler)

    with pytest.raises(FetchError, match="Failed to fetch"):
        decorated(object())


def test_db_poll_accepts_explicit_normalizer() -> None:
    handled: list[list[RowChange]] = []

    def handler(events: list[RowChange]) -> None:
        handled.append(events)

    decorated = db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
        normalizer=default_normalizer,
    )(handler)

    decorated(object())

    assert handled[0][0].cursor is None  # noqa: S101
    assert handled[0][0].pk == {}  # noqa: S101


def test_decorator_preserves_function_name() -> None:
    @db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler.__name__ == "handler"  # noqa: S101


def test_decorator_preserves_wrapped() -> None:
    @db.poll(
        name="test_poller",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert getattr(handler, "__wrapped__", None) is not None  # noqa: S101
