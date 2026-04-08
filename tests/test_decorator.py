from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, cast

from pydantic import BaseModel
import pytest
from sqlalchemy import create_engine, text

from azure_functions_db import ConfigurationError, DbBindings
from azure_functions_db.binding.reader import DbReader
from azure_functions_db.binding.writer import DbWriter
from azure_functions_db.core.errors import NotFoundError
from azure_functions_db.decorator import OutputResult
from azure_functions_db.observability import NoOpCollector
from azure_functions_db.trigger.errors import FetchError
from azure_functions_db.trigger.events import RowChange
from tests.test_poll_trigger import FakeSourceAdapter, FakeStateStore


class UserModel(BaseModel):
    id: int
    name: str


class OrderModel(BaseModel):
    id: int
    status: str


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


def _create_users_table_with_data(url: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE users "
                    "(id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL)"
                )
            )
            conn.execute(
                text("INSERT INTO users (id, name, active) VALUES (:id, :name, :active)"),
                [
                    {"id": 1, "name": "Alice", "active": 1},
                    {"id": 2, "name": "Bob", "active": 0},
                    {"id": 3, "name": "Carol", "active": 1},
                ],
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


def _read_orders(url: str) -> list[dict[str, object]]:
    engine = create_engine(url)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT id, status FROM processed_orders ORDER BY id"))
            return [dict(row._mapping) for row in result]
    finally:
        engine.dispose()


# =====================================================================
# trigger tests (unchanged from previous version)
# =====================================================================


def test_trigger_returns_callable() -> None:
    decorator = DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )

    assert callable(decorator)


def test_trigger_calls_handler_with_events() -> None:
    handled: list[list[RowChange]] = []

    @DbBindings().trigger(
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


def test_trigger_returns_processed_count() -> None:
    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    result = handler(object())

    assert isinstance(result, int)
    assert result == 1


def test_trigger_async_handler_rejected() -> None:
    async def handler(events: list[RowChange]) -> None:
        del events

    with pytest.raises(ConfigurationError, match="does not support async"):
        DbBindings().trigger(
            arg_name="events",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )(handler)


def test_trigger_swallows_lease_error() -> None:
    store = FakeStateStore()
    store.acquire_error = RuntimeError("lease conflict")

    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=store,
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler(object()) == 0


def test_trigger_propagates_other_errors() -> None:
    source = FakeSourceAdapter(batches=[])
    source.fetch_error = RuntimeError("db unavailable")

    @DbBindings().trigger(
        arg_name="events",
        source=source,
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    with pytest.raises(FetchError, match="Failed to fetch"):
        handler(object())


def test_trigger_preserves_function_name() -> None:
    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler.__name__ == "handler"


def test_trigger_default_name_from_function() -> None:
    store = FakeStateStore()

    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=store,
    )
    def orders_poll(events: list[RowChange]) -> None:
        del events

    orders_poll(object())

    assert "orders_poll" in store.checkpoints


def test_trigger_explicit_name() -> None:
    store = FakeStateStore()

    @DbBindings().trigger(
        arg_name="events",
        name="custom_poller",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=store,
    )
    def orders_poll(events: list[RowChange]) -> None:
        del events

    orders_poll(object())

    assert "custom_poller" in store.checkpoints


def test_trigger_accepts_metrics() -> None:
    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
        metrics=NoOpCollector(),
    )
    def handler(events: list[RowChange]) -> None:
        del events

    assert handler(object()) == 1


def test_trigger_signature_preserves_host_params() -> None:
    import inspect

    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(timer: object, events: list[RowChange]) -> None:
        del timer, events

    sig = inspect.signature(handler)
    assert "timer" in sig.parameters
    assert "events" not in sig.parameters


def test_trigger_reserved_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="conflicts with Azure Functions"):

        @DbBindings().trigger(
            arg_name="timer",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )
        def handler(timer: object) -> None:
            del timer


# =====================================================================
# input tests — data injection
# =====================================================================


def test_input_pk_static_returns_row(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-pk-static.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1})
    def handler(user: dict[str, object] | None) -> dict[str, object] | None:
        return user

    assert handler() == {"id": 1, "name": "Alice"}


def test_input_pk_static_returns_none_when_not_found(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-pk-none.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 999})
    def handler(user: dict[str, object] | None) -> dict[str, object] | None:
        return user

    assert handler() is None


def test_input_pk_callable_resolves_from_kwargs(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-pk-callable.db")
    _create_users_table(url)

    class FakeReq:
        def __init__(self, user_id: int) -> None:
            self.user_id = user_id

    @DbBindings().input("user", url=url, table="users", pk=lambda req: {"id": req.user_id})
    def handler(req: FakeReq, user: dict[str, object] | None) -> dict[str, object] | None:
        return user

    assert handler(req=FakeReq(1)) == {"id": 1, "name": "Alice"}
    assert handler(req=FakeReq(999)) is None


def test_input_on_not_found_raise(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-not-found-raise.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 999}, on_not_found="raise")
    def handler(user: dict[str, object]) -> dict[str, object]:
        return user

    with pytest.raises(NotFoundError, match="no row found"):
        handler()


def test_input_query_static_returns_rows(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-query-static.db")
    _create_users_table(url)

    @DbBindings().input("users", url=url, query="SELECT id, name FROM users ORDER BY id")
    def handler(users: list[dict[str, object]]) -> list[dict[str, object]]:
        return users

    assert handler() == [{"id": 1, "name": "Alice"}]


def test_input_query_with_static_params(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-query-params.db")
    _create_users_table_with_data(url)

    @DbBindings().input(
        "users",
        url=url,
        query="SELECT id, name FROM users WHERE active = :active ORDER BY id",
        params={"active": 1},
    )
    def handler(users: list[dict[str, object]]) -> list[dict[str, object]]:
        return users

    result = handler()
    assert len(result) == 2
    assert result[0]["name"] == "Alice"
    assert result[1]["name"] == "Carol"


def test_input_query_with_callable_params(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-query-callable-params.db")
    _create_users_table_with_data(url)

    class FakeReq:
        def __init__(self, active: int) -> None:
            self.active = active

    @DbBindings().input(
        "users",
        url=url,
        query="SELECT id, name FROM users WHERE active = :active ORDER BY id",
        params=lambda req: {"active": req.active},
    )
    def handler(req: FakeReq, users: list[dict[str, object]]) -> list[dict[str, object]]:
        return users

    result = handler(req=FakeReq(0))
    assert len(result) == 1
    assert result[0]["name"] == "Bob"


def test_input_query_returns_empty_list(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-query-empty.db")
    _create_users_table(url)

    @DbBindings().input(
        "users",
        url=url,
        query="SELECT id, name FROM users WHERE id = :id",
        params={"id": 999},
    )
    def handler(users: list[dict[str, object]]) -> list[dict[str, object]]:
        return users

    assert handler() == []


def test_input_pk_with_model(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-pk-model.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1}, model=UserModel)
    def handler(user: UserModel | None) -> UserModel | None:
        return user

    result = handler()

    assert result == UserModel(id=1, name="Alice")


def test_input_query_with_model(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-query-model.db")
    _create_users_table_with_data(url)

    @DbBindings().input(
        "users",
        url=url,
        query="SELECT id, name FROM users WHERE active = :active ORDER BY id",
        params={"active": 1},
        model=UserModel,
    )
    def handler(users: list[UserModel]) -> list[UserModel]:
        return users

    assert handler() == [UserModel(id=1, name="Alice"), UserModel(id=3, name="Carol")]


def test_input_pk_with_model_not_found_returns_none(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-pk-model-none.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 999}, model=UserModel)
    def handler(user: UserModel | None) -> UserModel | None:
        return user

    assert handler() is None


@pytest.mark.parametrize("model", [str, int])
def test_input_invalid_model_raises(model: type[object]) -> None:
    with pytest.raises(ConfigurationError, match="subclass of BaseModel"):
        DbBindings().input(
            "user",
            url="sqlite:///unused.db",
            table="users",
            pk={"id": 1},
            model=cast(type[BaseModel], model),
        )


def test_input_auto_closes_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "input-close.db")
    _create_users_table(url)
    closed: list[DbReader] = []
    original_close = DbReader.close

    def tracking_close(self: DbReader) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbReader, "close", tracking_close)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1})
    def handler(user: dict[str, object] | None) -> None:
        pass

    handler()

    assert len(closed) == 1


def test_input_invalid_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="input arg_name='user' not found"):

        @DbBindings().input("user", url="sqlite:///unused.db", table="users", pk={"id": 1})
        def handler() -> None:
            return None


def test_input_requires_pk_or_query() -> None:
    with pytest.raises(ConfigurationError, match="exactly one of 'pk' or 'query'"):
        DbBindings().input("data", url="sqlite:///unused.db")


def test_input_rejects_both_pk_and_query() -> None:
    with pytest.raises(ConfigurationError, match="not both"):
        DbBindings().input(
            "data", url="sqlite:///unused.db", table="t", pk={"id": 1}, query="SELECT 1"
        )


def test_input_pk_requires_table() -> None:
    with pytest.raises(ConfigurationError, match="requires 'table'"):
        DbBindings().input("data", url="sqlite:///unused.db", pk={"id": 1})


def test_input_params_requires_query() -> None:
    with pytest.raises(ConfigurationError, match="only valid with 'query'"):
        DbBindings().input(
            "data", url="sqlite:///unused.db", table="t", pk={"id": 1}, params={"x": 1}
        )


def test_input_resolver_rejects_var_args() -> None:
    with pytest.raises(ConfigurationError, match="must not use \\*args or \\*\\*kwargs"):

        @DbBindings().input(
            "user",
            url="sqlite:///unused.db",
            table="users",
            pk=lambda *args: {"id": 1},
        )
        def handler(req: object, user: object) -> None:
            pass


def test_input_resolver_rejects_unknown_params() -> None:
    with pytest.raises(ConfigurationError, match="references parameters.*not found"):

        @DbBindings().input(
            "user",
            url="sqlite:///unused.db",
            table="users",
            pk=lambda unknown: {"id": 1},
        )
        def handler(req: object, user: object) -> None:
            pass


def test_input_preserves_function_name(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "input-name.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1})
    def my_handler(user: dict[str, object] | None) -> None:
        pass

    assert my_handler.__name__ == "my_handler"


def test_input_hides_arg_from_signature(tmp_path: Path) -> None:
    import inspect

    url = _sqlite_url(tmp_path, "input-sig.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1})
    def handler(req: object, user: dict[str, object] | None) -> None:
        pass

    sig = inspect.signature(handler)
    assert "req" in sig.parameters
    assert "user" not in sig.parameters


# =====================================================================
# output tests — return-value auto-write
# =====================================================================


def test_output_insert_dict(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-insert-dict.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> dict[str, object]:
        return {"id": 1, "status": "done"}

    result = handler()

    assert result == {"id": 1, "status": "done"}
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


def test_output_insert_list(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-insert-list.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> list[dict[str, object]]:
        return [
            {"id": 1, "status": "a"},
            {"id": 2, "status": "b"},
        ]

    result = handler()

    assert len(result) == 2
    assert _read_orders(url) == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]


def test_output_none_is_noop(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-none.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> None:
        return None

    handler()

    assert _read_orders(url) == []


def test_output_accepts_basemodel_return(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-model.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> OrderModel:
        return OrderModel(id=1, status="done")

    result = handler()

    assert result == OrderModel(id=1, status="done")
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


def test_output_accepts_list_basemodel_return(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-model-list.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> list[OrderModel]:
        return [OrderModel(id=1, status="a"), OrderModel(id=2, status="b")]

    result = handler()

    assert result == [OrderModel(id=1, status="a"), OrderModel(id=2, status="b")]
    assert _read_orders(url) == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]


def test_output_accepts_mixed_list(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-model-mixed.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> list[dict[str, object] | OrderModel]:
        return [{"id": 1, "status": "a"}, OrderModel(id=2, status="b")]

    result = handler()

    assert result == [{"id": 1, "status": "a"}, OrderModel(id=2, status="b")]
    assert _read_orders(url) == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]


def test_output_upsert_dict(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-upsert-dict.db")
    _create_orders_table(url)

    db = DbBindings()

    @db.output(url=url, table="processed_orders", action="upsert", conflict_columns=["id"])
    def handler() -> dict[str, object]:
        return {"id": 1, "status": "first"}

    handler()
    assert _read_orders(url) == [{"id": 1, "status": "first"}]

    @db.output(url=url, table="processed_orders", action="upsert", conflict_columns=["id"])
    def handler2() -> dict[str, object]:
        return {"id": 1, "status": "updated"}

    handler2()
    assert _read_orders(url) == [{"id": 1, "status": "updated"}]


def test_output_upsert_list(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-upsert-list.db")
    _create_orders_table(url)

    @DbBindings().output(
        url=url, table="processed_orders", action="upsert", conflict_columns=["id"]
    )
    def handler() -> list[dict[str, object]]:
        return [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]

    handler()
    assert _read_orders(url) == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]


def test_output_upsert_requires_conflict_columns() -> None:
    with pytest.raises(ConfigurationError, match="requires 'conflict_columns'"):
        DbBindings().output(url="sqlite:///unused.db", table="t", action="upsert")


def test_output_rejects_invalid_return_type(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-invalid.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> str:
        return "not a dict"

    with pytest.raises(ConfigurationError, match="expected dict, list"):
        handler()


def test_output_auto_closes_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "output-close.db")
    _create_orders_table(url)
    closed: list[DbWriter] = []
    original_close = DbWriter.close

    def tracking_close(self: DbWriter) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbWriter, "close", tracking_close)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> dict[str, object]:
        return {"id": 1, "status": "done"}

    handler()

    assert len(closed) == 1


def test_output_preserves_function_name() -> None:
    @DbBindings().output(url="sqlite:///unused.db", table="t")
    def my_handler() -> None:
        return None

    assert my_handler.__name__ == "my_handler"


def test_output_returns_original_value(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-return.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> dict[str, object]:
        return {"id": 1, "status": "done"}

    result = handler()
    assert result == {"id": 1, "status": "done"}


# =====================================================================
# inject_reader tests — client injection (imperative escape hatch)
# =====================================================================


def test_inject_reader_injects_reader(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "reader.db")
    _create_users_table(url)

    @DbBindings().inject_reader("reader", url=url, table="users")
    def handler(reader: DbReader) -> dict[str, object] | None:
        return reader.get(pk={"id": 1})

    assert handler() == {"id": 1, "name": "Alice"}


def test_inject_reader_auto_closes_reader(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "reader-close.db")
    _create_users_table(url)
    closed: list[DbReader] = []
    original_close = DbReader.close

    def tracking_close(self: DbReader) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbReader, "close", tracking_close)

    @DbBindings().inject_reader("reader", url=url, table="users")
    def handler(reader: DbReader) -> None:
        assert reader.get(pk={"id": 1}) == {"id": 1, "name": "Alice"}

    handler()

    assert len(closed) == 1


def test_inject_reader_invalid_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="inject_reader arg_name='reader' not found"):

        @DbBindings().inject_reader("reader", url="sqlite:///unused.db", table="users")
        def handler() -> None:
            return None


def test_inject_reader_async_proxy_get(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "reader-async-get.db")
    _create_users_table(url)

    @DbBindings().inject_reader("reader", url=url, table="users")
    async def handler(reader: Any) -> Any:
        return await reader.get(pk={"id": 1})

    assert asyncio.run(handler()) == {"id": 1, "name": "Alice"}


def test_inject_reader_async_proxy_query(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "reader-async-query.db")
    _create_users_table_with_data(url)

    @DbBindings().inject_reader("reader", url=url, table="users")
    async def handler(reader: Any) -> Any:
        return await reader.query(
            "SELECT id, name FROM users WHERE active = :active ORDER BY id",
            params={"active": 1},
        )

    assert asyncio.run(handler()) == [{"id": 1, "name": "Alice"}, {"id": 3, "name": "Carol"}]


# =====================================================================
# inject_writer tests — client injection (imperative escape hatch)
# =====================================================================


def test_inject_writer_injects_writer(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "writer.db")
    _create_orders_table(url)

    @DbBindings().inject_writer("writer", url=url, table="processed_orders")
    def handler(writer: DbWriter) -> None:
        writer.insert(data={"id": 1, "status": "processed"})

    handler()

    assert _read_orders(url) == [{"id": 1, "status": "processed"}]


def test_inject_writer_auto_closes_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    url = _sqlite_url(tmp_path, "writer-close.db")
    _create_orders_table(url)
    closed: list[DbWriter] = []
    original_close = DbWriter.close

    def tracking_close(self: DbWriter) -> None:
        closed.append(self)
        original_close(self)

    monkeypatch.setattr(DbWriter, "close", tracking_close)

    @DbBindings().inject_writer("writer", url=url, table="processed_orders")
    def handler(writer: DbWriter) -> None:
        writer.insert(data={"id": 1, "status": "processed"})

    handler()

    assert len(closed) == 1


def test_inject_writer_invalid_arg_name_raises() -> None:
    with pytest.raises(ConfigurationError, match="inject_writer arg_name='writer' not found"):

        @DbBindings().inject_writer("writer", url="sqlite:///unused.db", table="processed")
        def handler() -> None:
            return None


def test_inject_writer_async_proxy_insert(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "writer-async-insert.db")
    _create_orders_table(url)

    @DbBindings().inject_writer("writer", url=url, table="processed_orders")
    async def handler(writer: Any) -> None:
        await writer.insert(data={"id": 1, "status": "processed"})

    asyncio.run(handler())

    assert _read_orders(url) == [{"id": 1, "status": "processed"}]


def test_inject_writer_async_proxy_upsert(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "writer-async-upsert.db")
    _create_orders_table(url)

    @DbBindings().inject_writer("writer", url=url, table="processed_orders")
    async def handler(writer: Any) -> None:
        await writer.upsert(data={"id": 1, "status": "processed"}, conflict_columns=["id"])

    asyncio.run(handler())

    assert _read_orders(url) == [{"id": 1, "status": "processed"}]


# =====================================================================
# Stacking tests
# =====================================================================


def test_trigger_stacked_with_output(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "stack-trigger-output.db")
    _create_orders_table(url)

    db = DbBindings()

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    @db.output(url=url, table="processed_orders")
    def handler(events: list[RowChange]) -> list[dict[str, object]]:
        return [{"id": e.pk["id"], "status": "done"} for e in events]

    result = handler(object())

    assert result == 1
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


def test_trigger_stacked_with_inject_writer(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "stack-trigger-writer.db")
    _create_orders_table(url)

    db = DbBindings()
    captured: dict[str, object] = {}

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    @db.inject_writer("writer", url=url, table="processed_orders")
    def handler(events: list[RowChange], writer: DbWriter) -> None:
        captured["events_count"] = len(events)
        captured["has_writer"] = writer is not None
        writer.insert(data={"id": 1, "status": "done"})

    result = handler(object())

    assert result == 1
    assert captured["events_count"] == 1
    assert captured["has_writer"] is True


def test_trigger_stacked_signature_hides_db_params() -> None:
    import inspect

    db = DbBindings()

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    @db.inject_writer("writer", url="sqlite:///unused.db", table="t")
    def handler(timer: object, events: list[RowChange], writer: DbWriter) -> None:
        del timer, events, writer

    sig = inspect.signature(handler)
    param_names = list(sig.parameters.keys())
    assert "timer" in param_names
    assert "events" not in param_names
    assert "writer" not in param_names


def test_trigger_rejects_async_inject_writer_wrapper() -> None:
    db = DbBindings()

    with pytest.raises(ConfigurationError, match="does not support async"):

        @db.trigger(
            arg_name="events",
            source=FakeSourceAdapter(batches=[]),
            checkpoint_store=FakeStateStore(),
        )
        @db.inject_writer("writer", url="sqlite:///unused.db", table="t")
        async def handler(events: list[RowChange], writer: DbWriter) -> None:
            del events, writer


def test_input_stacked_with_output(tmp_path: Path) -> None:
    url_read = _sqlite_url(tmp_path, "stack-input-read.db")
    url_write = _sqlite_url(tmp_path, "stack-input-write.db")
    _create_users_table(url_read)
    _create_orders_table(url_write)

    db = DbBindings()

    @db.input("user", url=url_read, table="users", pk={"id": 1})
    @db.output(url=url_write, table="processed_orders")
    def handler(user: dict[str, object] | None) -> dict[str, object] | None:
        if user is None:
            return None
        return {"id": user["id"], "status": "processed"}

    result = handler()

    assert result == {"id": 1, "status": "processed"}
    assert _read_orders(url_write) == [{"id": 1, "status": "processed"}]


def test_duplicate_trigger_raises() -> None:
    db = DbBindings()
    mock_source = FakeSourceAdapter(batches=[])
    mock_store = FakeStateStore()

    with pytest.raises(ConfigurationError, match="cannot be applied twice"):

        @db.trigger(arg_name="e1", source=mock_source, checkpoint_store=mock_store)
        @db.trigger(arg_name="e2", source=mock_source, checkpoint_store=mock_store)
        def handler(e1: list[RowChange], e2: list[RowChange]) -> None:
            del e1, e2


def test_input_and_inject_reader_conflict() -> None:
    db = DbBindings()

    with pytest.raises(ConfigurationError, match="Cannot combine"):

        @db.input("user", url="sqlite:///x.db", table="t", pk={"id": 1})
        @db.inject_reader("reader", url="sqlite:///x.db", table="t")
        def handler(user: dict[str, object] | None, reader: DbReader) -> None:
            del user, reader


def test_trigger_plus_output_allowed(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "composition-trigger-output.db")
    db = DbBindings()

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    @db.output(url=url, table="processed_orders")
    def handler(events: list[RowChange]) -> list[dict[str, object]]:
        return [{"id": e.pk["id"], "status": "done"} for e in events]

    assert callable(handler)


def test_input_plus_output_allowed(tmp_path: Path) -> None:
    url_read = _sqlite_url(tmp_path, "composition-input-read.db")
    url_write = _sqlite_url(tmp_path, "composition-input-write.db")
    _create_users_table(url_read)
    _create_orders_table(url_write)

    db = DbBindings()

    @db.input("user", url=url_read, table="users", pk={"id": 1})
    @db.output(url=url_write, table="processed_orders")
    def handler(user: dict[str, object] | None) -> dict[str, object] | None:
        if user is None:
            return None
        return {"id": user["id"], "status": "processed"}

    assert handler() == {"id": 1, "status": "processed"}
    assert _read_orders(url_write) == [{"id": 1, "status": "processed"}]


# =====================================================================
# Review feedback: validation, positional args, host param forwarding
# =====================================================================


def test_input_resolver_rejects_positional_only_params() -> None:
    def resolver(x: int, /) -> dict[str, object]:
        return {"id": x}

    with pytest.raises(ConfigurationError, match="positional-only"):

        @DbBindings().input(
            "user",
            url="sqlite:///unused.db",
            table="users",
            pk=resolver,
        )
        def handler(x: int, user: object) -> None:
            pass


def test_input_invalid_on_not_found_raises() -> None:
    with pytest.raises(ConfigurationError, match="on_not_found must be"):
        DbBindings().input(
            "user",
            url="sqlite:///unused.db",
            table="users",
            pk={"id": 1},
            on_not_found=cast(Any, "bogus"),
        )


def test_output_invalid_action_raises() -> None:
    with pytest.raises(ConfigurationError, match="action must be"):
        DbBindings().output(
            url="sqlite:///unused.db",
            table="t",
            action=cast(Any, "bogus"),
        )


def test_output_rejects_invalid_list_elements(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "output-bad-list.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> list[object]:
        return [{"id": 1, "status": "ok"}, "not_a_dict"]

    with pytest.raises(ConfigurationError, match="non-dict element at index 1"):
        handler()


def test_trigger_forwards_host_params_at_runtime() -> None:
    received: dict[str, object] = {}
    host_timer = object()

    @DbBindings().trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(timer: object, events: list[RowChange]) -> None:
        received["timer"] = timer
        received["events_count"] = len(events)

    handler(host_timer)

    assert received["timer"] is host_timer
    assert received["events_count"] == 1


def test_trigger_forwards_host_params_with_output(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "trigger-host-output.db")
    _create_orders_table(url)
    received: dict[str, object] = {}
    host_timer = object()

    db = DbBindings()

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[[{"id": 1, "updated_at": 100}]]),
        checkpoint_store=FakeStateStore(),
    )
    @db.output(url=url, table="processed_orders")
    def handler(timer: object, events: list[RowChange]) -> list[dict[str, object]]:
        received["timer"] = timer
        return [{"id": e.pk["id"], "status": "done"} for e in events]

    result = handler(host_timer)

    assert result == 1
    assert received["timer"] is host_timer
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


# =====================================================================
# Async decorator tests
# =====================================================================


def test_input_async_pk_static(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-input-pk.db")
    _create_users_table(url)

    @DbBindings().input("user", url=url, table="users", pk={"id": 1})
    async def handler(user: dict[str, object] | None) -> dict[str, object] | None:
        return user

    assert asyncio.run(handler()) == {"id": 1, "name": "Alice"}


def test_input_async_query(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-input-query.db")
    _create_users_table(url)

    @DbBindings().input("users", url=url, query="SELECT id, name FROM users ORDER BY id")
    async def handler(users: list[dict[str, object]]) -> list[dict[str, object]]:
        return users

    assert asyncio.run(handler()) == [{"id": 1, "name": "Alice"}]


def test_input_async_pk_callable(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-input-pk-callable.db")
    _create_users_table(url)

    class FakeReq:
        def __init__(self, user_id: int) -> None:
            self.user_id = user_id

    @DbBindings().input("user", url=url, table="users", pk=lambda req: {"id": req.user_id})
    async def handler(req: FakeReq, user: dict[str, object] | None) -> dict[str, object] | None:
        return user

    assert asyncio.run(handler(req=FakeReq(1))) == {"id": 1, "name": "Alice"}
    assert asyncio.run(handler(req=FakeReq(999))) is None


def test_output_async_insert(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-output-insert.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    async def handler() -> dict[str, object]:
        return {"id": 1, "status": "async_done"}

    result = asyncio.run(handler())

    assert result == {"id": 1, "status": "async_done"}
    assert _read_orders(url) == [{"id": 1, "status": "async_done"}]


def test_output_async_none_is_noop(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-output-none.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    async def handler() -> None:
        return None

    asyncio.run(handler())

    assert _read_orders(url) == []


def test_inject_reader_async_injects_reader(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-reader.db")
    _create_users_table(url)

    @DbBindings().inject_reader("reader", url=url, table="users")
    async def handler(reader: Any) -> Any:
        return await reader.get(pk={"id": 1})

    assert asyncio.run(handler()) == {"id": 1, "name": "Alice"}


def test_inject_writer_async_injects_writer(tmp_path: Path) -> None:
    import asyncio

    url = _sqlite_url(tmp_path, "async-writer.db")
    _create_orders_table(url)

    @DbBindings().inject_writer("writer", url=url, table="processed_orders")
    async def handler(writer: Any) -> None:
        await writer.insert(data={"id": 1, "status": "async_written"})

    asyncio.run(handler())

    assert _read_orders(url) == [{"id": 1, "status": "async_written"}]


# =====================================================================
# OutputResult tests — separate return value from write payload
# =====================================================================


def test_output_result_dict_write(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-dict.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> OutputResult[str]:
        return OutputResult(
            return_value="Created",
            write={"id": 1, "status": "done"},
        )

    result = handler()

    assert result == "Created"
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


def test_output_result_none_write_skips_db(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-none.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> OutputResult[str]:
        return OutputResult(return_value="OK", write=None)

    result = handler()

    assert result == "OK"
    assert _read_orders(url) == []


def test_output_result_list_write(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-list.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> OutputResult[str]:
        return OutputResult(
            return_value="batch",
            write=[{"id": 1, "status": "a"}, {"id": 2, "status": "b"}],
        )

    result = handler()

    assert result == "batch"
    assert _read_orders(url) == [{"id": 1, "status": "a"}, {"id": 2, "status": "b"}]


def test_output_result_basemodel_write(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-model.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> OutputResult[str]:
        return OutputResult(
            return_value="model_written",
            write=OrderModel(id=1, status="done"),
        )

    result = handler()

    assert result == "model_written"
    assert _read_orders(url) == [{"id": 1, "status": "done"}]


def test_output_result_async_dict_write(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-async-dict.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    async def handler() -> OutputResult[str]:
        return OutputResult(
            return_value="async_created",
            write={"id": 1, "status": "async_done"},
        )

    result = asyncio.run(handler())

    assert result == "async_created"
    assert _read_orders(url) == [{"id": 1, "status": "async_done"}]


def test_output_result_async_none_write(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-async-none.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    async def handler() -> OutputResult[str]:
        return OutputResult(return_value="async_ok", write=None)

    result = asyncio.run(handler())

    assert result == "async_ok"
    assert _read_orders(url) == []


def test_output_result_upsert(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-upsert.db")
    _create_orders_table(url)

    db = DbBindings()

    @db.output(url=url, table="processed_orders", action="upsert", conflict_columns=["id"])
    def handler() -> OutputResult[str]:
        return OutputResult(
            return_value="upserted",
            write={"id": 1, "status": "first"},
        )

    result = handler()
    assert result == "upserted"
    assert _read_orders(url) == [{"id": 1, "status": "first"}]

    @db.output(url=url, table="processed_orders", action="upsert", conflict_columns=["id"])
    def handler2() -> OutputResult[str]:
        return OutputResult(
            return_value="updated",
            write={"id": 1, "status": "second"},
        )

    result2 = handler2()
    assert result2 == "updated"
    assert _read_orders(url) == [{"id": 1, "status": "second"}]


def test_output_plain_dict_unaffected_by_output_result(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-legacy-dict.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> dict[str, object]:
        return {"id": 1, "status": "legacy"}

    result = handler()

    assert result == {"id": 1, "status": "legacy"}
    assert _read_orders(url) == [{"id": 1, "status": "legacy"}]


def test_output_plain_none_unaffected_by_output_result(tmp_path: Path) -> None:
    url = _sqlite_url(tmp_path, "or-legacy-none.db")
    _create_orders_table(url)

    @DbBindings().output(url=url, table="processed_orders")
    def handler() -> None:
        return None

    handler()
    assert _read_orders(url) == []
