from __future__ import annotations

from pydantic import BaseModel

from azure_functions_db import DbBindings, get_db_metadata
from azure_functions_db.trigger.events import RowChange
from tests.test_poll_trigger import FakeSourceAdapter, FakeStateStore


class MyModel(BaseModel):
    id: int
    name: str


def test_input_pk_sets_metadata() -> None:
    """Test that input decorator with PK sets toolkit metadata."""
    db = DbBindings()

    @db.input("user", url="sqlite:///test.db", table="users", pk={"id": 1})
    def handler(user: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    assert metadata is not None
    assert "db" in metadata
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert isinstance(db_meta["bindings"], list)
    assert len(db_meta["bindings"]) == 1
    binding = db_meta["bindings"][0]
    assert binding["kind"] == "input"
    assert binding["parameter"] == "user"
    assert binding["connection_setting"] == "sqlite:///test.db"
    assert binding["resource"] == {"table": "users"}
    assert binding["query_kind"] == "pk"
    assert db_meta["injections"] == []


def test_input_query_sets_metadata() -> None:
    """Test that input decorator with query sets correct query_kind."""
    db = DbBindings()

    @db.input("rows", url="sqlite:///test.db", query="SELECT 1")
    def handler(rows: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert len(db_meta["bindings"]) == 1
    binding = db_meta["bindings"][0]
    assert binding["kind"] == "input"
    assert binding["parameter"] == "rows"
    assert binding["connection_setting"] == "sqlite:///test.db"
    assert binding["resource"] == {}
    assert binding["query_kind"] == "text"
    assert db_meta["injections"] == []


def test_input_with_model_sets_model_ref() -> None:
    """Test that input decorator with model sets model_ref in metadata."""
    db = DbBindings()

    @db.input(
        "item",
        url="sqlite:///test.db",
        table="t",
        pk={"id": 1},
        model=MyModel,
    )
    def handler(item: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    binding = db_meta["bindings"][0]
    assert "model_ref" in binding
    assert "MyModel" in binding["model_ref"]


def test_output_sets_metadata() -> None:
    """Test that output decorator sets toolkit metadata correctly."""
    db = DbBindings()

    @db.output("out", url="sqlite:///test.db", table="orders")
    def handler(out: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert len(db_meta["bindings"]) == 1
    binding = db_meta["bindings"][0]
    assert binding["kind"] == "output"
    assert binding["parameter"] == "out"
    assert binding["connection_setting"] == "sqlite:///test.db"
    assert binding["resource"] == {"table": "orders"}
    assert db_meta["injections"] == []


def test_trigger_sets_metadata() -> None:
    """Test that trigger decorator sets toolkit metadata."""
    db = DbBindings()

    @db.trigger(
        arg_name="events",
        source=FakeSourceAdapter(batches=[]),
        checkpoint_store=FakeStateStore(),
    )
    def handler(events: list[RowChange]) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert len(db_meta["bindings"]) == 1
    binding = db_meta["bindings"][0]
    assert binding["kind"] == "trigger"
    assert binding["parameter"] == "events"
    assert "connection_setting" not in binding
    assert "resource" not in binding
    assert db_meta["injections"] == []


def test_inject_reader_sets_metadata() -> None:
    """Test that inject_reader decorator sets metadata with injections."""
    db = DbBindings()

    @db.inject_reader("reader", url="sqlite:///test.db")
    def handler(reader: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert db_meta["bindings"] == []
    assert len(db_meta["injections"]) == 1
    injection = db_meta["injections"][0]
    assert injection["kind"] == "reader"
    assert injection["parameter"] == "reader"


def test_inject_writer_sets_metadata() -> None:
    """Test that inject_writer decorator sets metadata with injections."""
    db = DbBindings()

    @db.inject_writer("writer", url="sqlite:///test.db", table="t")
    def handler(writer: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert db_meta["bindings"] == []
    assert len(db_meta["injections"]) == 1
    injection = db_meta["injections"][0]
    assert injection["kind"] == "writer"
    assert injection["parameter"] == "writer"


def test_stacked_decorators_merge_metadata() -> None:
    """Test that stacking input and output decorators merges bindings."""
    db = DbBindings()

    @db.input("user", url="sqlite:///test.db", table="users", pk={"id": 1})
    @db.output("out", url="sqlite:///test.db", table="orders")
    def handler(user: object, out: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    db_meta = metadata["db"]
    assert len(db_meta["bindings"]) == 2
    kinds = {b["kind"] for b in db_meta["bindings"]}
    assert kinds == {"input", "output"}
    assert db_meta["injections"] == []


def test_stacked_preserves_other_namespaces() -> None:
    """Test that db decorator preserves other toolkit metadata namespaces."""
    db = DbBindings()

    def handler(out: object) -> None:
        pass

    # Manually set metadata for another namespace
    setattr(
        handler,
        "_azure_functions_metadata",
        {"validation": {"version": 1, "rules": []}},
    )

    # Apply db decorator
    handler = db.output("out", url="sqlite:///test.db", table="t")(handler)

    metadata = getattr(handler, "_azure_functions_metadata")
    assert "validation" in metadata
    assert "db" in metadata
    assert metadata["validation"] == {"version": 1, "rules": []}
    assert "bindings" in metadata["db"]


def test_decorator_order_independent() -> None:
    """Test that decorator order doesn't affect binding count."""
    db1 = DbBindings()
    db2 = DbBindings()

    # Order 1: output then input
    @db1.input("user", url="sqlite:///test.db", table="users", pk={"id": 1})
    @db1.output("out", url="sqlite:///test.db", table="orders")
    def handler1(user: object, out: object) -> None:
        pass

    # Order 2: input then output
    @db2.output("out", url="sqlite:///test.db", table="orders")
    @db2.input("user", url="sqlite:///test.db", table="users", pk={"id": 1})
    def handler2(user: object, out: object) -> None:
        pass

    meta1 = getattr(handler1, "_azure_functions_metadata")["db"]
    meta2 = getattr(handler2, "_azure_functions_metadata")["db"]

    assert len(meta1["bindings"]) == 2
    assert len(meta2["bindings"]) == 2

    # Both should have one input and one output (regardless of order)
    kinds1 = {b["kind"] for b in meta1["bindings"]}
    kinds2 = {b["kind"] for b in meta2["bindings"]}
    assert kinds1 == {"input", "output"}
    assert kinds2 == {"input", "output"}


def test_get_db_metadata_returns_none_for_undecorated() -> None:
    """Test that get_db_metadata returns None for undecorated function."""
    def undecorated() -> None:
        pass
    result = get_db_metadata(undecorated)
    assert result is None


def test_async_handler_sets_metadata() -> None:
    """Test that metadata is set correctly on async handlers."""
    db = DbBindings()

    @db.input("user", url="sqlite:///test.db", table="users", pk={"id": 1})
    async def handler(user: object) -> None:
        pass

    metadata = getattr(handler, "_azure_functions_metadata")
    assert metadata is not None
    db_meta = metadata["db"]
    assert db_meta["version"] == 1
    assert len(db_meta["bindings"]) == 1
    binding = db_meta["bindings"][0]
    assert binding["kind"] == "input"
    assert binding["parameter"] == "user"
    assert binding["connection_setting"] == "sqlite:///test.db"
    assert binding["resource"] == {"table": "users"}
    assert binding["query_kind"] == "pk"
    assert db_meta["injections"] == []
