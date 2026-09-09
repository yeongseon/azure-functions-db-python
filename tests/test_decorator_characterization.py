"""Characterization tests locking the public decorator contract before the split.

These tests intentionally assert *current* behavior (import paths, exported
symbols, and the metadata each decorator emits) so that splitting
``decorator.py`` into cohesive modules cannot silently change the public API.
See issue #265.
"""

from __future__ import annotations

import importlib
from typing import Any

import azure_functions_db as pkg
import azure_functions_db.decorator as decorator_mod
from azure_functions_db.decorator import DbBindings, DbOut, get_db_metadata


class TestPublicImportSurface:
    """The public names exported from the package must remain stable."""

    def test_top_level_reexports(self) -> None:
        assert pkg.DbBindings is DbBindings
        assert pkg.DbOut is DbOut
        assert pkg.get_db_metadata is get_db_metadata

    def test_public_names_importable_from_decorator_module(self) -> None:
        for name in ("DbBindings", "DbOut", "get_db_metadata"):
            assert hasattr(decorator_mod, name), name


class TestPrivateImportSurface:
    """Private helpers used by tests / advanced consumers must stay reachable.

    Downstream code and the existing test-suite reach these through
    ``azure_functions_db.decorator``; the split must preserve every one.
    """

    PRIVATE_NAMES = (
        "_AsyncProxyBase",
        "_AsyncDbOutProxy",
        "_AsyncDbReaderProxy",
        "_AsyncDbWriterProxy",
        "_AsyncTxWriterProxy",
        "_get_db_decorators",
        "_mark_decorator",
        "_merge_toolkit_metadata",
        "_check_composition",
        "_validate_arg_name",
        "_build_host_signature",
        "_validate_resolver",
        "_resolve_callable",
        "_validate_model_type",
        "_apply_input_model",
        "_normalize_output_row",
        "_finalize_wrapper",
        "_wrap_handler",
        "_RESERVED_ARGS",
        "_DB_DECORATOR_ATTR",
    )

    def test_private_helpers_reachable(self) -> None:
        for name in self.PRIVATE_NAMES:
            assert hasattr(decorator_mod, name), name

    def test_module_reimport_is_idempotent(self) -> None:
        reloaded = importlib.reload(decorator_mod)
        assert reloaded.DbBindings is DbBindings or reloaded.DbBindings.__name__ == "DbBindings"


class TestDecoratorMetadataContract:
    """Each decorator must emit exactly the metadata shape it emits today."""

    def test_trigger_metadata(self) -> None:
        from tests.test_poll_trigger import FakeSourceAdapter, FakeStateStore

        db = DbBindings()

        @db.trigger(
            arg_name="events",
            source=FakeSourceAdapter([]),
            checkpoint_store=FakeStateStore(),
        )
        def handler(timer: Any, events: Any) -> None: ...

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [{"kind": "trigger", "parameter": "events"}],
            "injections": [],
        }

    def test_input_pk_metadata(self) -> None:
        db = DbBindings()

        @db.input("user", url="sqlite://", table="users", pk={"id": 1})
        def handler(req: Any, user: Any) -> Any:
            return user

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [
                {
                    "kind": "input",
                    "parameter": "user",
                    "connection_setting": "sqlite://",
                    "resource": {"table": "users"},
                    "query_kind": "pk",
                }
            ],
            "injections": [],
        }

    def test_input_query_metadata(self) -> None:
        db = DbBindings()

        @db.input("rows", url="sqlite://", query="SELECT 1")
        def handler(rows: Any) -> Any:
            return rows

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [
                {
                    "kind": "input",
                    "parameter": "rows",
                    "connection_setting": "sqlite://",
                    "resource": {},
                    "query_kind": "text",
                }
            ],
            "injections": [],
        }

    def test_output_metadata(self) -> None:
        db = DbBindings()

        @db.output("out", url="sqlite://", table="orders")
        def handler(req: Any, out: DbOut) -> None: ...

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [
                {
                    "kind": "output",
                    "parameter": "out",
                    "connection_setting": "sqlite://",
                    "resource": {"table": "orders"},
                }
            ],
            "injections": [],
        }

    def test_inject_reader_metadata(self) -> None:
        db = DbBindings()

        @db.inject_reader("reader", url="sqlite://")
        def handler(req: Any, reader: Any) -> None: ...

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [],
            "injections": [{"kind": "reader", "parameter": "reader"}],
        }

    def test_inject_writer_metadata(self) -> None:
        db = DbBindings()

        @db.inject_writer("writer", url="sqlite://", table="orders")
        def handler(req: Any, writer: Any) -> None: ...

        assert get_db_metadata(handler) == {
            "version": 1,
            "bindings": [],
            "injections": [{"kind": "writer", "parameter": "writer"}],
        }


class TestSignatureHidingContract:
    """Injected parameters must stay hidden from the host-visible signature."""

    def test_output_hides_injected_param(self) -> None:
        import inspect

        db = DbBindings()

        @db.output("out", url="sqlite://", table="orders")
        def handler(req: Any, out: DbOut) -> None: ...

        params = inspect.signature(handler).parameters
        assert "out" not in params
        assert "req" in params

    def test_get_db_metadata_returns_none_for_plain_function(self) -> None:
        def plain(x: int) -> int:
            return x

        assert get_db_metadata(plain) is None
