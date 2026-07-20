"""Tests for the typed ``db`` metadata contract (:mod:`azure_functions_db._metadata`)."""

from __future__ import annotations

from azure_functions_db import _metadata
from azure_functions_db._metadata import (
    DB_METADATA_VERSION,
    METADATA_ATTR,
    NAMESPACE,
    merge_db_metadata,
    read_db_metadata,
)


def _handler() -> object:
    def fn() -> None: ...

    return fn


class TestContractConstants:
    def test_attr_name(self) -> None:
        assert METADATA_ATTR == "_azure_functions_metadata"

    def test_namespace(self) -> None:
        assert NAMESPACE == "db"

    def test_version(self) -> None:
        assert DB_METADATA_VERSION == 1


class TestMergeDbMetadata:
    def test_writes_payload(self) -> None:
        fn = _handler()
        payload = {
            "version": 1,
            "bindings": [{"kind": "trigger", "parameter": "events"}],
            "injections": [],
        }
        merge_db_metadata(fn, payload)  # type: ignore[arg-type]

        meta = getattr(fn, METADATA_ATTR)
        assert meta == {NAMESPACE: payload}

    def test_concatenates_bindings_and_injections(self) -> None:
        fn = _handler()
        merge_db_metadata(
            fn,
            {
                "version": 1,
                "bindings": [{"kind": "trigger", "parameter": "events"}],
                "injections": [],
            },
        )
        merge_db_metadata(
            fn,
            {
                "version": 1,
                "bindings": [{"kind": "output", "parameter": "out"}],
                "injections": [{"kind": "reader", "parameter": "reader"}],
            },
        )

        db_meta = getattr(fn, METADATA_ATTR)[NAMESPACE]
        assert [b["parameter"] for b in db_meta["bindings"]] == ["events", "out"]
        assert [i["parameter"] for i in db_meta["injections"]] == ["reader"]

    def test_preserves_other_namespaces(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, {"logging": {"version": 1}})
        merge_db_metadata(
            fn,
            {"version": 1, "bindings": [], "injections": []},
        )

        meta = getattr(fn, METADATA_ATTR)
        assert meta["logging"] == {"version": 1}
        assert meta[NAMESPACE] == {"version": 1, "bindings": [], "injections": []}

    def test_ignores_non_dict_attr(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, "invalid")
        merge_db_metadata(
            fn,
            {"version": 1, "bindings": [], "injections": []},
        )

        meta = getattr(fn, METADATA_ATTR)
        assert meta == {NAMESPACE: {"version": 1, "bindings": [], "injections": []}}

    def test_ignores_non_dict_namespace(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, {NAMESPACE: "bad"})
        merge_db_metadata(
            fn,
            {"version": 1, "bindings": [], "injections": []},
        )

        db_meta = getattr(fn, METADATA_ATTR)[NAMESPACE]
        assert db_meta == {"version": 1, "bindings": [], "injections": []}


class TestReadDbMetadata:
    def test_returns_payload_when_present(self) -> None:
        fn = _handler()
        payload = {"version": 1, "bindings": [], "injections": []}
        setattr(fn, METADATA_ATTR, {NAMESPACE: payload})

        assert read_db_metadata(fn) == payload

    def test_returns_none_when_attr_missing(self) -> None:
        assert read_db_metadata(_handler()) is None

    def test_returns_none_when_attr_not_dict(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, "invalid")
        assert read_db_metadata(fn) is None

    def test_returns_none_when_namespace_missing(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, {"logging": {"version": 1}})
        assert read_db_metadata(fn) is None

    def test_returns_none_when_namespace_not_dict(self) -> None:
        fn = _handler()
        setattr(fn, METADATA_ATTR, {NAMESPACE: "bad"})
        assert read_db_metadata(fn) is None


class TestMergeShimGenericNamespace:
    """The decorator shim also supports non-``db`` namespaces generically."""

    def test_writes_foreign_namespace(self) -> None:
        from azure_functions_db import decorator as decorator_mod

        fn = _handler()
        decorator_mod._merge_toolkit_metadata(fn, "other", {"version": 1})
        assert getattr(fn, _metadata.METADATA_ATTR) == {"other": {"version": 1}}

    def test_foreign_namespace_ignores_non_dict_attr(self) -> None:
        from azure_functions_db import decorator as decorator_mod

        fn = _handler()
        setattr(fn, _metadata.METADATA_ATTR, "invalid")
        decorator_mod._merge_toolkit_metadata(fn, "other", {"version": 1})
        assert getattr(fn, _metadata.METADATA_ATTR) == {"other": {"version": 1}}
