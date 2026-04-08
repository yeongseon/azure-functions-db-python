from azure_functions_db.core.types import CursorValue, JsonScalar, SourceDescriptor


class TestSourceDescriptor:
    def test_frozen(self) -> None:
        sd = SourceDescriptor(name="orders", kind="sqlalchemy", fingerprint="abc123")
        assert sd.name == "orders"
        assert sd.kind == "sqlalchemy"
        assert sd.fingerprint == "abc123"

    def test_immutable(self) -> None:
        sd = SourceDescriptor(name="orders", kind="sqlalchemy", fingerprint="abc123")
        try:
            sd.name = "other"  # type: ignore[misc]
            raise AssertionError("Expected FrozenInstanceError")
        except AttributeError:
            pass

    def test_equality(self) -> None:
        sd1 = SourceDescriptor(name="a", kind="b", fingerprint="c")
        sd2 = SourceDescriptor(name="a", kind="b", fingerprint="c")
        assert sd1 == sd2

    def test_inequality(self) -> None:
        sd1 = SourceDescriptor(name="a", kind="b", fingerprint="c")
        sd2 = SourceDescriptor(name="x", kind="b", fingerprint="c")
        assert sd1 != sd2


class TestTypeAliases:
    def test_json_scalar_types(self) -> None:
        values: list[JsonScalar] = ["hello", 42, 3.14, True, None]
        assert len(values) == 5

    def test_cursor_value_scalar(self) -> None:
        cursor: CursorValue = 42
        assert cursor == 42

    def test_cursor_value_tuple(self) -> None:
        cursor: CursorValue = ("2026-01-01", 100)
        assert cursor == ("2026-01-01", 100)

    def test_cursor_value_none(self) -> None:
        cursor: CursorValue = None
        assert cursor is None
