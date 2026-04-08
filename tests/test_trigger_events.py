import pytest

from azure_functions_db.core.types import SourceDescriptor
from azure_functions_db.trigger.events import RowChange


def _make_source() -> SourceDescriptor:
    return SourceDescriptor(name="orders", kind="sqlalchemy", fingerprint="fp1")


class TestRowChange:
    def test_valid_ops(self) -> None:
        for op in ("insert", "update", "upsert", "delete", "unknown"):
            rc = RowChange(
                event_id="evt1",
                op=op,
                source=_make_source(),
                cursor=1,
                pk={"id": 1},
                before=None,
                after={"id": 1, "name": "test"},
            )
            assert rc.op == op

    def test_invalid_op_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid op"):
            RowChange(
                event_id="evt1",
                op="invalid",
                source=_make_source(),
                cursor=1,
                pk={"id": 1},
                before=None,
                after=None,
            )

    def test_empty_event_id_raises(self) -> None:
        with pytest.raises(ValueError, match="event_id must not be empty"):
            RowChange(
                event_id="",
                op="upsert",
                source=_make_source(),
                cursor=1,
                pk={"id": 1},
                before=None,
                after=None,
            )

    def test_frozen(self) -> None:
        rc = RowChange(
            event_id="evt1",
            op="upsert",
            source=_make_source(),
            cursor=1,
            pk={"id": 1},
            before=None,
            after={"id": 1},
        )
        with pytest.raises(AttributeError):
            rc.op = "insert"  # type: ignore[misc]

    def test_default_metadata(self) -> None:
        rc = RowChange(
            event_id="evt1",
            op="upsert",
            source=_make_source(),
            cursor=1,
            pk={"id": 1},
            before=None,
            after=None,
        )
        assert rc.metadata == {}

    def test_with_metadata(self) -> None:
        rc = RowChange(
            event_id="evt1",
            op="upsert",
            source=_make_source(),
            cursor=1,
            pk={"id": 1},
            before=None,
            after=None,
            metadata={"batch_id": "b1"},
        )
        assert rc.metadata == {"batch_id": "b1"}

    def test_with_before_and_after(self) -> None:
        rc = RowChange(
            event_id="evt1",
            op="update",
            source=_make_source(),
            cursor=2,
            pk={"id": 1},
            before={"name": "old"},
            after={"name": "new"},
        )
        assert rc.before == {"name": "old"}
        assert rc.after == {"name": "new"}
