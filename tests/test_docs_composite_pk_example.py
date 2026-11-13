"""Run the composite primary-key recipe from docs/examples/client_injection.md (#524).

The doc's SQL and Python blocks are executed against a temporary SQLite file, so the
documentation cannot drift from the public ``DbReader.get()`` behavior. Unit-level
composite-key cases already exist in test_binding_reader.py; this pins the user guide.
"""

from pathlib import Path
import re
import sqlite3

import pytest

from azure_functions_db import ConfigurationError, DbReader

DOC = Path(__file__).resolve().parents[1] / "docs" / "examples" / "client_injection.md"


def _section() -> str:
    text = DOC.read_text()
    start = text.index("### Composite primary keys")
    return text[start : text.index("\n!!! warning", start)]


@pytest.fixture
def database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    sql = re.findall(r"```sql\n(.*?)```", _section(), re.S)
    assert len(sql) == 1
    path = tmp_path / "order_items.db"
    connection = sqlite3.connect(path)
    try:
        connection.execute(sql[0].split(";", 1)[0])
        connection.executemany(
            "INSERT INTO order_items VALUES (?, ?, ?)", [(1, 1, 2), (1, 2, 3), (2, 1, 1)]
        )
        connection.commit()
    finally:
        connection.close()
    url = f"sqlite:///{path}"
    monkeypatch.setenv("ORDER_ITEMS_URL", url)
    return url


def test_documented_snippet_prints_the_documented_results(
    database: str, capsys: pytest.CaptureFixture[str]
) -> None:
    blocks = re.findall(r"```python\n(.*?)```", _section(), re.S)
    assert len(blocks) == 1
    exec(compile(blocks[0], str(DOC), "exec"), {})
    lines = capsys.readouterr().out.splitlines()
    assert lines == ["{'order_id': 1, 'item_id': 2, 'qty': 3}", "None"]


def test_both_columns_identify_exactly_one_row(database: str) -> None:
    with DbReader(url=database, table="order_items") as reader:
        assert reader.get(pk={"item_id": 1, "order_id": 2}) == {
            "order_id": 2,
            "item_id": 1,
            "qty": 1,
        }
        assert reader.get(pk={"order_id": 1, "item_id": 3}) is None


def test_partial_or_non_key_columns_are_rejected(database: str) -> None:
    with DbReader(url=database, table="order_items") as reader:
        with pytest.raises(ConfigurationError, match="Incomplete primary key"):
            reader.get(pk={"order_id": 1})
        with pytest.raises(ConfigurationError, match="not part of the primary key"):
            reader.get(pk={"order_id": 1, "item_id": 1, "qty": 2})
