"""Offline contract for duplicate SQL result labels (#520); SQLite only, no services."""

from pathlib import Path
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys

import pytest

from azure_functions_db import DbReader, QueryError
from examples import duplicate_result_labels as demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def url(tmp_path: Path) -> str:
    created: str = demo.create_database(tmp_path / "labels.db")
    return created


def test_scalar_returns_first_column_by_position(url: str) -> None:
    assert demo.run(url)["scalar"] == "user-name"


def test_query_and_one_collapse_to_the_last_column_with_that_label(url: str) -> None:
    results = demo.run(url)
    assert results["query"] == [{"name": "account-name", "id": 1}]
    assert results["one"] == {"name": "account-name", "id": 1}


def test_query_matches_sqlalchemy_row_mapping_and_keeps_both_driver_keys(url: str) -> None:
    driver = demo.driver_mapping(url)
    assert driver["keys"] == ["name", "name", "id"]
    assert driver["mapping"] == demo.run(url)["one"]


def test_distinct_labels_keep_every_value(url: str) -> None:
    sql = demo.DUPLICATE_LABEL_SQL.replace("a.name AS name", "a.name AS account_name")
    with DbReader(url=url) as reader:
        assert reader.one(sql, params={"uid": 1}) == {
            "name": "user-name",
            "account_name": "account-name",
            "id": 1,
        }


def test_scalar_on_multiple_rows_still_raises(url: str) -> None:
    with DbReader(url=url) as reader, pytest.raises(QueryError, match="multiple"):
        reader.scalar("SELECT name, name FROM (SELECT 'a' AS name UNION ALL SELECT 'b')", params={})


def test_example_script_runs_offline() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "duplicate_result_labels.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "scalar : user-name" in result.stdout
    assert "'name': 'account-name'" in result.stdout


def test_docs_link_the_recipe_with_the_documented_values() -> None:
    text = (ROOT / "docs" / "22-binding-semantics.md").read_text()
    assert "examples/duplicate_result_labels.py" in text
    assert "user-name" in text and "account-name" in text
