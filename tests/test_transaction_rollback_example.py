"""Offline rollback-boundary recipe for ``DbWriter.transaction()`` on SQLite (#521).

Scope: the tested API and backend only. Unit tests in test_binding_writer_transaction.py
cover internals with mocks; this pins the user-visible commit/rollback boundary through
the documented recipe. No Azure, no network, no exactly-once claim.
"""

from pathlib import Path
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys

import pytest

from azure_functions_db import WriteError
from examples import transaction_rollback as demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def url(tmp_path: Path) -> str:
    created: str = demo.create_database(tmp_path / "orders.db")
    return created


def test_successful_block_commits_every_write(url: str) -> None:
    demo.commit_two_rows(url)
    assert demo.order_ids(url) == [1, 2]


def test_failing_handler_leaves_no_partial_write(url: str) -> None:
    with pytest.raises(RuntimeError, match="after the first write"):
        demo.handler_raises_after_first_insert(url)
    assert demo.order_ids(url) == []


def test_write_error_rolls_back_earlier_writes_in_the_block(url: str) -> None:
    demo.commit_two_rows(url)
    error = demo.second_statement_violates_primary_key(url)
    assert isinstance(error, WriteError)
    assert demo.order_ids(url) == [1, 2]  # id 20 was rolled back; committed rows untouched


def test_write_outside_the_block_is_its_own_transaction(url: str) -> None:
    with pytest.raises(RuntimeError, match="inside the block"):
        demo.write_outside_transaction_then_fail(url)
    assert demo.order_ids(url) == [30]  # 31 (inside the block) rolled back, 30 committed


def test_run_demo_reports_each_boundary(tmp_path: Path) -> None:
    assert demo.run_demo(tmp_path) == {
        "after_commit": [1, 2],
        "after_handler_failure": [1, 2],
        "after_write_error": [1, 2],
        "after_outside_write_then_failure": [1, 2, 30],
    }


def test_example_script_runs_offline() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "transaction_rollback.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "after_outside_write_then_failure: [1, 2, 30]" in result.stdout


def test_guide_is_linked_and_states_the_boundary() -> None:
    guide = " ".join((ROOT / "docs" / "examples" / "transaction_rollback.md").read_text().split())
    assert "examples/transaction_rollback.py" in guide
    assert "does **not** make trigger delivery exactly-once" in guide
    assert "SQLite" in guide
    assert "examples/transaction_rollback.md" in (ROOT / "mkdocs.yml").read_text()
    assert "examples/transaction_rollback/" in (ROOT / "docs" / "llms.txt").read_text()
    assert (
        "transaction_rollback.md" in (ROOT / "docs" / "examples" / "output_binding.md").read_text()
    )
