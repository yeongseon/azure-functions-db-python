"""Deterministic correctness checks for the opt-in catch-up benchmark (#531).

No timing thresholds: only row/batch counts, metadata and failure detection.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from benchmarks import bench_catchup
from benchmarks.bench_catchup import run_catchup


def test_run_processes_every_row_in_expected_batches() -> None:
    result = run_catchup(rows=250, batch_size=100)

    assert result.rows == 250
    assert result.rows_processed == 250
    assert result.batches == result.expected_batches == 3
    assert result.ticks == 2
    assert result.elapsed_seconds >= 0.0


def test_exact_multiple_of_batch_size() -> None:
    result = run_catchup(rows=200, batch_size=100)

    assert (result.rows_processed, result.batches) == (200, 2)


def test_two_runs_report_comparable_metadata_and_counts() -> None:
    first = run_catchup(rows=120, batch_size=50)
    second = run_catchup(rows=120, batch_size=50)

    def stable(result: Any) -> dict[str, Any]:
        return {
            k: v for k, v in vars(result).items() if k not in {"elapsed_seconds", "rows_per_second"}
        }

    assert stable(first) == stable(second)
    assert first.python and first.sqlite and first.sqlalchemy and first.platform


@pytest.mark.parametrize(("rows", "batch_size"), [(0, 10), (10, 0), (-1, 5)])
def test_invalid_parameters_are_rejected(rows: int, batch_size: int) -> None:
    with pytest.raises(ValueError, match=">= 1"):
        run_catchup(rows=rows, batch_size=batch_size)


def test_a_runner_that_processes_nothing_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """The benchmark must fail, not report a throughput, when rows are lost."""

    class DeadRunner:
        def __init__(self, **_: Any) -> None:
            pass

        def tick(self) -> int:
            return 0

    monkeypatch.setattr(bench_catchup, "PollRunner", DeadRunner)

    with pytest.raises(AssertionError, match="processed 0 rows, expected 10"):
        run_catchup(rows=10, batch_size=5)


def test_a_runner_that_never_terminates_fails_the_run(monkeypatch: pytest.MonkeyPatch) -> None:
    class EndlessRunner:
        def __init__(self, **_: Any) -> None:
            pass

        def tick(self) -> int:
            return 1

    monkeypatch.setattr(bench_catchup, "PollRunner", EndlessRunner)

    with pytest.raises(AssertionError, match="did not terminate"):
        run_catchup(rows=10, batch_size=5)


def test_main_prints_json_metadata(capsys: pytest.CaptureFixture[str]) -> None:
    bench_catchup.main(["--rows", "30", "--batch-size", "10", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"] == 30
    assert payload["batch_size"] == 10
    assert payload["rows_processed"] == 30
    assert payload["batches"] == payload["expected_batches"] == 3
    assert {"python", "sqlite", "sqlalchemy", "azure_functions_db", "platform"} <= set(payload)


def test_main_human_output_labels_timing_as_local(capsys: pytest.CaptureFixture[str]) -> None:
    bench_catchup.main(["--rows", "20", "--batch-size", "10"])

    out = capsys.readouterr().out
    assert "Rows processed  : 20" in out
    assert "not a production performance claim" in out
