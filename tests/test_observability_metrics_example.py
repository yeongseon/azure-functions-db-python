"""Offline metrics recipe: SQLite read plus a deliberately failed query (#525).

Asserts metric names, labels and counts only. Durations and timestamps vary per run
and are never compared against thresholds. DbReader/DbWriter emit no metrics, so the
recipe uses the supported poller ``MetricsCollector`` surface.
"""

from pathlib import Path
import re
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys

import pytest

from azure_functions_db import MetricsCollector
from azure_functions_db.observability import (
    METRIC_BATCH_SIZE,
    METRIC_BATCHES_TOTAL,
    METRIC_COMMIT_DURATION_MS,
    METRIC_EVENTS_TOTAL,
    METRIC_FAILURES_TOTAL,
    METRIC_FETCH_DURATION_MS,
    METRIC_HANDLER_DURATION_MS,
    METRIC_LAST_SUCCESS_TIMESTAMP,
)
from examples import observability_metrics as demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def runs(tmp_path: Path) -> dict[str, demo.RecordingCollector]:
    result: dict[str, demo.RecordingCollector] = demo.run_demo(tmp_path)
    return result


def test_recording_collector_satisfies_the_public_protocol() -> None:
    assert isinstance(demo.RecordingCollector(), MetricsCollector)


def test_successful_read_emits_batch_and_event_counters(
    runs: dict[str, demo.RecordingCollector],
) -> None:
    metrics = runs["success"]
    base = {"poller_name": "orders", "source": "orders"}
    assert metrics.counter(METRIC_BATCHES_TOTAL) == [(1, {**base, "result": "success"})]
    assert metrics.counter(METRIC_EVENTS_TOTAL) == [(2, base)]
    assert metrics.counter(METRIC_FAILURES_TOTAL) == []


def test_successful_read_emits_duration_and_size_series_without_timing_asserts(
    runs: dict[str, demo.RecordingCollector],
) -> None:
    metrics = runs["success"]
    assert {
        METRIC_BATCH_SIZE,
        METRIC_FETCH_DURATION_MS,
        METRIC_HANDLER_DURATION_MS,
        METRIC_COMMIT_DURATION_MS,
        METRIC_LAST_SUCCESS_TIMESTAMP,
    } <= metrics.names()
    for kind, name, value, _ in metrics.samples:
        if kind == "histogram" and name.endswith("_duration_ms"):
            assert value >= 0  # sign only: never a flaky threshold


def test_failed_query_emits_failure_counters_and_no_success_series(
    runs: dict[str, demo.RecordingCollector],
) -> None:
    metrics = runs["failure"]
    failures = metrics.counter(METRIC_FAILURES_TOTAL)
    assert len(failures) == 1
    value, labels = failures[0]
    assert value == 1
    assert labels["error_type"] == "FetchError"
    assert labels["poller_name"] == "orders"
    batches = metrics.counter(METRIC_BATCHES_TOTAL)
    assert [(v, lab["result"]) for v, lab in batches] == [(1, "failure")]
    assert metrics.counter(METRIC_EVENTS_TOTAL) == []
    assert METRIC_LAST_SUCCESS_TIMESTAMP not in metrics.names()


def test_labels_never_carry_query_text_credentials_or_row_values(
    runs: dict[str, demo.RecordingCollector],
) -> None:
    forbidden = (demo.CANARY_ROW_VALUE, "table_that_does_not_exist", "SELECT", "sqlite:///")
    for metrics in runs.values():
        for _, _, _, labels in metrics.samples:
            assert set(labels) <= demo.ALLOWED_LABELS
            for value in labels.values():
                assert not any(token in value for token in forbidden)
    source = runs["failure"].counter(METRIC_FAILURES_TOTAL)[0][1]["source"]
    assert re.fullmatch(r"query_[0-9a-f]{12}", source)


def test_example_script_runs_offline_and_hides_values() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "observability_metrics.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "azfdb_failures_total 1" in result.stdout
    assert "'error_type': 'FetchError'" in result.stdout
    assert demo.CANARY_ROW_VALUE not in result.stdout
    assert "table_that_does_not_exist" not in result.stdout


def test_guide_states_scope_and_redaction_rules() -> None:
    text = " ".join((ROOT / "docs" / "11-observability.md").read_text().split())
    assert "examples/observability_metrics.py" in text
    assert "`DbReader` and `DbWriter` emit no metrics" in text
    assert "query text, connection credentials and record values are never metric labels" in text
