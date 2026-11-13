"""Offline checkpoint/replay recipe over real SQLite and a JSON checkpoint file (#517).

Existing runner tests use in-memory fakes; this one drives ``SqlAlchemySource`` +
``PollTrigger`` end to end and proves the checkpoint survives a simulated restart.
It documents at-least-once delivery; it makes no exactly-once or lease claim.
"""

from pathlib import Path
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys

import pytest

from azure_functions_db import HandlerError
from examples import poller_checkpoint_replay as demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def outcome(tmp_path: Path) -> dict[str, object]:
    result: dict[str, object] = demo.run_demo(tmp_path)
    return result


def test_successful_tick_commits_cursor_and_primary_key(outcome: dict[str, object]) -> None:
    assert outcome["tick1_count"] == 2
    assert outcome["checkpoint_after_success"] == [20, 2]


def test_failed_handler_does_not_advance_checkpoint(outcome: dict[str, object]) -> None:
    assert outcome["tick2_error"] == "HandlerError"
    assert outcome["checkpoint_after_failure"] == outcome["checkpoint_after_success"]


def test_restart_replays_the_failed_batch_so_delivery_is_at_least_once(
    outcome: dict[str, object],
) -> None:
    # Row 3 was handled before the failure and is delivered again after the restart.
    assert outcome["deliveries_before_restart"] == [1, 2, 3]
    assert outcome["tick3_count"] == 2
    assert outcome["deliveries"] == [1, 2, 3, 3, 4]
    assert outcome["checkpoint_after_replay"] == [40, 4]


def test_idempotent_sink_applies_each_event_once_despite_replays(
    outcome: dict[str, object],
) -> None:
    assert outcome["applied_once"] == [1, 2, 3, 4]
    assert outcome["final_applied_once"] == [1, 2, 3, 4, 5, 6]


def test_commit_failure_after_successful_handler_also_replays(outcome: dict[str, object]) -> None:
    assert outcome["tick4_error"] == "CommitError"
    assert outcome["checkpoint_after_commit_failure"] == [40, 4]
    assert outcome["tick5_count"] == 2
    assert outcome["final_deliveries"] == [1, 2, 3, 3, 4, 5, 6, 5, 6]


def test_replayed_event_keeps_the_same_event_id(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'orders.db'}"
    demo.seed(url)
    store = demo.LocalFileStateStore(tmp_path / "checkpoint.json")
    seen: list[str] = []

    def fail_once(events: list[object]) -> None:
        seen.extend(str(getattr(event, "event_id")) for event in events)
        if len(seen) == 2:
            raise RuntimeError("boom")

    trigger = demo.make_trigger(url, store)
    with pytest.raises(HandlerError):
        trigger.run(timer=None, handler=fail_once)
    assert store.load_checkpoint(demo.POLLER_NAME) == {}
    demo.make_trigger(url, store).run(timer=None, handler=fail_once)
    assert seen[:2] == seen[2:4]


def test_example_script_runs_offline() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "poller_checkpoint_replay.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "deliveries: [1, 2, 3, 3, 4]" in result.stdout
    assert "tick4_error: CommitError" in result.stdout


def test_docs_link_recipe_and_state_the_guarantee() -> None:
    text = " ".join((ROOT / "docs" / "24-polling-runtime-semantics.md").read_text().split())
    assert "examples/poller_checkpoint_replay.py" in text
    assert "at-least-once" in text and "not exactly-once" in text
    assert "tests/test_poller_checkpoint_replay_example.py" in text
