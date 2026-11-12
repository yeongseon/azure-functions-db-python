"""Offline EngineProvider reuse/separation recipe over two SQLite files (#523).

Existing tests cover single-URL caching (test_shared_core.py) and reader+writer sharing
(test_binding_integration.py). This adds the guard against sharing an engine, or rows,
across distinct configurations, and the ownership/cleanup contract for warm invocations.
"""

from pathlib import Path
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Engine

from azure_functions_db import DbConfig, EngineProvider
from examples import engine_provider_reuse as demo

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def outcome(tmp_path: Path) -> dict[str, object]:
    result: dict[str, object] = demo.run_demo(tmp_path)
    return result


def test_equal_configs_share_one_engine(outcome: dict[str, object]) -> None:
    assert outcome["equal_config_shares_engine"] is True


def test_distinct_url_or_engine_kwargs_never_share_an_engine(outcome: dict[str, object]) -> None:
    assert outcome["other_url_separate_engine"] is True
    assert outcome["other_engine_kwargs_separate_engine"] is True


def test_distinct_configs_read_and_write_independent_data(outcome: dict[str, object]) -> None:
    assert outcome["read_a"] == "database-a"
    assert outcome["read_b"] == "database-b"
    assert outcome["read_a_after_update"] == "database-a-updated"
    assert outcome["read_b_after_update"] == "database-b"


def test_warm_invocations_reuse_the_engine_and_dispose_all_rebuilds(
    outcome: dict[str, object],
) -> None:
    assert outcome["engine_stable_across_invocations"] is True
    assert outcome["engine_rebuilt_after_dispose_all"] is True


def test_closing_readers_never_disposes_provider_engines(tmp_path: Path) -> None:
    url = demo.create_database(tmp_path / "own.db", "x")
    config = DbConfig(connection_url=url)
    provider = EngineProvider()
    try:
        with patch.object(Engine, "dispose", autospec=True, side_effect=Engine.dispose) as dispose:
            for _ in range(3):
                assert demo.read_label(provider, config) == "x"
            assert dispose.call_count == 0  # reader/writer close() leave the engine alone
            provider.dispose_all()
            assert dispose.call_count == 1  # the provider owns disposal
    finally:
        provider.dispose_all()


def test_example_script_runs_offline() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "engine_provider_reuse.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "other_url_separate_engine: True" in result.stdout
    assert "read_b_after_update: database-b" in result.stdout


def test_guide_documents_ownership_without_cross_worker_claims() -> None:
    text = " ".join((ROOT / "docs" / "25-engine-provider-pooling.md").read_text().split())
    assert "examples/engine_provider_reuse.py" in text
    assert "never disposes" in text
    assert "does not share a pool across Azure Functions workers" in text
