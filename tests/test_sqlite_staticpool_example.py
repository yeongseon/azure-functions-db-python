"""Offline StaticPool + EngineProvider recipe over in-memory SQLite (#522).

``tests/test_shared_core.py`` proves a StaticPool config can be built; this runs it
end to end (write, two sequential reads, explicit cleanup) and pins the guide.
"""

from pathlib import Path
import subprocess  # nosec B404  # runs the repo's own example with a fixed argv
import sys
from unittest.mock import patch

from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

from azure_functions_db import EngineProvider
from examples import sqlite_staticpool as demo

ROOT = Path(__file__).resolve().parents[1]


def test_demo_uses_static_pool_and_sequential_queries_share_state() -> None:
    seen = demo.run_demo()
    assert seen["pool"] == "StaticPool"
    assert seen["first_query"] == [{"id": 1, "body": "first"}]
    assert seen["second_query"] == 2


def test_closing_reader_and_writer_does_not_dispose_provider_engine() -> None:
    assert demo.run_demo()["same_engine_after_close"] is True


def test_dispose_all_drops_the_in_memory_database() -> None:
    seen = demo.run_demo()
    assert seen["rebuilt_is_new_engine"] is True
    assert seen["tables_after_dispose"] == []


def test_every_engine_is_disposed_even_on_the_cleanup_path() -> None:
    # One dispose for the first engine, one for the rebuilt engine (finally block).
    with patch.object(Engine, "dispose", autospec=True, side_effect=Engine.dispose) as dispose:
        demo.run_demo()
    assert dispose.call_count == 2


def test_config_is_the_documented_one() -> None:
    assert demo.CONFIG.connection_url == "sqlite://"
    assert demo.CONFIG.connect_args == {"check_same_thread": False}
    assert demo.CONFIG.engine_kwargs == {"poolclass": StaticPool}
    provider = EngineProvider()
    try:
        # pool_size must not be forwarded to StaticPool (it would be rejected).
        assert isinstance(provider.get_engine(demo.CONFIG).pool, StaticPool)
    finally:
        provider.dispose_all()


def test_example_script_runs_offline() -> None:
    result = subprocess.run(  # nosec B603  # fixed argv, no shell
        [sys.executable, str(ROOT / "examples" / "sqlite_staticpool.py")],
        capture_output=True,
        text=True,
        check=True,
        cwd=ROOT,
        env={"PYTHONPATH": str(ROOT / "src"), "PATH": ""},
    )
    assert "pool: StaticPool" in result.stdout
    assert "tables_after_dispose: []" in result.stdout


def test_guide_links_recipe_and_warns_about_scale_out() -> None:
    text = " ".join((ROOT / "docs" / "25-engine-provider-pooling.md").read_text().split())
    assert "examples/sqlite_staticpool.py" in text
    assert "not a production pool" in text
    assert "Azure Functions scale-out" in text
