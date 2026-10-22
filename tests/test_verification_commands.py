from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("backend", ["postgres", "mysql", "mssql", "azurite"])
def test_missing_required_connection_fails_with_evidence(tmp_path: Path, backend: str) -> None:
    # Given an isolated checkout context with no external connection configuration.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("TEST_")}
    environment["PYTHONPATH"] = str(ROOT)
    # When the real lane CLI is invoked.
    result = subprocess.run(
        [sys.executable, "-m", "tools.verification", "backend", backend],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    # Then it preserves non-success and records why no tests executed.
    assert result.returncode != 0
    summary = tmp_path / f"test-results/integration/{backend}/summary.json"
    assert summary.exists(), result.stderr
    assert json.loads(summary.read_text())["status"] == "INCOMPLETE"


def test_unconfigured_azure_fails_closed(tmp_path: Path) -> None:
    # Given no configured deployment or candidate identity.
    environment = dict(os.environ, PYTHONPATH=str(ROOT))
    for key in ("E2E_BASE_URL", "EXPECTED_VERSION"):
        environment.pop(key, None)
    # When invoking the real entrypoint.
    result = subprocess.run(
        [sys.executable, "-m", "tools.verification", "azure-e2e"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )
    # Then absence of prerequisites never certifies Azure.
    assert result.returncode != 0
    summary = tmp_path / "test-results/azure-e2e/summary.json"
    assert summary.exists(), result.stderr
    assert json.loads(summary.read_text())["status"] == "INCOMPLETE"


@pytest.mark.parametrize("target", ["check", "test", "integration", "verify", "report", "clean"])
def test_routine_commands_do_not_install_tools(target: str) -> None:
    # Given the command contract, when expanding its recipe.
    result = subprocess.run(["make", "-n", target], cwd=ROOT, capture_output=True, text=True)
    # Then installation is explicit, never a hidden upgrade.
    assert result.returncode == 0, result.stderr
    assert "install --upgrade pip" not in result.stdout
    assert "pip install hatch" not in result.stdout


def test_check_all_is_exact_alias_and_ci_preserves_interpreter() -> None:
    # Given the Makefile, when expanding CI's offline target.
    result = subprocess.run(
        ["make", "--no-print-directory", "-n", "test", "CI=true", "PYTHON=matrix-python"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    # Then no Hatch interpreter substitution or additional alias work occurs.
    assert "check-all: verify\n" in (ROOT / "Makefile").read_text()
    assert result.stdout.strip() == "matrix-python -m tools.verification test"
