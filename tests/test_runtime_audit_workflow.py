"""The release publishes only after the candidate's runtime closure passes the advisory gate."""

from __future__ import annotations

from pathlib import Path
import re
import tomllib
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _gate() -> tuple[dict[str, Any], dict[str, Any]]:
    jobs = yaml.safe_load((ROOT / ".github/workflows/publish-pypi.yml").read_text())["jobs"]
    return jobs["publish"], jobs["runtime-dependency-audit"]


def _pyproject() -> dict[str, Any]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


def test_publish_needs_the_audit_and_no_step_can_swallow_its_failure() -> None:
    publish, gate = _gate()
    assert "runtime-dependency-audit" in publish["needs"]
    assert gate["needs"] == "build"
    assert gate["permissions"] == {"contents": "read"}
    assert not gate.get("continue-on-error")
    assert "if" not in gate
    assert not any(
        s.get("continue-on-error") or "|| true" in s.get("run", "") for s in gate["steps"]
    )


def test_audit_scans_the_downloaded_wheel_and_cannot_pass_vacuously() -> None:
    _, gate = _gate()
    download = next(s for s in gate["steps"] if "download-artifact" in s.get("uses", ""))
    step = next(s for s in gate["steps"] if "pip_audit" in s.get("run", ""))
    commands = [line for line in step["run"].splitlines() if not line.lstrip().startswith("#")]
    audit = next(line for line in commands if "pip_audit" in line).split()
    assert download["with"]["name"] == "dist"
    assert "if" not in step
    assert "set -euo pipefail" in commands
    assert any(f"{download['with']['path']}/*.whl)[all]" in line for line in commands)
    assert any(line.startswith("test -s req.txt") for line in commands)
    assert {"--strict", "--no-deps", "--disable-pip"} <= set(audit)


def test_audit_covers_every_supported_python() -> None:
    _, gate = _gate()
    supported = {
        f"{m[1]}.{m[2]}"
        for c in _pyproject()["classifiers"]
        if (m := re.fullmatch(r"Programming Language :: Python :: (\d+)\.(\d+)", c))
    }
    assert set(gate["strategy"]["matrix"]["python-version"]) == supported


def test_all_extra_is_the_union_of_the_database_driver_extras() -> None:
    extras = _pyproject()["optional-dependencies"]
    drivers = (set(reqs) for name, reqs in extras.items() if name not in {"all", "dev", "docs"})
    assert set(extras["all"]) == set().union(*drivers)
