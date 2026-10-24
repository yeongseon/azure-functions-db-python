import json
from pathlib import Path
import subprocess
import sys

import pytest

from tools import dependency_audit_policy, security_evidence
from tools.security_report import Check, Status


@pytest.mark.parametrize("failure", [OSError(), subprocess.TimeoutExpired("scanner", 1)])
def test_tool_failure_never_exports_raw_diagnostics(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: OSError | subprocess.TimeoutExpired
) -> None:
    # Given unavailable tools; When invoked; Then no raw diagnostics are retained.
    from tests.test_dependency_candidate import install_metadata

    install_metadata(tmp_path, "candidate", "1")
    install_metadata(tmp_path, "example", "1")

    def fail(*args: str, **kwargs: str) -> subprocess.CompletedProcess[str]:
        raise failure

    monkeypatch.setattr(subprocess, "run", fail)
    assert security_evidence.run_bandit().status == Status.TOOL_ERROR
    assert (
        dependency_audit_policy.run_audit(tmp_path, tmp_path, candidate=("candidate", "1")).status
        == Status.TOOL_ERROR
    )


def test_bandit_inventory_matches_every_declared_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given a complete scan; When a file disappears from evidence; Then coverage fails closed.
    metrics = {
        p.as_posix(): {"loc": 1}
        for surface in security_evidence.SCOPE
        for p in Path(surface).rglob("*.py")
    }
    metrics["_totals"] = {"loc": len(metrics)}
    raw = json.dumps({"errors": [], "metrics": metrics, "results": []})

    def complete(command: list[str], **kwargs: str) -> subprocess.CompletedProcess[str]:
        assert "--ignore-nosec" in command
        return subprocess.CompletedProcess(command, 0, stdout=raw, stderr="CANARY_SECRET")

    monkeypatch.setattr(subprocess, "run", complete)
    monkeypatch.setattr(security_evidence, "version", lambda name: "1.9.4")
    assert security_evidence.run_bandit().status == Status.PASS
    del metrics[next(iter(metrics))]
    raw = json.dumps({"errors": [], "metrics": metrics, "results": []})
    assert security_evidence.run_bandit().status == Status.INCOMPLETE


def test_audit_scans_only_candidate_installation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Given an isolated runtime; When audited; Then tooling cannot contaminate the closure.
    from tests.test_dependency_candidate import install_metadata

    install_metadata(tmp_path, "candidate", "1")
    install_metadata(tmp_path, "example", "1")
    policy = tmp_path / "policy.json"
    policy.write_text('{"schema_version":1,"blocking":"all-known-runtime","exceptions":[]}')

    def complete(command: list[str], **kwargs: str) -> subprocess.CompletedProcess[str]:
        assert Path(command[command.index("--requirement") + 1]).read_text() == "example==1\n"
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=('{"dependencies":[{"name":"example","version":"1","vulns":[]}]}'),
            stderr="CANARY_SECRET",
        )

    monkeypatch.setattr(subprocess, "run", complete)
    monkeypatch.setattr(dependency_audit_policy, "version", lambda name: "2.10.1")
    assert (
        dependency_audit_policy.run_audit(tmp_path, policy, candidate=("candidate", "1")).status
        == Status.PASS
    )


@pytest.mark.parametrize(
    "status,code", [(Status.FAIL, 1), (Status.TOOL_ERROR, 2), (Status.INCOMPLETE, 3)]
)
def test_cli_preserves_non_success_codes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, status: Status, code: int
) -> None:
    # Given a classified failure; When CLI writes evidence; Then the failure reaches CI.
    monkeypatch.setattr(
        sys, "argv", ["audit", "--site-packages", str(tmp_path), "--output", str(tmp_path)]
    )
    monkeypatch.setattr(
        dependency_audit_policy,
        "run_audit",
        lambda path, policy, **kwargs: Check("audit", (), status, 1),
    )
    assert dependency_audit_policy.main() == code
    assert json.loads((tmp_path / "summary.json").read_text())["checks"][0]["status"] == status


def test_real_cli_writes_stdlib_only_incomplete_evidence(tmp_path: Path) -> None:
    # Given missing action evidence; When the real CLI runs; Then it exits INCOMPLETE.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.security_evidence",
            "semgrep",
            "--outcome",
            "cancelled",
            "--output",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
    )
    assert result.returncode == 3
    assert json.loads((tmp_path / "summary.json").read_text())["schema_version"] == 1
