import json
from pathlib import Path
import subprocess

import pytest

from tools.dependency_audit_policy import run_audit
from tools.security_report import Status


def install_metadata(root: Path, name: str, version: str) -> None:
    directory = root / f"{name}-{version}.dist-info"
    directory.mkdir()
    (directory / "METADATA").write_text(f"Name: {name}\nVersion: {version}\n")


@pytest.mark.parametrize("outcome", ["clean", "advisory", "skip", "missing"])
def test_verified_unpublished_candidate_excludes_only_itself(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    install_metadata(tmp_path, "azure-functions-db", "999.123")
    install_metadata(tmp_path, "third-party", "1.2")
    policy = tmp_path / "policy.json"
    policy.write_text('{"schema_version":1,"blocking":"all-known-runtime","exceptions":[]}')

    def scan(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        requirements = Path(command[command.index("--requirement") + 1]).read_text()
        assert requirements == "third-party==1.2\n"
        assert "--no-deps" in command and "--disable-pip" in command
        dependency: dict[str, object] = {"name": "third-party", "version": "1.2", "vulns": []}
        if outcome == "advisory":
            dependency["vulns"] = [{"id": "CVE-2026-123", "fix_versions": []}]
        if outcome == "skip":
            dependency = {"name": "third-party", "skip_reason": "not on PyPI"}
        dependencies = [] if outcome == "missing" else [dependency]
        return subprocess.CompletedProcess(
            command, int(outcome == "advisory"), json.dumps({"dependencies": dependencies})
        )

    monkeypatch.setattr("tools.dependency_audit_policy.subprocess.run", scan)
    monkeypatch.setattr("tools.dependency_audit_policy.version", lambda name: "2.10.1")
    result = run_audit(tmp_path, policy, candidate=("azure-functions-db", "999.123"))
    assert (
        result.status
        == {
            "clean": Status.PASS,
            "advisory": Status.FAIL,
            "skip": Status.INCOMPLETE,
            "missing": Status.INCOMPLETE,
        }[outcome]
    )


def test_candidate_identity_mismatch_blocks_before_scanner(tmp_path: Path) -> None:
    install_metadata(tmp_path, "azure-functions-db", "0.1")
    result = run_audit(tmp_path, tmp_path / "policy", candidate=("azure-functions-db", "999"))
    assert result.status == Status.INCOMPLETE
