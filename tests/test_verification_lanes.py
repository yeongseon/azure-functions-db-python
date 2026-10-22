from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tools.verification import aggregate, verify
from tools.verification_environment import clean
from tools.verification_evidence import Summary, collect, save
from tools.verification_lanes import backend_lane
from tools.verification_services import MissingPrerequisite, ServiceFailure


@pytest.mark.parametrize("backend", ["postgres", "azurite"])
@pytest.mark.parametrize(
    "failure", [ServiceFailure("startup failed"), MissingPrerequisite("missing tool")]
)
def test_service_startup_failure_never_passes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    failure: ServiceFailure | MissingPrerequisite,
) -> None:
    # Given a required service that cannot start.
    @contextmanager
    def broken_service(*args: str | Path) -> Iterator[None]:
        raise failure
        yield

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("tools.verification_lanes.database", broken_service)
    monkeypatch.setattr("tools.verification_lanes.azurite", broken_service)
    # When running the required owned backend.
    summary = backend_lane(backend, owned=True)
    # Then neither startup nor prerequisite failure can certify execution.
    assert summary.status != "PASS"
    assert summary.exit_code != 0
    assert summary.passed == 0


@pytest.mark.parametrize("backend", ["postgres", "azurite"])
@pytest.mark.parametrize(
    ("junit", "code"),
    [
        ("<testcase><failure/></testcase>", 1),
        ("", 5),
        ("<testcase><skipped/></testcase>", 0),
        ("<testcase/>", 124),
    ],
)
def test_backend_process_outcomes_cannot_be_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend: str,
    junit: str,
    code: int,
) -> None:
    # Given a ready service but a failing, empty, skipped or timed-out selection.
    def fake_run(command: list[str], log: Path, timeout: float = 600) -> int:
        destination = next(
            part.split("=", 1)[1] for part in command if part.startswith("--junitxml=")
        )
        Path(destination).write_text(f"<testsuite>{junit}</testsuite>")
        return code

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("tools.verification_lanes.preflight", lambda *args, **kwargs: None)
    monkeypatch.setattr("tools.verification_lanes.run", fake_run)
    # When executing the required backend or Azurite lane.
    summary = backend_lane(backend)
    # Then no fallback selection or exit-code rewrite produces PASS.
    assert summary.status != "PASS"
    assert summary.exit_code != 0


def test_clean_preserves_external_resources_and_environments(tmp_path: Path) -> None:
    # Given generated artifacts alongside user-managed resources.
    for name in (".venv", "user.db", "shared-azurite", "test-results", "dist"):
        (tmp_path / name).mkdir()
    # When cleaning generated output.
    clean(tmp_path)
    # Then unrelated resources remain untouched.
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        ".venv",
        "shared-azurite",
        "user.db",
    ]


def test_full_aggregate_requires_every_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Given a successful SQLite lane and missing PostgreSQL execution.
    monkeypatch.chdir(tmp_path)
    summaries = [
        Summary(1, "sqlite", (), 1, 1, 0, 0, "PASS", 0),
        Summary(1, "postgres", (), 0, 0, 0, 0, "INCOMPLETE", 2),
    ]
    # When combining the required lanes.
    summary = aggregate("integration", summaries)
    # Then a partial success cannot become a full certificate.
    assert summary.status == "INCOMPLETE"
    assert summary.scope == "PARTIAL"


def test_verify_clears_focus_and_rejects_stale_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given stale successful evidence and a focused backend selection.
    calls: list[str] = []

    def fake_run(command: list[str], log: Path, timeout: float = 600) -> int:
        assert os.environ["DB_BACKEND"] == ""
        calls.append(command[1])
        return 1

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DB_BACKEND", "sqlite")
    save(Path("test-results/test"), Summary(1, "test", (), 1, 1, 0, 0, "PASS", 0))
    monkeypatch.setattr("tools.verification.run", fake_run)
    # When full verification runs, then it continues and cannot reuse stale PASS.
    assert verify() != 0
    assert calls == ["check", "test", "integration", "host-smoke", "wheel-smoke", "build", "docs"]
    lanes = json.loads(Path("test-results/verify/lanes.json").read_text())
    assert lanes[1]["status"] == "INCOMPLETE"


def test_real_failing_azurite_selection_exits_nonzero(tmp_path: Path) -> None:
    # Given a deliberately failing pytest test with the required Azurite markers.
    test = tmp_path / "test_failure.py"
    test.write_text("def test_required_azurite():\n    assert False\n")
    # When exercising the real process and classifier, not a mocked exit code.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(test),
            "-o",
            "addopts=",
            "--junitxml=" + str(tmp_path / "junit.xml"),
        ],
        capture_output=True,
    )
    # Then the same required policy yields a red gate.
    summary = collect(tmp_path, ["pytest"], result.returncode)
    assert summary.status == "FAIL"
    assert summary.exit_code != 0
