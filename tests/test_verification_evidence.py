from __future__ import annotations

import json
from pathlib import Path
import sys
from xml.sax.saxutils import escape

import pytest

from tools.verification_evidence import collect, publish, redact, save
from tools.verification_process import child, run


@pytest.mark.parametrize(
    ("cases", "code", "status"),
    [
        ("", 0, "INCOMPLETE"),
        ("", 5, "INCOMPLETE"),
        ("<testcase><skipped/></testcase>", 0, "INCOMPLETE"),
        ("<testcase/><testcase><skipped/></testcase>", 0, "INCOMPLETE"),
        ("<testcase><failure/></testcase>", 1, "FAIL"),
        ("<testcase><error/></testcase>", 0, "FAIL"),
        ("<testcase/>", 124, "FAIL"),
        ("<testcase/>", 2, "FAIL"),
        ("<testcase/>", 0, "PASS"),
    ],
)
def test_required_lane_classification(tmp_path: Path, cases: str, code: int, status: str) -> None:
    # Given actual JUnit outcomes and the original subprocess exit code.
    (tmp_path / "junit.xml").write_text(f"<testsuite>{cases}</testsuite>")
    # When classifying a required lane.
    summary = collect(tmp_path, ["pytest"], code)
    # Then missing execution, skips and failures can never pass.
    assert summary.status == status
    assert (summary.exit_code == 0) == (status == "PASS")


@pytest.mark.parametrize(
    "secret",
    [
        "postgresql+psycopg://CANARYUSER:CANARYSECRET@localhost/db",
        "mysql+pymysql://CANARYUSER:CANARYSECRET@localhost/db",
        "mssql+pyodbc://CANARYUSER:CANARYSECRET@localhost/db",
        "AccountKey=CANARYSECRET;",
        "sig=CANARYSECRET&sp=r",
        "sas=CANARYSECRET",
        "SharedAccessSignature=CANARYSECRET;",
        "password=CANARYSECRET",
    ],
)
def test_redacts_failure_messages_logs_and_published_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    secret: str,
) -> None:
    # Given a failing JUnit testcase and log carrying credential canaries.
    (tmp_path / "junit.xml").write_text(
        f"<testsuite><testcase><failure>{escape(secret)}</failure></testcase></testsuite>"
    )
    (tmp_path / "runner.log").write_text(secret)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "job-summary"))
    # When persisting and publishing the actual failure.
    summary = save(tmp_path, collect(tmp_path, ["pytest", secret], 1))
    publish(summary)
    # Then every surface is redacted and remains parseable.
    assert "CANARYSECRET" not in capsys.readouterr().out
    for path in tmp_path.rglob("*"):
        if path.is_file():
            assert "CANARYSECRET" not in path.read_text()
            assert "CANARYUSER" not in path.read_text()
    assert json.loads((tmp_path / "summary.json").read_text())["status"] == "FAIL"


def test_redacts_known_environment_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given an opaque token outside recognizable URL syntax.
    monkeypatch.setenv("CUSTOM_ACCESS_TOKEN", "opaque-secret-canary")
    # When output is sanitized, then the value is removed.
    assert redact("diagnostic opaque-secret-canary") == "diagnostic [REDACTED]"


def test_timeout_preserves_failure_and_reaps_process(tmp_path: Path) -> None:
    # Given a command that cannot finish within the deadline.
    command = [sys.executable, "-c", "import time; time.sleep(60)"]
    # When running with a bounded timeout, then timeout is nonzero.
    assert run(command, tmp_path / "log", 0.1) == 124


def test_interrupt_reaps_owned_process(tmp_path: Path) -> None:
    # Given an owned child process, when the caller is interrupted.
    with (
        pytest.raises(KeyboardInterrupt),
        child(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            tmp_path / "log",
        ) as process,
    ):
        raise KeyboardInterrupt
    # Then its context manager has waited for termination.
    assert process.poll() is not None
