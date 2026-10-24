"""The Bandit gate must fail instead of passing vacuously, proven with real Bandit.

Bandit exits 0 for an empty directory, a missing path, a file it cannot parse and a scope narrowed
by a ``.bandit`` file, so each case builds a scratch tree, runs Bandit with the flags of
``make security`` and hands its JSON report to the guard.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from tools import check_bandit_report

_OK = "x = 1\n"
_SHELL_TRUE = "import subprocess\nsubprocess.call('echo', shell=True)\n"
_NEW_CALL = "import subprocess\nimport sys\n\nsubprocess.run([sys.argv[1]], check=True)\n"
_SUPPRESSED = (
    "import subprocess  # nosec B404 # test fixture\n"
    "subprocess.call('echo', shell=True)  # nosec B602 B607 # test fixture\n"
)


@pytest.mark.parametrize(
    ("files", "exit_code", "message"),
    [
        pytest.param({"ok.py": _OK}, 0, "no findings", id="clean-tree"),
        # Bandit 1.9.4 draws a progress bar on stdout above 50 files; the report must still parse.
        pytest.param({f"m{n}.py": _OK for n in range(60)}, 0, "no findings", id="over-50-files"),
        pytest.param({"ok.py": _SUPPRESSED}, 0, "no findings", id="inline-nosec-with-reason"),
        pytest.param({}, 1, "no code was analysed", id="empty-dir"),
        pytest.param({"empty.py": ""}, 1, "no code was analysed", id="only-empty-files"),
        pytest.param(None, 1, "skipped", id="missing-dir"),
        pytest.param({"bad.py": "def (:\n"}, 1, "syntax error", id="unparsable-file"),
        pytest.param(
            {"a.py": _OK, "gone.py": _OK, ".bandit": "[bandit]\nexclude = gone.py\n"},
            1,
            "not scanned",
            id="narrowed-scope",
        ),
        pytest.param({"bad.py": _SHELL_TRUE}, 1, "B602", id="shell-true-finding"),
        pytest.param({"bad.py": _NEW_CALL}, 1, "B603", id="unsuppressed-subprocess-call"),
    ],
)
def test_bandit_gate_fails_unless_everything_was_analysed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    files: dict[str, str] | None,
    exit_code: int,
    message: str,
) -> None:
    # Given a scope (``None``: a path that does not exist) scanned like `make security` does
    scope = tmp_path / "scope"
    if files is not None:
        scope.mkdir()
        for name, text in files.items():
            (scope / name).write_text(text, encoding="utf-8")
    report = tmp_path / "bandit.json"
    bandit = [sys.executable, "-m", "bandit", "-q", "-f", "json", "-o", str(report)]
    subprocess.run([*bandit, "-r", str(scope)], check=False, capture_output=True)

    # When the guard reads Bandit's report
    result = check_bandit_report.main(str(report), [str(scope)])

    # Then it passes only a complete, finding-free scan, and names why it failed otherwise
    captured = capsys.readouterr()
    assert result == exit_code
    assert message in captured.out + captured.err


@pytest.mark.parametrize(
    "report",
    [
        pytest.param(None, id="never-written"),
        pytest.param("", id="empty-file-left-by-a-bandit-crash"),
        pytest.param("not json", id="not-json"),
        pytest.param("[]", id="not-an-object"),
        pytest.param('{"errors": [], "results": []}', id="no-metrics"),
    ],
)
def test_unusable_report_fails_instead_of_counting_as_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], report: str | None
) -> None:
    # Given a clean scope and a report that is missing, empty or malformed
    scope = tmp_path / "scope"
    scope.mkdir()
    (scope / "ok.py").write_text(_OK, encoding="utf-8")
    path = tmp_path / "bandit.json"
    if report is not None:
        path.write_text(report, encoding="utf-8")

    # When the guard reads it
    result = check_bandit_report.main(str(path), [str(scope)])

    # Then it fails and says the report is unusable; it never reports a clean scan
    captured = capsys.readouterr()
    assert result == 1
    assert "cannot use" in captured.err
    assert "no findings" not in captured.out
