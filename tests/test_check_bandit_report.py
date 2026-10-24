"""The Bandit guard must fail instead of passing vacuously, proven with real Bandit.

Bandit exits 0 for an empty directory, a missing path, a file it cannot parse and a scope narrowed
by a ``.bandit`` file, and it suppresses a finding for a nosec comment that names no test it knows,
so these cases build a scratch tree, run Bandit with the flags of ``make security`` and hand its
JSON report to the guard. ``tests/test_make_security.py`` runs the real recipe around it.
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
_NOSEC_WITH_IDS = (
    "import subprocess  # noqa: S404  # nosec: B404 # test fixture\n"
    "subprocess.call('echo', shell=True)  # nosec:B602 B607 # test fixture\n"
)
_NO_ID = "names no Bandit test ID"
_VENV_GLOB = "*/.venv/*"


def _calls_with(comment: str) -> str:
    """Two lines Bandit reports, each carrying ``comment``."""
    return f"import subprocess  {comment}\nsubprocess.call('echo', shell=True)  {comment}\n"


def _scan(tmp_path: Path, files: dict[str, str] | None, *flags: str) -> tuple[str, list[str]]:
    """Run Bandit like `make security` on a scratch scope (``None``: a path that does not exist).

    Returns the JSON report and the scope, as the guard takes them.
    """
    scope = tmp_path / "scope"
    for name, text in (files or {}).items():
        (scope / name).parent.mkdir(parents=True, exist_ok=True)
        (scope / name).write_text(text, encoding="utf-8")
    if files is not None:
        scope.mkdir(exist_ok=True)
    report = tmp_path / "bandit.json"
    bandit = [sys.executable, "-m", "bandit", "-q", "-f", "json", "-o", str(report), *flags]
    subprocess.run([*bandit, "-r", str(scope)], check=False, capture_output=True)
    return str(report), [str(scope)]


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
        # tokenize raises on these two, so the guard must report them, not crash before printing
        pytest.param({"bad.py": 'x = """\n'}, 1, "cannot read its comments", id="open-string"),
        pytest.param(
            {"bad.py": "if 1:\n        x = 1\n    y = 2\n"},
            1,
            "cannot read its comments",
            id="bad-dedent",
        ),
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
    # Given a scope scanned like `make security` does
    report, scopes = _scan(tmp_path, files)

    # When the guard reads Bandit's report
    result = check_bandit_report.main(report, scopes)

    # Then it passes only a complete, finding-free scan, and names why it failed otherwise
    captured = capsys.readouterr()
    assert result == exit_code
    assert message in captured.out + captured.err


@pytest.mark.parametrize(
    ("files", "exit_code", "message"),
    [
        pytest.param({"ok.py": _NOSEC_WITH_IDS}, 0, "no findings", id="ids-in-colon-form"),
        pytest.param({"ok.py": 'x = "# nosec"\n'}, 0, "no findings", id="text-in-a-string"),
        pytest.param({"bad.py": _calls_with("# nosec")}, 1, _NO_ID, id="bare"),
        pytest.param({"bad.py": _calls_with("#nosec")}, 1, _NO_ID, id="without-a-space"),
        pytest.param({"bad.py": _calls_with("# noqa  # nosec")}, 1, _NO_ID, id="second-comment"),
        pytest.param({"bad.py": _calls_with("# nosec reviewed")}, 1, _NO_ID, id="words-not-ids"),
        pytest.param({"ok.py": "x = 1  # nosec\n"}, 1, _NO_ID, id="on-a-clean-line"),
        pytest.param(
            {"bad.py": _calls_with("# nosec B999 # reason")},
            1,
            "suppressed a finding",
            id="unknown-test-id-bandit-treats-as-blanket",
        ),
    ],
)
def test_guard_rejects_a_nosec_comment_that_names_no_bandit_test(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    files: dict[str, str],
    exit_code: int,
    message: str,
) -> None:
    # Given a scan in which Bandit itself exits 0 for every failing case here
    report, scopes = _scan(tmp_path, files)

    # When the guard reads Bandit's report
    result = check_bandit_report.main(report, scopes)

    # Then only a comment that names a test Bandit knows passes; text in a string is no comment
    captured = capsys.readouterr()
    assert result == exit_code
    assert message in captured.out + captured.err


@pytest.mark.parametrize(
    ("bandit_skips", "guard_skips", "exit_code", "message"),
    [
        pytest.param(True, True, 0, "no findings", id="both-skip-the-directory"),
        pytest.param(True, False, 1, "not scanned", id="guard-expects-a-file-bandit-skipped"),
        pytest.param(False, True, 1, "B602", id="a-finding-in-a-scanned-file-still-fails"),
    ],
)
def test_guard_expects_every_file_except_those_bandit_was_told_to_skip(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    bandit_skips: bool,
    guard_skips: bool,
    exit_code: int,
    message: str,
) -> None:
    # Given a .venv holding a vendored file with a finding, and Bandit told to skip it (or not)
    files = {"ok.py": _OK, ".venv/vendored.py": _SHELL_TRUE}
    report, scopes = _scan(tmp_path, files, *(["-x", _VENV_GLOB] if bandit_skips else []))

    # When the guard reads the report, expecting the same files or not
    result = check_bandit_report.main(report, scopes, (_VENV_GLOB,) if guard_skips else ())

    # Then the skipped directory is not expected, and a finding Bandit did report is never hidden
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
