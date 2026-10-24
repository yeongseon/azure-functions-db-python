"""The real ``make security`` recipe must hold every scanned directory to every Bandit test.

Each case runs the recipe (a shim stands in for ``hatch``, ``true`` for ``pip``) on a scratch copy
of the layout it scans, so a skip list, a dropped directory, a missing delete-first or an ignored
exit status in that recipe turns one of them red. ``tests/test_check_bandit_report.py`` covers the
guard on its own.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCANNED = ("src", "examples", "tools", "benchmarks")
_OK = "x = 1\n"
_SHELL_TRUE = "import subprocess\nsubprocess.call('echo', shell=True)\n"
_NEW_CALL = "import subprocess\nimport sys\n\nsubprocess.run([sys.argv[1]], check=True)\n"
_REVIEWED_CALL = (
    "import subprocess  # nosec B404 # test fixture\n"
    "import sys\n\n"
    "subprocess.run([sys.argv[1]], check=True)  # nosec B603 # test fixture\n"
)
needs_make = pytest.mark.skipif(
    sys.platform == "win32" or shutil.which("make") is None,
    reason="runs the real `make security` recipe through /bin/sh",
)


def _scratch_layout(root: Path, files: dict[str, str] | None = None) -> None:
    """Create the directories `make security` scans, each with a clean file, plus ``files``."""
    for name in _SCANNED:
        (root / name).mkdir(exist_ok=True)
        (root / name / "ok.py").write_text(_OK, encoding="utf-8")
    for name, text in (files or {}).items():
        (root / name).write_text(text, encoding="utf-8")


def _make_security(
    root: Path, *, hatch_prelude: str = "", scope: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the real ``make security`` recipe in ``root``, a scratch copy of the layout it scans.

    ``hatch`` is a shim that runs this interpreter, which has Bandit; ``hatch_prelude`` is shell
    that runs first and can stand in for a broken Bandit. ``true`` stands in for ``pip``.
    """
    shutil.copy(_ROOT / "Makefile", root / "Makefile")
    shutil.copy(_ROOT / "tools" / "check_bandit_report.py", root / "tools")
    hatch = root / "hatch"
    hatch.write_text(
        f'#!/bin/sh\n{hatch_prelude}shift 2\nexec {shlex.quote(sys.executable)} "$@"\n'
    )
    hatch.chmod(0o755)
    environment = {k: v for k, v in os.environ.items() if not k.startswith(("MAKE", "MFLAGS"))}
    overrides = [f"BANDIT_SCOPE={scope}"] if scope else []
    return subprocess.run(
        ["make", "security", f"HATCH={hatch}", "PIP=true", f"VENV_DIR={root}", *overrides],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


@needs_make
@pytest.mark.parametrize(
    ("source", "failing_tests"),
    [
        pytest.param(_OK, set(), id="clean"),
        pytest.param(_NEW_CALL, {"B404", "B603"}, id="subprocess-call-without-nosec"),
        pytest.param(_SHELL_TRUE, {"B602"}, id="shell-true"),
        pytest.param(_REVIEWED_CALL, set(), id="subprocess-call-with-reasoned-nosec"),
    ],
)
def test_make_security_holds_every_scanned_directory_to_every_bandit_test(
    tmp_path: Path, source: str, failing_tests: set[str]
) -> None:
    # Given the layout `make security` scans, with the same new code in every directory
    _scratch_layout(tmp_path, {f"{name}/new_code.py": source for name in _SCANNED})

    # When the real recipe runs
    result = _make_security(tmp_path)

    # Then it fails exactly when the new code is unreviewed, naming the test in each directory
    reported = set(re.findall(r"^bandit: (\S+?):\d+ (B\d+) ", result.stderr, re.MULTILINE))
    expected = {(f"{name}/new_code.py", test) for name in _SCANNED for test in failing_tests}
    assert (result.returncode != 0) == bool(failing_tests), result.stderr
    assert expected <= reported, result.stderr


@needs_make
@pytest.mark.parametrize(
    ("files", "scope", "message"),
    [
        pytest.param({"examples/bad.py": "def (:\n"}, None, "syntax error", id="unparsable-file"),
        pytest.param(
            {"tools/gone.py": _OK, "tools/.bandit": "[bandit]\nexclude = gone.py\n"},
            None,
            "not scanned",
            id="narrowed-scope",
        ),
        pytest.param({}, "empty-dir", "no code was analysed", id="empty-scope"),
        pytest.param({}, "no-such-dir", "No such file or directory", id="missing-scope"),
    ],
)
def test_make_security_fails_when_bandit_exits_zero_but_the_scan_is_incomplete(
    tmp_path: Path, files: dict[str, str], scope: str | None, message: str
) -> None:
    # Given a scan Bandit itself accepts: it exits 0 for each of these
    _scratch_layout(tmp_path, files)
    (tmp_path / "empty-dir").mkdir()

    # When the real recipe runs
    result = _make_security(tmp_path, scope=scope)

    # Then the guard fails the target and says why
    assert result.returncode != 0
    assert message in result.stderr


@needs_make
def test_make_security_keeps_bandits_own_exit_status(tmp_path: Path) -> None:
    # Given a clean tree and a Bandit that writes a clean report but still exits non-zero
    _scratch_layout(tmp_path)
    run_bandit_then_fail = (
        f'case "$*" in *"-m bandit"*) shift 2; {shlex.quote(sys.executable)} "$@"; exit 3;; esac\n'
    )

    # When the real recipe runs
    result = _make_security(tmp_path, hatch_prelude=run_bandit_then_fail)

    # Then the target fails although the guard was satisfied
    assert "no findings" in result.stdout
    assert result.returncode != 0


@needs_make
def test_make_security_keeps_the_bandit_progress_bar_out_of_the_log(tmp_path: Path) -> None:
    # Given more than 50 files in scope, where Bandit 1.9.4 draws a progress bar on stdout
    _scratch_layout(tmp_path, {f"src/m{number}.py": _OK for number in range(60)})

    # When the real recipe runs
    result = _make_security(tmp_path)

    # Then it passes and the bar is not in the log
    assert result.returncode == 0, result.stderr
    assert "Working" not in result.stdout + result.stderr


@needs_make
@pytest.mark.parametrize("crash", ["never-starts", "refuses-to-run"])
def test_make_security_never_passes_on_a_report_left_by_an_earlier_run(
    tmp_path: Path, crash: str
) -> None:
    # Given a clean scan that left its report behind
    _scratch_layout(tmp_path)
    assert _make_security(tmp_path).returncode == 0
    assert (tmp_path / "test-results" / "bandit.json").is_file()

    # When Bandit then fails: it never starts, or it refuses to run because two .bandit files
    # are in scope (it exits 2 and leaves an empty report)
    prelude = ""
    if crash == "never-starts":
        prelude = 'case "$*" in *"-m bandit"*) exit 1;; esac\n'
    else:
        (tmp_path / "examples" / ".bandit").write_text("[bandit]\n", encoding="utf-8")
        (tmp_path / "tools" / ".bandit").write_text("[bandit]\n", encoding="utf-8")
    result = _make_security(tmp_path, hatch_prelude=prelude)

    # Then the target fails and the stale clean report is not read as this scan's result
    assert result.returncode != 0
    assert "cannot use" in result.stderr
    assert "no findings" not in result.stdout
