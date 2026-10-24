"""The real ``make security`` recipe must hold every scanned directory to every Bandit test.

Each case runs it (a shim stands in for ``hatch``, ``true`` for ``pip``) on a scratch copy of the
layout it scans; ``tests/test_check_bandit_report.py`` covers the guard on its own.
"""

from __future__ import annotations

from pathlib import Path
import shlex
import sys

import pytest

from tests.bandit_recipe import (
    NEW_CALL,
    OK,
    SCANNED,
    SHELL_TRUE,
    make_security,
    needs_make,
    reported,
    scratch_layout,
)

_REVIEWED_CALL = (
    "import subprocess  # nosec B404 # test fixture\n"
    "import sys\n\n"
    "subprocess.run([sys.argv[1]], check=True)  # nosec B603 # test fixture\n"
)


@needs_make
@pytest.mark.parametrize(
    ("source", "failing_tests"),
    [
        pytest.param(OK, set(), id="clean"),
        pytest.param(NEW_CALL, {"B404", "B603"}, id="subprocess-call-without-nosec"),
        pytest.param(SHELL_TRUE, {"B602"}, id="shell-true"),
        pytest.param(_REVIEWED_CALL, set(), id="subprocess-call-with-reasoned-nosec"),
    ],
)
def test_make_security_holds_every_scanned_directory_to_every_bandit_test(
    tmp_path: Path, source: str, failing_tests: set[str]
) -> None:
    scratch_layout(tmp_path, {f"{name}/new_code.py": source for name in SCANNED})

    result = make_security(tmp_path)

    expected = {(f"{name}/new_code.py", test) for name in SCANNED for test in failing_tests}
    assert (result.returncode != 0) == bool(failing_tests), result.stderr
    assert expected <= reported(result), result.stderr


@needs_make
@pytest.mark.parametrize(
    ("files", "scope", "message"),
    [
        pytest.param({"examples/bad.py": "def (:\n"}, None, "syntax error", id="unparsable-file"),
        pytest.param({}, "empty-dir", "no code was analysed", id="empty-scope"),
        pytest.param({}, "no-such-dir", "No such file or directory", id="missing-scope"),
    ],
)
def test_make_security_fails_when_bandit_exits_zero_but_the_scan_is_incomplete(
    tmp_path: Path, files: dict[str, str], scope: str | None, message: str
) -> None:
    # Bandit itself exits 0 for each of these
    scratch_layout(tmp_path, files)
    (tmp_path / "empty-dir").mkdir()

    result = make_security(tmp_path, scope=scope)

    assert result.returncode != 0
    assert message in result.stderr


@needs_make
def test_make_security_fails_when_something_narrows_what_bandit_scans(tmp_path: Path) -> None:
    # the shim adds a `-x` the recipe does not know about: Bandit exits 0 without the file
    scratch_layout(tmp_path, {"tools/gone.py": OK})
    narrow = 'case "$*" in *"-m bandit"*) set -- "$@" -x tools/gone.py;; esac\n'

    result = make_security(tmp_path, hatch_prelude=narrow)

    assert result.returncode != 0
    assert "not scanned: tools/gone.py" in result.stderr


@needs_make
@pytest.mark.parametrize(
    ("directory", "bandit"),
    [
        pytest.param("tools", "[bandit]\nskips = B404,B603\n", id="skips-in-tools"),
        pytest.param("examples", "[bandit]\nskips = B404,B603\n", id="skips-in-examples"),
        pytest.param("benchmarks", "[bandit]\nskips = B404,B603\n", id="skips-in-benchmarks"),
        pytest.param("src", "[bandit]\nskips = B404,B603\n", id="skips-in-src"),
        pytest.param("tools", "[bandit]\ntests = B101\n", id="test-selection"),
        pytest.param("tools", "[bandit]\nconfigfile = hide.yaml\n", id="skips-in-a-config-file"),
        pytest.param("tools", "[bandit]\nexclude = new_code.py\n", id="excluded-file"),
    ],
)
def test_make_security_ignores_a_bandit_file_in_a_scanned_directory(
    tmp_path: Path, directory: str, bandit: str
) -> None:
    files = {f"{directory}/new_code.py": NEW_CALL, f"{directory}/.bandit": bandit}
    scratch_layout(tmp_path, {**files, "hide.yaml": "skips: [B404, B603]\n"})

    result = make_security(tmp_path)

    assert result.returncode != 0, result.stderr
    assert {(f"{directory}/new_code.py", "B404"), (f"{directory}/new_code.py", "B603")} <= (
        reported(result)
    )
    assert "Found project level .bandit file" not in result.stderr


@needs_make
@pytest.mark.parametrize(
    ("source", "message"),
    [
        pytest.param(
            "import subprocess  # nosec\nsubprocess.run(['x'], check=True)  # nosec\n",
            "names no Bandit test ID",
            id="blanket-nosec-on-an-unreviewed-call",
        ),
        pytest.param("x = 1  # nosec\n", "names no Bandit test ID", id="blanket-on-a-clean-line"),
        pytest.param(
            "import subprocess  # nosec B999 # reason\n"
            "subprocess.run(['x'], check=True)  # nosec B999 # reason\n",
            "suppressed a finding",
            id="unknown-test-id-bandit-treats-as-blanket",
        ),
    ],
)
def test_make_security_rejects_a_nosec_comment_that_names_no_bandit_test(
    tmp_path: Path, source: str, message: str
) -> None:
    # Bandit itself exits 0 for the same comment in every scanned directory
    scratch_layout(tmp_path, {f"{name}/new_code.py": source for name in SCANNED})

    result = make_security(tmp_path)

    assert result.returncode != 0
    assert message in result.stderr
    assert all(f"{name}/new_code.py" in result.stderr for name in SCANNED), result.stderr


@needs_make
def test_make_security_keeps_bandits_own_exit_status(tmp_path: Path) -> None:
    # a Bandit that writes a clean report but still exits non-zero
    scratch_layout(tmp_path)
    run_bandit_then_fail = (
        f'case "$*" in *"-m bandit"*) shift 2; {shlex.quote(sys.executable)} "$@"; exit 3;; esac\n'
    )

    result = make_security(tmp_path, hatch_prelude=run_bandit_then_fail)

    assert "no findings" in result.stdout
    assert result.returncode != 0


@needs_make
def test_make_security_keeps_the_log_free_of_the_progress_bar_and_config_warnings(
    tmp_path: Path,
) -> None:
    # Bandit 1.9.4 draws a progress bar on stdout above 50 files
    scratch_layout(tmp_path, {f"src/m{number}.py": OK for number in range(60)})

    result = make_security(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "Working" not in result.stdout + result.stderr
    assert "Unable to parse config file" not in result.stderr


@needs_make
@pytest.mark.parametrize(
    "bandit_fails",
    [
        pytest.param('case "$*" in *"-m bandit"*) exit 1;; esac\n', id="never-starts"),
        # Bandit opens its -o file before it can refuse to run: a refusal (exit 2) leaves it empty
        pytest.param(
            'case "$*" in *"-m bandit"*) : > test-results/bandit.json; exit 2;; esac\n',
            id="refuses-to-run",
        ),
    ],
)
def test_make_security_never_passes_on_a_report_left_by_an_earlier_run(
    tmp_path: Path, bandit_fails: str
) -> None:
    scratch_layout(tmp_path)
    assert make_security(tmp_path).returncode == 0
    assert (tmp_path / "test-results" / "bandit.json").is_file()

    result = make_security(tmp_path, hatch_prelude=bandit_fails)

    assert result.returncode != 0
    assert "cannot use" in result.stderr
    assert "no findings" not in result.stdout
