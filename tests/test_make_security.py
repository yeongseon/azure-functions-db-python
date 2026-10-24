"""The real ``make security`` recipe must hold every scanned directory to every Bandit test.

Each case runs it (a shim stands in for ``hatch``, ``true`` for ``pip``) on a scratch copy of the
layout it scans; ``tests/test_check_bandit_report.py`` covers the guard on its own.
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
_EXAMPLE = "examples/postgresql-poll-trigger"
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
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text, encoding="utf-8")


def _example_venv(root: Path) -> Path:
    """Run `python -m venv .venv` inside an example, as its README says, and vendor code in it."""
    venv = root / _EXAMPLE / ".venv"
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(venv)], check=True)
    site_packages = next(venv.glob("lib*/python*/site-packages"))
    (site_packages / "vendored.py").write_text(_SHELL_TRUE, encoding="utf-8")
    (site_packages / "python2.py").write_text("print 'x'\n", encoding="utf-8")
    return venv


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


def _reported(result: subprocess.CompletedProcess[str]) -> set[tuple[str, str]]:
    """The (file, Bandit test ID) pairs the guard printed."""
    return set(re.findall(r"^bandit: (\S+?):\d+ (B\d+) ", result.stderr, re.MULTILINE))


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
    _scratch_layout(tmp_path, {f"{name}/new_code.py": source for name in _SCANNED})

    result = _make_security(tmp_path)

    expected = {(f"{name}/new_code.py", test) for name in _SCANNED for test in failing_tests}
    assert (result.returncode != 0) == bool(failing_tests), result.stderr
    assert expected <= _reported(result), result.stderr


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
    _scratch_layout(tmp_path, files)
    (tmp_path / "empty-dir").mkdir()

    result = _make_security(tmp_path, scope=scope)

    assert result.returncode != 0
    assert message in result.stderr


@needs_make
def test_make_security_fails_when_something_narrows_what_bandit_scans(tmp_path: Path) -> None:
    # the shim adds a `-x` the recipe does not know about: Bandit exits 0 without the file
    _scratch_layout(tmp_path, {"tools/gone.py": _OK})
    narrow = 'case "$*" in *"-m bandit"*) set -- "$@" -x tools/gone.py;; esac\n'

    result = _make_security(tmp_path, hatch_prelude=narrow)

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
    files = {f"{directory}/new_code.py": _NEW_CALL, f"{directory}/.bandit": bandit}
    _scratch_layout(tmp_path, {**files, "hide.yaml": "skips: [B404, B603]\n"})

    result = _make_security(tmp_path)

    assert result.returncode != 0, result.stderr
    assert {(f"{directory}/new_code.py", "B404"), (f"{directory}/new_code.py", "B603")} <= (
        _reported(result)
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
    _scratch_layout(tmp_path, {f"{name}/new_code.py": source for name in _SCANNED})

    result = _make_security(tmp_path)

    assert result.returncode != 0
    assert message in result.stderr
    assert all(f"{name}/new_code.py" in result.stderr for name in _SCANNED), result.stderr


@needs_make
def test_make_security_skips_the_venv_an_example_readme_creates(tmp_path: Path) -> None:
    _scratch_layout(tmp_path, {f"{_EXAMPLE}/function_app.py": _OK})
    venv = _example_venv(tmp_path)

    result = _make_security(tmp_path)

    own = [path for path in tmp_path.rglob("*.py") if venv not in path.parents]
    assert (venv / "pyvenv.cfg").is_file()
    assert result.returncode == 0, result.stderr
    assert f"in {len(own)} files, no findings" in result.stdout


@needs_make
def test_make_security_scans_every_directory_but_the_example_venv(tmp_path: Path) -> None:
    # an unannotated subprocess call in packages named like the directories once excluded
    names = ["env", "venv", ".venv", "node_modules", "site-packages"]
    packages = [f"src/azure_functions_db/{name}/zz.py" for name in names]
    packages += [f"{_EXAMPLE}/env/zz.py", f"{_EXAMPLE}/venv/zz.py"]
    _scratch_layout(tmp_path, dict.fromkeys(packages, _NEW_CALL))

    result = _make_security(tmp_path)

    expected = {(package, test) for package in packages for test in ("B404", "B603")}
    assert result.returncode != 0, result.stdout
    assert expected <= _reported(result), result.stderr


@needs_make
def test_make_security_still_scans_what_sits_beside_or_is_named_like_the_venv(
    tmp_path: Path,
) -> None:
    scanned = [
        f"{_EXAMPLE}/function_app.py",
        f"{_EXAMPLE}/venv_helpers.py",
        "examples/environment/app.py",
        "src/envelope.py",
        "benchmarks/node_modules_notes/bench.py",
        "tools/site_packages_check.py",
    ]
    _scratch_layout(tmp_path, dict.fromkeys(scanned, _SHELL_TRUE))
    _example_venv(tmp_path)

    result = _make_security(tmp_path)

    shell_true = {name for name, test in _reported(result) if test == "B602"}
    assert result.returncode != 0
    assert shell_true == set(scanned), result.stderr


@needs_make
def test_make_security_keeps_bandits_own_exit_status(tmp_path: Path) -> None:
    # a Bandit that writes a clean report but still exits non-zero
    _scratch_layout(tmp_path)
    run_bandit_then_fail = (
        f'case "$*" in *"-m bandit"*) shift 2; {shlex.quote(sys.executable)} "$@"; exit 3;; esac\n'
    )

    result = _make_security(tmp_path, hatch_prelude=run_bandit_then_fail)

    assert "no findings" in result.stdout
    assert result.returncode != 0


@needs_make
def test_make_security_keeps_the_log_free_of_the_progress_bar_and_config_warnings(
    tmp_path: Path,
) -> None:
    # Bandit 1.9.4 draws a progress bar on stdout above 50 files
    _scratch_layout(tmp_path, {f"src/m{number}.py": _OK for number in range(60)})

    result = _make_security(tmp_path)

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
    _scratch_layout(tmp_path)
    assert _make_security(tmp_path).returncode == 0
    assert (tmp_path / "test-results" / "bandit.json").is_file()

    result = _make_security(tmp_path, hatch_prelude=bandit_fails)

    assert result.returncode != 0
    assert "cannot use" in result.stderr
    assert "no findings" not in result.stdout
