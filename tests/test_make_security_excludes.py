"""``make security`` skips real virtualenvs and vendored trees, and nothing that looks like one.

Which directories those are is computed once, by ``tools/check_bandit_report.py --print-excludes``
(a directory holding a ``pyvenv.cfg``, any directory named ``node_modules`` or ``site-packages``),
and Bandit and the guard must get that same list. Each case runs the real recipe, with real Bandit,
on a scratch copy of the layout it scans.
"""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tests.bandit_recipe import (
    NEW_CALL,
    ROOT,
    SCANNED,
    SHELL_TRUE,
    hatch_calls,
    make_security,
    needs_make,
    reported,
    scratch_layout,
)

_EXAMPLE = "examples/postgresql-poll-trigger"
_README_EXAMPLES = ("mssql-poll-trigger", "mysql-poll-trigger", "postgresql-poll-trigger")


@pytest.fixture(scope="session")
def real_venv(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """One real ``python -m venv`` (pip included), built once; each case copies it."""
    venv = tmp_path_factory.mktemp("template") / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    return venv


def _copy_venv(template: Path, target: Path) -> Path:
    """Copy the real venv to ``target`` and vendor a finding and a syntax error into it."""
    shutil.copytree(template, target, symlinks=True)
    site_packages = next(target.glob("lib*/python*/site-packages"))
    (site_packages / "vendored.py").write_text(SHELL_TRUE, encoding="utf-8")
    (site_packages / "python2.py").write_text("print 'x'\n", encoding="utf-8")
    return target


def _own_files(root: Path, *skipped: Path) -> int:
    """The `.py` files under ``root`` that are not inside one of the ``skipped`` directories."""
    return sum(
        not any(path.is_relative_to(directory) for directory in skipped)
        for path in root.rglob("*.py")
    )


@needs_make
def test_make_security_skips_the_venv_each_example_readme_creates(
    tmp_path: Path, real_venv: Path
) -> None:
    for name in _README_EXAMPLES:
        readme = (ROOT / "examples" / name / "README.md").read_text(encoding="utf-8")
        assert "python -m venv .venv" in readme
    scratch_layout(tmp_path)
    venvs = [_copy_venv(real_venv, tmp_path / "examples" / n / ".venv") for n in _README_EXAMPLES]

    result = make_security(tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"in {_own_files(tmp_path, *venvs)} files, no findings" in result.stdout


@needs_make
@pytest.mark.parametrize(
    "where",
    [
        pytest.param(f"{_EXAMPLE}/venv", id="venv"),
        pytest.param(f"{_EXAMPLE}/custom-env", id="custom-env"),
        pytest.param(f"{_EXAMPLE}/deploy/nested/custom-env", id="deeper-in-the-example"),
        pytest.param("src/azure_functions_db/.venv", id="in-src"),
        pytest.param("tools/venv", id="in-tools"),
        pytest.param("benchmarks/custom-env", id="in-benchmarks"),
    ],
)
def test_make_security_skips_a_real_virtualenv_whatever_its_name_and_depth(
    tmp_path: Path, real_venv: Path, where: str
) -> None:
    scratch_layout(tmp_path)
    venv = _copy_venv(real_venv, tmp_path / where)

    result = make_security(tmp_path)

    assert result.returncode == 0, result.stderr
    assert f"in {_own_files(tmp_path, venv)} files, no findings" in result.stdout


@needs_make
def test_make_security_fails_on_a_fake_venv_that_has_no_pyvenv_cfg(tmp_path: Path) -> None:
    fakes = [f"{name}/.venv/zz.py" for name in ("src/azure_functions_db", _EXAMPLE, "tools")]
    fakes += ["benchmarks/.venv/lib/python3.11/pkg/zz.py", f"{_EXAMPLE}/.venv/bin/zz.py"]
    scratch_layout(tmp_path, dict.fromkeys(fakes, NEW_CALL))

    result = make_security(tmp_path)

    expected = {(fake, test) for fake in fakes for test in ("B404", "B603")}
    assert result.returncode != 0, result.stdout
    assert expected <= reported(result), result.stderr


@needs_make
def test_make_security_still_scans_owned_packages_named_env_or_venv(tmp_path: Path) -> None:
    packages = [
        f"{where}/{name}/zz.py"
        for where in ("src/azure_functions_db", _EXAMPLE)
        for name in ("env", "venv", "custom-env")
    ]
    scratch_layout(tmp_path, dict.fromkeys(packages, NEW_CALL))

    result = make_security(tmp_path)

    expected = {(package, test) for package in packages for test in ("B404", "B603")}
    assert result.returncode != 0, result.stdout
    assert expected <= reported(result), result.stderr


@needs_make
def test_make_security_skips_node_modules_and_site_packages(tmp_path: Path) -> None:
    trees = [
        "src/azure_functions_db/node_modules",
        f"{_EXAMPLE}/site-packages",
        "tools/node_modules",
        "benchmarks/site-packages",
    ]
    vendored = {f"{tree}/dep/node_modules/zz.py": NEW_CALL for tree in trees}
    scratch_layout(tmp_path, {**vendored, "benchmarks/site-packages/bad.py": "def (:\n"})

    result = make_security(tmp_path)

    assert result.returncode == 0, result.stderr
    assert (
        f"in {_own_files(tmp_path, *[tmp_path / tree for tree in trees])} files," in result.stdout
    )


@needs_make
def test_make_security_still_scans_what_sits_beside_or_is_named_like_the_excluded_directories(
    tmp_path: Path, real_venv: Path
) -> None:
    scanned = [
        f"{_EXAMPLE}/function_app.py",
        f"{_EXAMPLE}/venv_helpers.py",
        "examples/environment/app.py",
        "src/envelope.py",
        "tools/site_packages_check.py",
        "benchmarks/node_modules_notes/app.py",
    ]
    scratch_layout(tmp_path, dict.fromkeys(scanned, SHELL_TRUE))
    _copy_venv(real_venv, tmp_path / _EXAMPLE / ".venv")

    result = make_security(tmp_path)

    shell_true = {name for name, test in reported(result) if test == "B602"}
    assert result.returncode != 0
    assert shell_true == set(scanned), result.stderr


@needs_make
def test_bandit_and_the_guard_get_the_list_the_guard_computed(
    tmp_path: Path, real_venv: Path
) -> None:
    venvs = [f"{_EXAMPLE}/.venv", "src/azure_functions_db/custom-env", "tools/venv"]
    vendored = ["examples/mssql-poll-trigger/node_modules", "benchmarks/site-packages"]
    scratch_layout(tmp_path, {f"{name}/zz.py": SHELL_TRUE for name in vendored})
    for venv in venvs:
        _copy_venv(real_venv, tmp_path / venv)

    result = make_security(tmp_path)

    expected = ",".join(sorted(f"{name}/*" for name in [*venvs, *vendored]))
    calls = hatch_calls(tmp_path)
    printing = next(call for call in calls if "--print-excludes" in call)
    bandit = next(call for call in calls if "bandit" in call)
    guard = next(c for c in calls if "tools/check_bandit_report.py" in c and c is not printing)
    assert result.returncode == 0, result.stderr
    assert bandit[bandit.index("-x") + 1] == guard[guard.index("-x") + 1] == expected
    assert (
        printing[-len(SCANNED) :]
        == bandit[-len(SCANNED) :]
        == guard[-len(SCANNED) :]
        == list(SCANNED)
    )
    assert f"bandit: excluding {expected}" in result.stdout


@needs_make
def test_make_security_does_not_run_bandit_when_the_exclusions_cannot_be_computed(
    tmp_path: Path,
) -> None:
    scratch_layout(tmp_path)
    crash = 'case "$*" in *"--print-excludes"*) echo crashed >&2; exit 7;; esac\n'

    result = make_security(tmp_path, hatch_prelude=crash)

    assert result.returncode != 0
    assert not any("bandit" in call for call in hatch_calls(tmp_path))
    assert "no findings" not in result.stdout
