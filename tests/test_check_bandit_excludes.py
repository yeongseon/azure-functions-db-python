"""The guard computes what Bandit must not scan: virtualenvs and vendored trees.

A directory holding a ``pyvenv.cfg`` is a virtualenv root whatever it is called and however deep it
sits, and a directory named ``node_modules`` or ``site-packages`` is a vendored tree. Nothing else
is excluded, in particular not a directory that is merely called ``venv``, ``env`` or ``.venv``.
"""

from __future__ import annotations

from fnmatch import fnmatch
from pathlib import Path
import subprocess
import sys

import pytest

from tools import check_bandit_report


def _tree(root: Path, names: list[str]) -> None:
    for name in names:
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        text = "home = /usr\n" if name.endswith("pyvenv.cfg") else "x = 1\n"
        (root / name).write_text(text, encoding="utf-8")


@pytest.mark.parametrize(
    ("files", "excluded"),
    [
        pytest.param([".venv/pyvenv.cfg"], [".venv"], id="dot-venv"),
        pytest.param(["venv/pyvenv.cfg"], ["venv"], id="venv"),
        pytest.param(["custom-env/pyvenv.cfg"], ["custom-env"], id="any-name"),
        pytest.param(["a/b/c/anything/pyvenv.cfg"], ["a/b/c/anything"], id="any-depth"),
        pytest.param(
            ["x/node_modules/n.py", "site-packages/s.py"],
            ["x/node_modules", "site-packages"],
            id="vendored-trees",
        ),
        pytest.param(
            [".venv/pyvenv.cfg", ".venv/lib/site-packages/p.py", ".venv/node_modules/n.py"],
            [".venv"],
            id="nothing-inside-an-excluded-directory-is-listed-again",
        ),
        pytest.param(
            [".venv/x.py", "venv/x.py", "env/x.py", "custom-env/x.py"],
            [],
            id="look-alikes-without-a-pyvenv-cfg",
        ),
        pytest.param(
            ["pyvenv.cfg/x.py", "node_modules_notes/x.py", "site_packages_check.py"],
            [],
            id="names-that-only-resemble-them",
        ),
    ],
)
def test_excluded_globs_are_the_virtualenvs_and_vendored_trees_under_the_scope(
    tmp_path: Path, files: list[str], excluded: list[str]
) -> None:
    _tree(tmp_path, files)

    result = check_bandit_report.excluded_globs([str(tmp_path)])

    assert result == sorted(f"{tmp_path.as_posix()}/{name}/*" for name in excluded)


def test_a_glob_character_in_a_directory_name_does_not_widen_its_exclusion(tmp_path: Path) -> None:
    _tree(tmp_path, ["[ab]/pyvenv.cfg", "a/owned.py", "b/owned.py"])

    (pattern,) = check_bandit_report.excluded_globs([str(tmp_path)])

    assert fnmatch(f"{tmp_path.as_posix()}/[ab]/x.py", pattern)
    assert not fnmatch(f"{tmp_path.as_posix()}/a/owned.py", pattern)


@pytest.mark.parametrize(
    ("files", "printed"),
    [
        pytest.param(
            ["one/.venv/pyvenv.cfg", "two/node_modules/x.py"],
            "{0}/one/.venv/*,{0}/two/node_modules/*",
            id="two-scopes",
        ),
        pytest.param(["one/ok.py", "two/ok.py"], "", id="nothing-to-exclude"),
    ],
)
def test_print_excludes_prints_the_list_the_recipe_hands_to_bandit(
    tmp_path: Path, files: list[str], printed: str
) -> None:
    _tree(tmp_path, files)
    command = [sys.executable, check_bandit_report.__file__, "--print-excludes"]

    result = subprocess.run(
        [*command, str(tmp_path / "one"), str(tmp_path / "two")],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == printed.format(tmp_path.as_posix()) + "\n"
