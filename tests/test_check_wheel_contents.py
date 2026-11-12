"""Regression (#532): a wheel without the package must be rejected."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

import pytest

from tools.check_wheel_contents import (
    WheelContentError,
    check_wheel,
    main,
    wheel_package_modules,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_PACKAGE = REPO_ROOT / "src" / "azure_functions_db"


def _make_wheel(path: Path, names: list[str]) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        for name in names:
            archive.writestr(name, "")
    return path


DIST_INFO = [
    "azure_functions_db-0.4.0.dist-info/METADATA",
    "azure_functions_db-0.4.0.dist-info/WHEEL",
    "azure_functions_db-0.4.0.dist-info/RECORD",
    "azure_functions_db-0.4.0.dist-info/licenses/LICENSE",
]


def test_empty_wheel_like_0_4_0_is_rejected(tmp_path: Path) -> None:
    """The four published empty wheels had exactly these four entries."""
    wheel = _make_wheel(tmp_path / "empty-py3-none-any.whl", DIST_INFO)
    with pytest.raises(WheelContentError, match="no 'azure_functions_db/"):
        check_wheel(wheel)
    assert main([str(wheel)]) == 1


def test_wheel_without_init_is_rejected(tmp_path: Path) -> None:
    wheel = _make_wheel(
        tmp_path / "noinit-py3-none-any.whl",
        [*DIST_INFO, "azure_functions_db/trigger.py"],
    )
    with pytest.raises(WheelContentError, match="__init__.py"):
        check_wheel(wheel)


def test_complete_wheel_passes(tmp_path: Path) -> None:
    wheel = _make_wheel(
        tmp_path / "ok-py3-none-any.whl",
        [*DIST_INFO, "azure_functions_db/__init__.py", "azure_functions_db/a.py"],
    )
    assert check_wheel(wheel) == 2
    assert main([str(wheel)]) == 0


def test_modules_outside_package_do_not_count(tmp_path: Path) -> None:
    wheel = _make_wheel(
        tmp_path / "other-py3-none-any.whl",
        [*DIST_INFO, "other_pkg/__init__.py"],
    )
    assert wheel_package_modules(wheel) == set()
    with pytest.raises(WheelContentError):
        check_wheel(wheel)


def test_source_dir_comparison_detects_partial_wheel(tmp_path: Path) -> None:
    src = tmp_path / "src" / "azure_functions_db"
    (src / "sub").mkdir(parents=True)
    (src / "__init__.py").write_text("")
    (src / "sub" / "__init__.py").write_text("")
    (src / "sub" / "b.py").write_text("")
    wheel = _make_wheel(
        tmp_path / "partial-py3-none-any.whl",
        [*DIST_INFO, "azure_functions_db/__init__.py"],
    )
    with pytest.raises(WheelContentError, match="2 source module"):
        check_wheel(wheel, source_dir=src)


def test_unreadable_wheel_exits_2(tmp_path: Path) -> None:
    bad = tmp_path / "bad.whl"
    bad.write_text("not a zip")
    assert main([str(bad)]) == 2
    assert main([str(tmp_path / "missing.whl")]) == 2


@pytest.mark.skipif(
    importlib.util.find_spec("build") is None or importlib.util.find_spec("hatchling") is None,
    reason="needs 'build' and 'hatchling' installed for an offline --no-isolation build",
)
def test_real_default_build_produces_a_complete_wheel(tmp_path: Path) -> None:
    """Build the way the publish workflow did (``python -m build``: sdist, then
    wheel from the unpacked sdist). This is the path that produced 0 modules in
    0.2.2-0.4.1; it must now contain every source module."""
    work = tmp_path / "repo"
    work.mkdir()
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        if (REPO_ROOT / name).exists():
            shutil.copy(REPO_ROOT / name, work / name)
    shutil.copytree(REPO_ROOT / "src", work / "src", ignore=shutil.ignore_patterns("__pycache__"))
    dist = tmp_path / "dist"
    result = subprocess.run(
        [sys.executable, "-m", "build", "--no-isolation", "--outdir", str(dist)],
        cwd=work,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    wheels = list(dist.glob("*.whl"))
    assert len(wheels) == 1
    assert check_wheel(wheels[0], source_dir=SRC_PACKAGE) > 1
