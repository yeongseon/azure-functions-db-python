"""Exercise the real Make command graph without installing project dependencies."""

from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which("make") is None, reason="make is unavailable")
def test_check_uses_existing_hatch_without_bootstrap_installs(tmp_path: Path) -> None:
    # Given an executable Hatch shim and the repository's actual Makefile.
    root = Path(__file__).resolve().parents[1]
    shutil.copyfile(root / "Makefile", tmp_path / "Makefile")
    hatch = tmp_path / "hatch"
    hatch.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> commands\n')
    hatch.chmod(0o755)

    # When the contributor runs the complete lint/type-check entry point.
    result = subprocess.run(
        ["make", "check", f"HATCH={hatch}", "PIP=false"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    # Then no installer runs and mypy is reached through exactly one Hatch command.
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "commands").read_text().splitlines() == ["run lint"]


@pytest.mark.skipif(shutil.which("make") is None, reason="make is unavailable")
def test_missing_hatch_bootstraps_once_without_upgrading_pip(tmp_path: Path) -> None:
    # Given a venv directory whose Hatch executable is missing.
    root = Path(__file__).resolve().parents[1]
    shutil.copyfile(root / "Makefile", tmp_path / "Makefile")
    venv = tmp_path / "venv"
    venv.mkdir()
    python = venv / "bin" / "python"
    python.parent.mkdir()
    python.symlink_to(shutil.which("python3") or "/usr/bin/python3")
    hatch = venv / "hatch"
    installer = tmp_path / "installer"
    installer.write_text(
        '#!/bin/sh\nprintf "%s\\n" "$*" >> installs\n'
        f"printf '#!/bin/sh\\nexit 0\\n' > '{hatch}'\n"
        f"chmod +x '{hatch}'\n"
    )
    installer.chmod(0o755)
    command = ["make", "lint", f"VENV_DIR={venv}", f"HATCH={hatch}", f"PIP={installer}"]

    # When two normal quality commands encounter that environment in sequence.
    first = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, check=False)
    second = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True, check=False)

    # Then only the first command installs Hatch, with no pip upgrade.
    assert first.returncode == second.returncode == 0, first.stderr + second.stderr
    assert (tmp_path / "installs").read_text().splitlines() == ["install hatch"]
