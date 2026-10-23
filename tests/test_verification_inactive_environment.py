import os
from pathlib import Path
import subprocess
import sys
import sysconfig

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("selection", ["inactive", "active", "hatch"])
def test_doctor_discovers_existing_venv_without_dereferencing_python(
    tmp_path: Path, selection: str
) -> None:
    environment = dict(
        os.environ, PYTHONPATH=str(ROOT), HATCH_DATA_DIR=str(tmp_path / "hatch-data")
    )
    environment.pop("VIRTUAL_ENV", None)
    venv = tmp_path / (".venv" if selection == "inactive" else "existing")
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(venv)], check=True)
    site = venv / f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    (site / "development.pth").write_text(sysconfig.get_path("purelib") + "\n")
    if selection == "active":
        environment["VIRTUAL_ENV"] = str(venv)
    elif selection == "hatch":
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        hatch = bin_dir / "hatch"
        hatch.write_text(f"#!/bin/sh\nprintf '%s\\n' '{venv}'\n")
        hatch.chmod(0o755)
        environment["PATH"] = str(bin_dir) + os.pathsep + environment["PATH"]
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools/verification_maintenance.py"), "doctor"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert "ruff REQUIRED for check/test: found" in result.stdout, result.stdout + result.stderr
    assert "pytest REQUIRED for check/test: found" in result.stdout
    assert str(venv / "bin/python") in result.stdout
    assert not (tmp_path / "hatch-data/env").exists()


def test_doctor_without_existing_environment_requests_explicit_install(tmp_path: Path) -> None:
    environment = dict(
        os.environ, PYTHONPATH=str(ROOT), HATCH_DATA_DIR=str(tmp_path / "hatch-data")
    )
    environment.pop("VIRTUAL_ENV", None)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    runtime = tmp_path / "bare-runtime"
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(runtime)], check=True)
    (bin_dir / "python3.11").write_text(f'#!/bin/sh\nexec "{runtime / "bin/python"}" "$@"\n')
    (bin_dir / "python3.11").chmod(0o755)
    environment["PATH"] = str(bin_dir)
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools/verification_maintenance.py"), "doctor"],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "run make install" in result.stdout + result.stderr
    assert not (tmp_path / "hatch-data/env").exists()
