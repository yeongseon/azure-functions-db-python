"""Helpers for the tests that run the real ``make security`` recipe on a scratch tree."""

from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCANNED = ("src", "examples", "tools", "benchmarks")
OK = "x = 1\n"
SHELL_TRUE = "import subprocess\nsubprocess.call('echo', shell=True)\n"
NEW_CALL = "import subprocess\nimport sys\n\nsubprocess.run([sys.argv[1]], check=True)\n"
needs_make = pytest.mark.skipif(
    sys.platform == "win32" or shutil.which("make") is None,
    reason="runs the real `make security` recipe through /bin/sh",
)


def scratch_layout(root: Path, files: dict[str, str] | None = None) -> None:
    """Create the directories `make security` scans, each with a clean file, plus ``files``."""
    for name in SCANNED:
        (root / name).mkdir(exist_ok=True)
        (root / name / "ok.py").write_text(OK, encoding="utf-8")
    for name, text in (files or {}).items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_text(text, encoding="utf-8")


def make_security(
    root: Path, *, hatch_prelude: str = "", scope: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the real ``make security`` recipe in ``root``, a scratch copy of the layout it scans.

    ``hatch`` is a shim that runs this interpreter, which has Bandit, and records its arguments
    (``hatch_calls``); ``hatch_prelude`` is shell that runs first and can stand in for a broken
    Bandit. ``true`` stands in for ``pip``.
    """
    shutil.copy(ROOT / "Makefile", root / "Makefile")
    shutil.copy(ROOT / "tools" / "check_bandit_report.py", root / "tools")
    hatch = root / "hatch"
    hatch.write_text(
        "#!/bin/sh\n"
        f"{{ echo --; printf '%s\\n' \"$@\"; }} >> {shlex.quote(str(root / 'hatch-calls.txt'))}\n"
        f'{hatch_prelude}shift 2\nexec {shlex.quote(sys.executable)} "$@"\n'
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


def hatch_calls(root: Path) -> list[list[str]]:
    """The arguments of every ``hatch`` call the recipe made, one list per call."""
    calls: list[list[str]] = []
    for line in (root / "hatch-calls.txt").read_text(encoding="utf-8").splitlines():
        if line == "--":
            calls.append([])
        else:
            calls[-1].append(line)
    return calls


def reported(result: subprocess.CompletedProcess[str]) -> set[tuple[str, str]]:
    """The (file, Bandit test ID) pairs the guard printed."""
    return set(re.findall(r"^bandit: (\S+?):\d+ (B\d+) ", result.stderr, re.MULTILINE))
