"""Resolve an existing interpreter without creating or syncing an environment.

This bootstrap deliberately supports system Python 3.10; the selected interpreter
must support the project's Python 3.11 tooling.
"""

import os
from pathlib import Path
import shutil
import subprocess
import sys


def main() -> int:
    candidates = []
    if active := os.environ.get("VIRTUAL_ENV"):
        candidates.append(str(Path(active) / "bin/python"))
    candidates.append(str(Path(".venv/bin/python").resolve()))
    hatch = shutil.which("hatch")
    if hatch:
        found = subprocess.run([hatch, "env", "find"], capture_output=True, text=True, check=False)
        if found.returncode == 0:
            candidates.append(str(Path(found.stdout.strip()) / "bin/python"))
    candidates.extend(filter(None, (shutil.which("python3.11"), shutil.which("python3"))))
    for candidate in candidates:
        if not Path(candidate).is_file():
            continue
        compatible = subprocess.run(
            [
                candidate,
                "-c",
                "import sys; raise SystemExit(not (3,11) <= sys.version_info[:2] < (3,15))",
            ],
            check=False,
        )
        if compatible.returncode == 0:
            os.execv(candidate, [candidate, "-m", "tools.verification", *sys.argv[1:]])
    print("development environment not created: run make install", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
