from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys


def container_runtime() -> str | None:
    for name in ("docker", "podman"):
        if shutil.which(name):
            result = subprocess.run([name, "info"], capture_output=True, timeout=15, check=False)
            if result.returncode == 0:
                return name
    return None


def doctor() -> int:
    healthy = (3, 11) <= sys.version_info[:2] < (3, 15)
    print(f"Python REQUIRED >=3.11,<3.15: {sys.version.split()[0]} ({sys.executable})")
    for module in ("venv", "ruff", "mypy", "pytest", "bandit", "build", "mkdocs"):
        present = importlib.util.find_spec(module) is not None
        print(f"{module} REQUIRED for check/test: {'found' if present else 'MISSING'}")
        if not present:
            print("development environment not created: run make install")
        healthy = healthy and present
    for tool in ("func", "azurite"):
        executable = shutil.which(tool)
        print(f"{tool} REQUIRED for integration/verify: {executable or 'MISSING'}")
        healthy = healthy and executable is not None
    runtime = container_runtime()
    print(f"docker/podman REQUIRED for database integration/verify: {runtime or 'MISSING'}")
    healthy = healthy and runtime is not None
    for module in ("psycopg", "pymysql", "pyodbc"):
        present = importlib.util.find_spec(module) is not None
        print(f"{module} REQUIRED for integration/verify: {'found' if present else 'MISSING'}")
        healthy = healthy and present
    print("ODBC Driver 18 for SQL Server REQUIRED for MSSQL (connection preflight verifies it)")
    if importlib.util.find_spec("pyodbc") is not None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import pyodbc; "
                "raise SystemExit('ODBC Driver 18 for SQL Server' not in pyodbc.drivers())",
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        healthy = healthy and result.returncode == 0
    return 0 if healthy else 1


def clean(root: Path) -> None:
    for name in (
        "test-results",
        "dist",
        "build",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".coverage",
        "coverage.xml",
        "htmlcov",
        "site",
        "__pycache__",
    ):
        path = root / name
        if path.is_symlink() or path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
    for tree in ("src", "tests", "tools", "examples", "benchmarks"):
        for path in (root / tree).rglob("__pycache__"):
            if not any(part in {".venv", "venv", "env"} for part in path.parts):
                if path.is_symlink():
                    path.unlink()
                else:
                    shutil.rmtree(path)
