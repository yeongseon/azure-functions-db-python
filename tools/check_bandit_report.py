"""Exit 1 unless Bandit analysed every Python file in the scope and reported nothing.

Bandit exits 0 for an empty directory, a missing path and a file it cannot parse (it only
lists them as skipped), so its exit status alone can pass without analysing anything.
"""

from __future__ import annotations

import argparse
from fnmatch import fnmatch
import json
from pathlib import Path
import sys
from typing import Any


def python_files(scopes: list[str], excludes: tuple[str, ...]) -> list[str]:
    """Every .py file under the scopes that `bandit -x <excludes>` does not skip (same globs)."""
    paths = (path.as_posix() for scope in scopes for path in Path(scope).rglob("*.py"))
    return sorted(path for path in paths if not any(fnmatch(path, glob) for glob in excludes))


def problems(report: dict[str, Any], scopes: list[str], excludes: tuple[str, ...]) -> list[str]:
    scanned = {Path(name).as_posix() for name in report["metrics"]}
    found: list[str] = []
    if report["metrics"]["_totals"]["loc"] == 0:
        found.append("no code was analysed")
    found += [f"skipped {e['filename']}: {e['reason']}" for e in report["errors"]]
    found += [
        f"not scanned: {path}" for path in python_files(scopes, excludes) if path not in scanned
    ]
    found += [
        f"{r['filename']}:{r['line_number']} {r['test_id']} {r['issue_text']}"
        for r in report["results"]
    ]
    return found


def main(report_path: str, scopes: list[str], excludes: tuple[str, ...] = ()) -> int:
    try:
        report = json.loads(Path(report_path).read_text(encoding="utf-8"))
        found = problems(report, scopes, excludes)
        lines, files = report["metrics"]["_totals"]["loc"], len(report["metrics"]) - 1
    except (OSError, ValueError, LookupError, TypeError) as error:
        print(f"bandit: cannot use {report_path}: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    for message in found:
        print(f"bandit: {message}", file=sys.stderr)
    if not found:
        print(f"bandit: {' '.join(scopes)}: {lines} lines in {files} files, no findings")
    return 1 if found else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("-x", "--exclude", default="", help="comma-separated globs of bandit -x")
    parser.add_argument("report", help="Bandit JSON report")
    parser.add_argument("scopes", nargs="+", help="paths given to bandit -r")
    arguments = parser.parse_args()
    excluded = tuple(glob for glob in arguments.exclude.split(",") if glob)
    raise SystemExit(main(arguments.report, arguments.scopes, excluded))
