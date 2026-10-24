"""Exit 1 unless Bandit analysed every Python file in the scope and reported nothing.

Bandit exits 0 for an empty directory, a missing path and a file it cannot parse (it only
lists them as skipped), so its exit status alone can pass without analysing anything. It also
suppresses every test on a line whose nosec comment names no test it knows (a bare `# nosec`,
`# nosec reason`, `# nosec B999`) and only counts that, so such a comment fails here too: a
reviewed finding is suppressed as `# nosec Bxxx # <reason>`.

`--print-excludes <scopes>` prints what Bandit must not scan, as the comma-separated globs of
`bandit -x`: every virtualenv under the scopes (a directory holding a `pyvenv.cfg`) and every
`node_modules` and `site-packages` directory. The recipe prints it once and gives the same list
to Bandit and, as `-x`, to this guard, so the files Bandit skips are the ones not expected here.
"""

from __future__ import annotations

import argparse
from fnmatch import fnmatch
import glob
import json
import os
from pathlib import Path
import re
import sys
import tokenize
from typing import Any

_NOSEC_WITHOUT_ID = re.compile(r"#\s*nosec(?!:?\s*B\d)", re.IGNORECASE)
_VENDORED_DIRS = frozenset({"node_modules", "site-packages"})


def excluded_globs(scopes: list[str]) -> list[str]:
    """`bandit -x` globs: each virtualenv root (a `pyvenv.cfg` inside) and vendored tree below."""
    found: set[str] = set()
    for scope in scopes:
        for root, dirs, _ in os.walk(scope):
            path = Path(root)
            if (path / "pyvenv.cfg").is_file() or path.name in _VENDORED_DIRS:
                found.add(f"{glob.escape(path.as_posix())}/*")
                dirs.clear()
    return sorted(found)


def python_files(scopes: list[str], excludes: tuple[str, ...]) -> list[str]:
    """Every .py file under the scopes that `bandit -x <excludes>` does not skip (same globs)."""
    paths = (path.as_posix() for scope in scopes for path in Path(scope).rglob("*.py"))
    return sorted(path for path in paths if not any(fnmatch(path, entry) for entry in excludes))


def nosec_without_id(path: str) -> list[str]:
    try:
        with tokenize.open(path) as source:
            tokens = list(tokenize.generate_tokens(source.readline))
    except (OSError, SyntaxError, ValueError, tokenize.TokenError) as error:
        return [f"{path}: cannot read its comments: {type(error).__name__}: {error}"]
    return [
        f"{path}:{token.start[0]} nosec comment names no Bandit test ID"
        for token in tokens
        if token.type == tokenize.COMMENT and _NOSEC_WITHOUT_ID.search(token.string)
    ]


def problems(report: dict[str, Any], scopes: list[str], excludes: tuple[str, ...]) -> list[str]:
    files = python_files(scopes, excludes)
    scanned = {Path(name).as_posix() for name in report["metrics"]}
    found: list[str] = []
    if report["metrics"]["_totals"]["loc"] == 0:
        found.append("no code was analysed")
    found += [f"skipped {e['filename']}: {e['reason']}" for e in report["errors"]]
    found += [f"not scanned: {path}" for path in files if path not in scanned]
    found += [
        f"{r['filename']}:{r['line_number']} {r['test_id']} {r['issue_text']}"
        for r in report["results"]
    ]
    found += [message for path in files for message in nosec_without_id(path)]
    found += [
        f"{name}: a nosec comment without a known test ID suppressed a finding"
        for name, counts in report["metrics"].items()
        if name != "_totals" and counts["nosec"]
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
    parser.add_argument(
        "--print-excludes", action="store_true", help="print the -x list for the scopes and exit"
    )
    parser.add_argument("paths", nargs="+", help="the Bandit JSON report, then the paths of -r")
    arguments = parser.parse_args()
    if arguments.print_excludes:
        print(",".join(excluded_globs(arguments.paths)))
        raise SystemExit(0)
    report, *scopes = arguments.paths
    if not scopes:
        parser.error("a report and at least one scope are required")
    excluded = tuple(entry for entry in arguments.exclude.split(",") if entry)
    raise SystemExit(main(report, scopes, excluded))
