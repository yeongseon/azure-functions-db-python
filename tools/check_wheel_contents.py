#!/usr/bin/env python3
"""Fail when a built wheel does not contain the package it claims to ship.

The wheels of ``0.2.2``, ``0.3.0``, ``0.4.0`` and ``0.4.1`` were published
with only ``*.dist-info`` files (issue #532): ``pip install`` succeeded and
``import azure_functions_db`` failed. This check opens the built wheel itself,
so it catches that class of defect regardless of the build configuration that
caused it.

Usage::

    python -m tools.check_wheel_contents dist/*.whl
    python -m tools.check_wheel_contents dist/x.whl --source-dir src/azure_functions_db

Exit codes: 0 clean, 1 wheel is missing package files, 2 usage/IO error.
Stdlib-only, like the other release-gate tools.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import sys
import zipfile

DEFAULT_PACKAGE = "azure_functions_db"


class WheelContentError(Exception):
    """The wheel does not contain the expected package files."""


def wheel_package_modules(wheel: Path, package: str = DEFAULT_PACKAGE) -> set[str]:
    """Return the ``.py`` paths in ``wheel`` that live inside ``package/``."""
    prefix = f"{package}/"
    with zipfile.ZipFile(wheel) as archive:
        return {
            name for name in archive.namelist() if name.startswith(prefix) and name.endswith(".py")
        }


def source_package_modules(source_dir: Path, package: str = DEFAULT_PACKAGE) -> set[str]:
    """Return the ``.py`` paths of ``source_dir`` as they appear in a wheel."""
    return {
        f"{package}/{path.relative_to(source_dir).as_posix()}"
        for path in source_dir.rglob("*.py")
        if "__pycache__" not in path.parts
    }


def check_wheel(
    wheel: Path,
    package: str = DEFAULT_PACKAGE,
    source_dir: Path | None = None,
) -> int:
    """Validate ``wheel`` and return its package module count.

    Raises ``WheelContentError`` when the package ``__init__`` is absent, when
    there are no package modules, or (with ``source_dir``) when any source
    module is missing from the wheel.
    """
    modules = wheel_package_modules(wheel, package)
    if not modules:
        raise WheelContentError(
            f"{wheel.name}: contains no '{package}/*.py' modules "
            "(an install would succeed but the package could not be imported)"
        )
    init = f"{package}/__init__.py"
    if init not in modules:
        raise WheelContentError(f"{wheel.name}: missing {init}")
    if source_dir is not None:
        expected = source_package_modules(source_dir, package)
        if not expected:
            raise WheelContentError(f"{source_dir}: no source modules found to compare")
        missing = sorted(expected - modules)
        if missing:
            shown = ", ".join(missing[:5])
            more = f" (+{len(missing) - 5} more)" if len(missing) > 5 else ""
            raise WheelContentError(
                f"{wheel.name}: {len(missing)} source module(s) missing from wheel: {shown}{more}"
            )
    return len(modules)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("wheels", nargs="+", type=Path)
    parser.add_argument("--package", default=DEFAULT_PACKAGE)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=None,
        help="package source directory; every module in it must be in the wheel",
    )
    args = parser.parse_args(argv)

    status = 0
    for wheel in args.wheels:
        try:
            count = check_wheel(wheel, args.package, args.source_dir)
        except WheelContentError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            status = 1
        except (OSError, zipfile.BadZipFile) as exc:
            print(f"ERROR: cannot read {wheel}: {exc}", file=sys.stderr)
            return 2
        else:
            print(f"OK: {wheel.name} contains {count} '{args.package}' modules")
    return status


if __name__ == "__main__":
    raise SystemExit(main())
