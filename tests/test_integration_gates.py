"""Negative cases: required integration lanes must not go green falsely.

Each test runs the real integration suite in a subprocess, so it exercises the
same pytest exit code a CI step would see. No database is needed: the cases
below fail or skip before any connection is attempted.
"""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_URL_VARS = (
    "TEST_SQLITE_URL",
    "TEST_POSTGRES_URL",
    "TEST_MYSQL_URL",
    "TEST_MSSQL_URL",
    "TEST_AZURITE_CONN_STR",
    "INTEGRATION_FAIL_ON_SKIP",
)


def _run(marker: str, extra: tuple[str, ...] = (), **env: str) -> subprocess.CompletedProcess[str]:
    child_env = {k: v for k, v in os.environ.items() if k not in _URL_VARS}
    child_env.update(env)
    return subprocess.run(  # noqa: S603
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/integration",
            "-o",
            "addopts=",
            "-m",
            marker,
            "-q",
            "-p",
            "no:cacheprovider",
            *extra,
        ],
        cwd=_ROOT,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


@pytest.mark.parametrize("backend", ["sqlite", "postgres", "mysql", "mssql"])
def test_every_live_module_is_selected_by_each_backend_marker(backend: str) -> None:
    listing = _run(f"integration and {backend}", extra=("--collect-only",))
    for module in ("test_reader_live", "test_source_live", "test_writer_live"):
        assert module in listing.stdout


def test_missing_backend_url_fails_when_skips_are_forbidden() -> None:
    result = _run("integration and postgres", INTEGRATION_FAIL_ON_SKIP="1")
    assert result.returncode != 0
    assert "INTEGRATION_FAIL_ON_SKIP=1" in result.stdout


def test_missing_backend_url_only_skips_by_default() -> None:
    result = _run("integration and postgres")
    assert result.returncode == 0


def test_wrong_dialect_url_fails_even_without_skip_policy() -> None:
    result = _run("integration and postgres", TEST_POSTGRES_URL="sqlite:///wrong.db")
    assert result.returncode != 0
    assert "TEST_POSTGRES_URL does not use the 'postgresql' dialect" in result.stdout


def test_wrong_dialect_error_never_echoes_credentials() -> None:
    secret = "s3cr3t-pa55"  # noqa: S105
    result = _run(
        "integration and postgres",
        TEST_POSTGRES_URL=f"mysql+pymysql://user:{secret}@localhost:1/x",
    )
    assert result.returncode != 0
    assert secret not in result.stdout + result.stderr


def test_azurite_without_connection_string_fails_when_skips_are_forbidden() -> None:
    result = _run("integration and azurite", INTEGRATION_FAIL_ON_SKIP="1")
    assert result.returncode != 0


def test_zero_selected_tests_is_a_nonzero_exit() -> None:
    result = _run("integration and sqlite and mssql")
    assert result.returncode == 5
