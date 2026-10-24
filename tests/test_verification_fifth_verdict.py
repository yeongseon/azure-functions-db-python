from __future__ import annotations

from html.parser import HTMLParser
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import pytest

from tools.verification_evidence import Summary, redact, report, save

ROOT = Path(__file__).resolve().parents[1]
DSN = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=host;DATABASE=db;UID=sa;"
    "PWD=CANARYODBC471;TrustServerCertificate=yes"
)
CASES = (
    "Server=host;User ID=sa;Password={first;ORACLEADO471};Database=db",
    "Server=host;Passwd={first}};ORACLEBRACE471};Database=db",
    'Password="first;ORACLEDOUBLE471"',
    "Password='first;ORACLESINGLE471'",
    "Password={first;ORACLEUNTERMINATED471",
    DSN,
    quote_plus(DSN),
    json.dumps({"PWD": "CANARYJSONPWD471"}),
    "pwd=CANARYLOWER471",
    "Pwd={CANARYBRACE471;p;w}",
    "User ID=sa;Password=CANARYADO471",
    "Password=CANARYPASSWORD471",
    "mssql+pyodbc:///?odbc_connect=" + quote_plus(DSN),
)


def test_real_lane_odbc_ado_and_existing_masks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tests = tmp_path / "tests/integration/db"
    tests.mkdir(parents=True)
    masks = tuple(
        f"postgresql://sa:{mask}@host/db"
        for mask in ("***", "****", "<redacted>", "[REDACTED]", "xxxxx")
    )
    (tests / "test_odbc.py").write_text(
        "import pytest\npytestmark=[pytest.mark.integration,pytest.mark.sqlite]\n"
        f"@pytest.mark.parametrize('credential', {(CASES + masks)!r})\n"
        "def test_odbc(credential):\n    assert False, credential\n"
    )
    job = tmp_path / "job.md"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.verification",
            "backend",
            "sqlite",
            "--owned",
            "-rA",
            "--showlocals",
            "--html=test-results/integration/sqlite/report.html",
            "--self-contained-html",
        ],
        cwd=tmp_path,
        env=dict(os.environ, PYTHONPATH=str(ROOT), GITHUB_STEP_SUMMARY=str(job)),
        capture_output=True,
        text=True,
        timeout=60,
    )
    directory = tmp_path / "test-results/integration/sqlite"
    summary = json.loads((directory / "summary.json").read_text())
    assert summary["status"] == "FAIL" and summary["selected"] == len(CASES + masks)
    assert len(list(ET.parse(directory / "junit.xml").iter("testcase"))) == len(CASES + masks)
    leaks = [
        str(path)
        for path in (tmp_path / "test-results").rglob("*")
        if path.is_file() and b"CANARY" in path.read_bytes()
    ]
    assert leaks == []
    assert all(
        b"ORACLE" not in path.read_bytes() for path in directory.rglob("*") if path.is_file()
    )
    assert "CANARY" not in result.stdout + result.stderr + job.read_text()
    for path in directory.rglob("*.json"):
        json.loads(path.read_text())
    parser = HTMLParser()
    parser.feed((directory / "report.html").read_text())
    before = {path: path.read_bytes() for path in directory.rglob("*") if path.is_file()}
    saved = Summary(**summary)
    monkeypatch.chdir(tmp_path)
    relative = Path("test-results/integration/sqlite")
    save(relative, saved)
    save(relative, saved)
    report(relative)
    assert {path: path.read_bytes() for path in directory.rglob("*") if path.is_file()} == before


@pytest.mark.parametrize("mask", ["***", "****", "<redacted>", "[REDACTED]", "xxxxx"])
def test_existing_url_masks_are_byte_identical_and_idempotent(mask: str) -> None:
    text = f"postgresql://sa:{mask}@host/db"
    assert redact(text) == text
    assert redact(redact(text)) == text


@pytest.mark.parametrize("text", ["pwd=3", "PWD=/home/user", r"PWD=C:\Users\name"])
def test_shell_pwd_and_small_numeric_values_are_preserved(text: str) -> None:
    assert redact(text) == text


@pytest.mark.parametrize("value", ["3", "/home/user", r"C:\Users\name"])
def test_explicit_dsn_pwd_values_remain_sensitive(value: str) -> None:
    text = f"SERVER=host;UID=sa;PWD={value};DATABASE=db"
    assert f"PWD={value};" not in redact(text)


@pytest.mark.parametrize("character", ["A", "%", "\\", "=", ";", '"'])
def test_long_benign_lines_are_fast_and_preserved(character: str) -> None:
    text = character * 200_000
    start = time.perf_counter()
    assert redact(text) == text
    assert time.perf_counter() - start < 3


def test_megabyte_benign_log_has_near_linear_scaling() -> None:
    line = "Total lines of code: 3318 status code: 200 abcdef0123456789\n"
    text = line * (1_000_000 // len(line))
    durations = []
    for value in (text, text * 2):
        start = time.perf_counter()
        assert redact(value) == value
        durations.append(time.perf_counter() - start)
    assert durations[0] < 3
    assert durations[1] / max(durations[0], 0.001) < 3.5
