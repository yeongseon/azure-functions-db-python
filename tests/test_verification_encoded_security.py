from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import quote, quote_plus
import xml.etree.ElementTree as ET

import pytest

from tools.verification_environment import clean
from tools.verification_evidence import Summary, publish, redact, save

ROOT = Path(__file__).resolve().parents[1]
SHAPES = (
    'AccountKey="space CANARYENCODED471 value"',
    "SharedAccessKey='space CANARYSHARED471 +/ value'",
    'password="space CANARYPASSWORD471 value"',
    "password=CANARYPLAIN471",
    "Authorization: Bearer CANARYAUTH471",
    "postgresql://user:CANARYURL471@host/db",
    "sig=CANARYSAS471+/&sp=r",
)
ENCODED = tuple(
    encoder(value)
    for value in SHAPES
    for encoder in (
        quote,
        quote_plus,
        lambda text: quote(quote(text)),
        lambda text: quote(quote(quote(text))),
    )
)


def test_real_lane_redacts_encoded_failures_from_all_artifacts_and_console(tmp_path: Path) -> None:
    # Given a real failing SQLite selection with encoded credentials in messages and locals.
    tests = tmp_path / "tests/integration/db"
    tests.mkdir(parents=True)
    (tests / "test_encoded.py").write_text(
        "import pytest\npytestmark=[pytest.mark.integration,pytest.mark.sqlite]\n"
        f"@pytest.mark.parametrize('credential', {ENCODED!r})\n"
        "def test_encoded(credential):\n    assert False, credential\n"
    )
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
        env=dict(os.environ, PYTHONPATH=str(ROOT)),
        capture_output=True,
        text=True,
        timeout=60,
    )
    # Then counts and valid reports remain while every output surface hides the canaries.
    directory = tmp_path / "test-results/integration/sqlite"
    summary = json.loads((directory / "summary.json").read_text())
    assert summary["status"] == "FAIL" and summary["selected"] == len(ENCODED)
    assert len(list(ET.parse(directory / "junit.xml").iter("testcase"))) == len(ENCODED)
    leaks = [
        str(path.relative_to(tmp_path))
        for path in (tmp_path / "test-results").rglob("*")
        if path.is_file() and b"CANARY" in path.read_bytes()
    ]
    assert leaks == []
    assert "CANARY" not in result.stdout + result.stderr


@pytest.mark.parametrize("value", ENCODED)
def test_encoded_redaction_preserves_unrelated_original_spelling(value: str) -> None:
    original = "prefix%20unchanged+literal " + value + "\nend%2Funchanged"
    result = redact(original)
    assert "CANARY" not in result
    assert result.startswith("prefix%20unchanged+literal ")
    assert result.endswith("\nend%2Funchanged")


def test_encoded_summary_environment_and_job_summary_share_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    value = ENCODED[0]
    monkeypatch.setenv("EXPECTED_VERSION", value)
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "job.txt"))
    summary = replace(Summary(1, "test", (), 1, 0, 1, 0, "FAIL", 1), reason=value)
    publish(save(tmp_path, summary))
    assert "CANARY" not in capsys.readouterr().out
    assert all(b"CANARY" not in path.read_bytes() for path in tmp_path.iterdir())


def test_clean_refuses_symlinked_checkout_root_without_deleting_anything(tmp_path: Path) -> None:
    external = tmp_path / "external"
    cache = external / "src/__pycache__"
    cache.mkdir(parents=True)
    (cache / "unrelated.pyc").write_bytes(b"cache sentinel")
    (external / "dist").mkdir()
    (external / "dist/sentinel").write_bytes(b"artifact sentinel")
    link = tmp_path / "checkout"
    link.symlink_to(external, target_is_directory=True)
    with pytest.raises(ValueError, match="symlinked checkout root"):
        clean(link)
    assert (cache / "unrelated.pyc").read_bytes() == b"cache sentinel"
    assert (external / "dist/sentinel").read_bytes() == b"artifact sentinel"


@pytest.mark.parametrize("linked_parent", [False, True])
def test_clean_accepts_real_root_below_real_or_linked_ancestor(
    tmp_path: Path, linked_parent: bool
) -> None:
    parent = tmp_path / "parent"
    root = parent / "checkout"
    (root / "src/__pycache__").mkdir(parents=True)
    (root / "dist").mkdir()
    if linked_parent:
        alias = tmp_path / "alias"
        alias.symlink_to(parent, target_is_directory=True)
        root = alias / "checkout"
    clean(root)
    assert not (root / "src/__pycache__").exists()
    assert not (root / "dist").exists()


def test_clean_cli_refuses_logical_linked_root(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / "dist").mkdir(parents=True)
    (checkout / "dist/sentinel").write_text("preserve")
    link = tmp_path / "linked"
    link.symlink_to(checkout, target_is_directory=True)
    result = subprocess.run(
        [sys.executable, "-m", "tools.verification", "clean"],
        cwd=link,
        env=dict(os.environ, PYTHONPATH=str(ROOT), PWD=str(link)),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "symlinked checkout root" in result.stderr
    assert (checkout / "dist/sentinel").read_text() == "preserve"
