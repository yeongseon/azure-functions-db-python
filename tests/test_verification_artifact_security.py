from __future__ import annotations

from html import escape
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET

import pytest

from tools.verification_evidence import Summary, save

ROOT = Path(__file__).resolve().parents[1]
SUMMARY = Summary(1, "test", (), 1, 0, 1, 0, "FAIL", 1)
CREDENTIALS = (
    'AccountKey="space CANARY471ACCOUNT value"',
    "SharedAccessKey='space CANARY471SHARED value'",
    'password="space CANARY471PASSWORD value"',
    "password=CANARY471PLAIN",
    "postgresql://user:CANARYHTML471@host/db",
    "sig=CANARY471SAS&sp=r",
    "Authorization: Bearer CANARY471AUTH",
    json.dumps(
        {"authorization": "Bearer CANARY471JSONAUTH", "password": "quoted CANARY471JSON value"}
    ),
    escape('AccountKey="space CANARY471ESCAPED value"'),
)


def test_real_sqlite_failure_redacts_every_uploaded_artifact(tmp_path: Path) -> None:
    # Given an injected failing test executed through the real owned SQLite lane.
    tests = tmp_path / "tests/integration/db"
    tests.mkdir(parents=True)
    (tests / "test_canary.py").write_text(
        "import pytest\npytestmark = [pytest.mark.integration, pytest.mark.sqlite]\n"
        f"@pytest.mark.parametrize('credential', {CREDENTIALS!r})\n"
        "def test_credential(credential):\n"
        "    pytest.fail(credential, pytrace=False)\n"
    )
    environment = dict(os.environ, PYTHONPATH=str(ROOT), GITHUB_STEP_SUMMARY=str(tmp_path / "job"))
    # When pytest writes actual JUnit, captured output and a self-contained HTML report.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "tools.verification",
            "backend",
            "sqlite",
            "--owned",
            "--html=test-results/integration/sqlite/report.html",
            "--self-contained-html",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    directory = tmp_path / "test-results/integration/sqlite"
    summary = json.loads((directory / "summary.json").read_text())
    assert result.returncode != 0
    assert summary["status"] == "FAIL"
    assert summary["selected"] == len(CREDENTIALS)
    assert len(list(ET.parse(directory / "junit.xml").iter("testcase"))) == len(CREDENTIALS)
    assert (directory / "report.html").exists()
    # Then every uploaded file is credential-free, including encoded spellings.
    leaks = [
        str(path.relative_to(tmp_path))
        for path in (tmp_path / "test-results").rglob("*")
        if path.is_file() and b"CANARY" in path.read_bytes()
    ]
    assert leaks == []


@pytest.mark.parametrize(
    "key",
    [
        "authorization",
        "proxy-authorization",
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "api-key",
        "connection_string",
        "connection-string",
        "accountkey",
        "sharedaccesskey",
        "sig",
        "code",
        "x-functions-key",
        "cookie",
        "set-cookie",
    ],
)
def test_json_sensitive_keys_mask_arbitrary_nested_values(tmp_path: Path, key: str) -> None:
    # Given sensitive keys whose values contain no recognizable credential prefix.
    data = {"nested": [{key: ["CANARYJSON471", {"opaque": "CANARYJSON471"}]}], "count": 3}
    (tmp_path / "extra.json").write_text(json.dumps(data))
    save(tmp_path, SUMMARY)
    result = json.loads((tmp_path / "extra.json").read_text())
    assert result["nested"][0][key] == "[REDACTED]"
    assert result["count"] == 3


def test_xml_redacts_decoded_nodes_attributes_properties_and_cdata(tmp_path: Path) -> None:
    # Given credentials in every XML carrier, including entity-encoded attribute quotes.
    secret = escape('SharedAccessKey="space CANARYXML471 value"', quote=True)
    (tmp_path / "junit.xml").write_text(
        f'<testsuite tests="1" failures="1" label="{secret}"><properties>'
        '<property name="password" value="CANARYPROPERTY471"/></properties>'
        f'<testcase name="{secret}"><failure message="{secret}">{secret}</failure>'
        '<system-out><![CDATA[AccountKey="space CANARYCDATA471 value"]]></system-out>'
        f'<system-err>{secret}</system-err><skipped message="{secret}"/>'
        f'<error message="{secret}">{secret}</error></testcase>{secret}</testsuite>'
    )
    save(tmp_path, SUMMARY)
    root = ET.parse(tmp_path / "junit.xml").getroot()
    assert root.attrib["tests"] == "1"
    assert len(list(root.iter("testcase"))) == 1
    assert b"CANARY" not in (tmp_path / "junit.xml").read_bytes()


@pytest.mark.parametrize("suffix", [".xml", ".html", ".json", ".log", ".txt", ".md", ".out"])
def test_every_textual_suffix_redacts_encoded_credentials(tmp_path: Path, suffix: str) -> None:
    value = 'AccountKey="space CANARYSUFFIX471 value"'
    text = escape(value)
    if suffix == ".xml":
        text = f"<report>{text}</report>"
    elif suffix == ".html":
        text = f"<!DOCTYPE html><html><body><pre>{text}</pre></body></html>"
    elif suffix == ".json":
        text = json.dumps({"message": value})
    path = tmp_path / ("report" + suffix)
    path.write_text(text)
    save(tmp_path, SUMMARY)
    assert "CANARY" not in path.read_text()


def test_supporting_inventory_excludes_unredacted_symlinks(tmp_path: Path) -> None:
    outside = tmp_path.parent / (tmp_path.name + "-outside.log")
    outside.write_text("password=EXTERNAL471")
    link = tmp_path / "linked.log"
    link.symlink_to(outside)
    summary = save(tmp_path, SUMMARY)
    assert str(link) not in summary.supporting_files
    assert outside.read_text() == "password=EXTERNAL471"
