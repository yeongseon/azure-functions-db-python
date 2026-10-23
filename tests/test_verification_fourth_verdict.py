from __future__ import annotations

from collections.abc import Callable
from html import escape
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import random
import string
import subprocess
import sys
from urllib.parse import quote_plus
import uuid
import xml.etree.ElementTree as ET

import pytest

from tools.verification_evidence import redact

ROOT = Path(__file__).resolve().parents[1]
FUNCTION_KEY = "CANARYFUNCTION471" + "aB9_xY7-" * 4 + "Q1ab=="
PASSWORD = "CANARYESCAPED471"
ORACLE_SHAPES = (
    f"code: {FUNCTION_KEY}",
    f"code={FUNCTION_KEY}",
    json.dumps({"code": FUNCTION_KEY}),
    json.dumps(json.dumps({"password": PASSWORD})),
    json.dumps({"message": json.dumps({"password": PASSWORD})}),
    repr(json.dumps({"password": PASSWORD})),
    r"{\'password\': \'CANARYESCAPED471\'}",
)


def test_real_sqlite_lane_masks_fourth_verdict_keys_and_escaped_json(tmp_path: Path) -> None:
    tests = tmp_path / "tests/integration/db"
    tests.mkdir(parents=True)
    (tests / "test_fourth.py").write_text(
        "import pytest\npytestmark=[pytest.mark.integration,pytest.mark.sqlite]\n"
        f"@pytest.mark.parametrize('credential', {ORACLE_SHAPES!r})\n"
        "def test_fourth(credential):\n    assert False, credential\n"
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
    assert summary["status"] == "FAIL" and summary["selected"] == len(ORACLE_SHAPES)
    assert len(list(ET.parse(directory / "junit.xml").iter("testcase"))) == len(ORACLE_SHAPES)
    for path in (tmp_path / "test-results").rglob("*"):
        if not path.is_file():
            continue
        assert b"CANARY" not in path.read_bytes(), str(path)
        if path.suffix == ".json":
            json.loads(path.read_text())
        elif path.suffix == ".html":
            parser = HTMLParser()
            parser.feed(path.read_text())
            parser.close()
            assert "<html>" in path.read_text() and "</html>" in path.read_text()
    assert "CANARY" not in result.stdout + result.stderr + job.read_text()


@pytest.mark.parametrize("value", ORACLE_SHAPES)
def test_fourth_verdict_views_preserve_original_escaped_structure(value: str) -> None:
    source = json.dumps({"message": value, "ordinary": "path\\file"})
    result = redact(source)
    parsed = json.loads(result)
    assert parsed["ordinary"] == "path\\file"
    assert "CANARY" not in result


def test_seeded_benign_lines_include_hashes_statuses_paths_and_escaped_json() -> None:
    rng = random.Random(471004)
    for _ in range(3000):
        digest = "".join(rng.choices("0123456789abcdef", k=rng.choice([40, 64])))
        identifier = str(uuid.UUID(int=rng.getrandbits(128)))
        line = rng.choice(
            [
                f"status code: {rng.randrange(100, 600)}",
                f"exit code={rng.randrange(10)}",
                "HTTP code 404",
                "error code: E1234",
                "Total lines of code: 3318",
                f"git SHA {digest}",
                f"sha256: {digest}",
                f"key={rng.randrange(100)}",
                f"src/{identifier}/long_identifier_{digest}.py",
                f"test_identifier_{digest}",
                json.dumps(json.dumps({"message": "code=100%", "path": f"src/{identifier}.py"})),
                "tokens=10 author=ada secretary=available token_count=3",
            ]
        )
        assert redact(line) == line


def test_seeded_credentials_cover_all_encodings_and_carriers() -> None:
    rng = random.Random(471005)
    for index in range(1000):
        secret = "CANARY" + "".join(rng.choices(string.ascii_letters + string.digits + "_-", k=46))
        carriers = [
            f"code={secret}==",
            f"key: {secret}",
            f"password={secret}",
            json.dumps({"password": secret}),
            repr({"password": secret}),
            f"password: {secret}",
            f"Authorization: Bearer {secret}",
            f"https://user:{secret}@example.test/db",
            f"?code={secret}",
            f'<failure message="password={secret}"/>',
            f"<![CDATA[password={secret}]]>",
        ]
        value = carriers[index % len(carriers)]
        transforms: list[Callable[[str], str]] = [
            str,
            json.dumps,
            lambda s: json.dumps(json.dumps(s)),
            quote_plus,
            escape,
            repr,
            lambda s: s.replace('"', r"\""),
            lambda s: s.replace("p", r"\u0070"),
            lambda s: quote_plus(json.dumps(s)),
        ]
        encoded = transforms[index % len(transforms)](value)
        assert "CANARY" not in redact(encoded), (index, encoded)
