from dataclasses import replace
import json
from pathlib import Path
import random
import string
import xml.etree.ElementTree as ET

import pytest

from tools.verification_evidence import Summary, publish, redact, save

BENIGN = (
    "Total lines of code: 3318",
    "Total lines of code: 100%",
    "test_percent[code=100%]",
    "test_percent[sig=1]",
    "token_count=42 tokens=100%",
    "author=alice secretary=present postcode=12345",
    "key=1 auth=2 pwd=3",
    "src/example.py 100 0 100%",
    "sig=1 code=0 tokens: 3",
)


@pytest.mark.parametrize("text", BENIGN)
def test_benign_evidence_survives_save_console_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], text: str
) -> None:
    log = tmp_path / "bandit.log"
    log.write_text(text)
    suite = ET.Element("testsuite", tests="1")
    ET.SubElement(suite, "testcase", name=text)
    ET.ElementTree(suite).write(tmp_path / "junit.xml", encoding="unicode")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "job.md"))
    summary = replace(Summary(1, "test", (), 1, 1, 0, 0, "PASS", 0), reason=text)
    publish(save(tmp_path, summary))
    assert log.read_text() == text
    case = ET.parse(tmp_path / "junit.xml").find("testcase")
    assert case is not None and case.get("name") == text
    assert json.loads((tmp_path / "summary.json").read_text())["reason"] == text
    assert (
        capsys.readouterr().out
        == f"test: PASS | scope=PARTIAL selected=1 passed=1 failed=0 skipped=0 exit=0 | {text}\n"
    )
    assert (
        (tmp_path / "job.md").read_text()
        == "\n- test: PASS | scope=PARTIAL selected=1 passed=1 failed=0 skipped=0 exit=0 | "
        + text
        + "\n"
    )


def test_seeded_benign_vocabulary_is_preserved() -> None:
    rng = random.Random(471)
    vocabulary = [
        "code",
        "sig",
        "token_count",
        "tokens",
        "author",
        "secretary",
        "key",
        "auth",
        "pwd",
        "passwords",
    ]
    for _ in range(2000):
        line = (
            f"{rng.choice(vocabulary)}{rng.choice(['=', ': '])}{rng.randrange(1000)}"
            f"{rng.choice(['%', '', ' lines'])}"
        )
        assert redact(line) == line


def test_seeded_credential_values_are_always_masked() -> None:
    rng = random.Random(466)
    for _ in range(500):
        value = "CANARY" + "".join(rng.choices(string.ascii_letters + string.digits, k=24))
        key = rng.choice(
            [
                "password",
                "passwd",
                "secret",
                "AccountKey",
                "SharedAccessKey",
                "Authorization",
                "?code",
                "sig",
            ]
        )
        assert value not in redact(f"{key}={value}")
