from collections.abc import Callable
import json
from pathlib import Path
import random
import time
from urllib.parse import quote_plus

import pytest

from tools.verification_evidence import Summary, redact, report, save


@pytest.mark.parametrize("key", ["Password", "Passwd", "Pwd", "Secret", "AccountKey"])
@pytest.mark.parametrize(
    "value",
    [
        "{first;ORACLEADO471}",
        "{first}};ORACLEADO471}",
        '"first;ORACLEADO471"',
        "'first;ORACLEADO471'",
        "{first;ORACLEADO471",
    ],
)
def test_complete_credential_values_are_masked(key: str, value: str) -> None:
    for text in (f"{key}={value}", quote_plus(f"{key}={value}"), json.dumps(f"{key}={value}")):
        assert "ORACLE" not in redact(text)


def test_seeded_redaction_is_idempotent() -> None:
    rng = random.Random(471006)
    for _ in range(2000):
        key = rng.choice(["PWD", "Password", "secret", "sig", "?code", "Authorization"])
        value = rng.choice(
            [
                "[REDACTED]",
                "[MASKED]",
                "***",
                "****",
                "<redacted>",
                "xxxxx",
                "{first;CREDENTIAL471}",
                "{first}};CREDENTIAL471}",
                "CANARYSECRET471",
            ]
        )
        transforms: list[Callable[[str], str]] = [str, json.dumps, quote_plus]
        text = rng.choice(transforms)(f"{key}={value}")
        assert redact(redact(text)) == redact(text)


def test_saved_reports_are_stable_across_repeated_save_and_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    (tmp_path / "junit.xml").write_text('<testsuite><testcase name="PWD=[REDACTED]"/></testsuite>')
    (tmp_path / "report.html").write_text("<html><body>PWD=[REDACTED]</body></html>")
    (tmp_path / "runner.log").write_text("PWD=[REDACTED]")
    summary = Summary(1, "test", (), 1, 1, 0, 0, "PASS", 0)
    save(tmp_path, summary)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    save(tmp_path, summary)
    report(tmp_path)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


@pytest.mark.parametrize("text", ["Password={" * 100000, "{" * 400000, "Password={" + "{" * 400000])
def test_adversarial_braces_are_linear(text: str) -> None:
    start = time.perf_counter()
    redact(text)
    assert time.perf_counter() - start < 10


def test_five_megabyte_log_stays_within_budget() -> None:
    text = "status code: 200 Total lines of code: 3318\n" * 130000
    start = time.perf_counter()
    assert redact(text) == text
    assert time.perf_counter() - start < 10
