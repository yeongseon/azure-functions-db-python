from datetime import date
import json
from pathlib import Path

import pytest

from tools.security_exceptions import parse_exceptions
from tools.security_report import (
    Check,
    EvidenceError,
    Status,
    identifier,
    integer,
    load,
    sequence,
    text,
    write_report,
)


@pytest.mark.parametrize("raw", ["null", "[]", '"text"', "true"])
def test_json_boundary_requires_object(raw: str) -> None:
    # Given a non-object; When parsed; Then the evidence boundary rejects it.
    with pytest.raises(EvidenceError):
        load(raw)


@pytest.mark.parametrize("value", [None, 0, True, ""])
def test_text_requires_nonempty_string(value: None | int | str) -> None:
    # Given invalid text; When parsed; Then it cannot enter metadata.
    with pytest.raises(EvidenceError):
        text(value)


@pytest.mark.parametrize("value", [None, True, -1, "1"])
def test_count_requires_nonnegative_integer(value: None | int | str) -> None:
    # Given invalid counts; When parsed; Then malformed evidence cannot pass.
    with pytest.raises(EvidenceError):
        integer(value)


@pytest.mark.parametrize("value", [None, "[]", 0])
def test_array_requires_sequence(value: None | str | int) -> None:
    # Given a nonarray; When parsed; Then the boundary rejects it.
    with pytest.raises(EvidenceError):
        sequence(value)


@pytest.mark.parametrize(
    "value",
    [
        "/tmp/file.py",
        "../secret",
        "https://example.com",
        "Authorization: Bearer CANARY_SECRET",
        "x?code=CANARY_SECRET",
        "postgresql+psycopg://user:CANARY_SECRET_pw@host/db",
    ],
)
def test_sensitive_metadata_is_rejected(value: str) -> None:
    # Given unsafe metadata; When parsed; Then credentials cannot enter evidence.
    with pytest.raises(EvidenceError):
        identifier(value)


def test_duplicate_exception_is_rejected() -> None:
    # Given duplicate exceptions; When parsed; Then accounting is unambiguous.
    rule = {
        "id": "CVE-1",
        "package": "example",
        "kind": "not-affected",
        "reason": "reviewed",
        "owner": "maintainer",
        "added": "2026-10-01",
        "expires": "2026-11-01",
    }
    with pytest.raises(EvidenceError):
        parse_exceptions(
            load(json.dumps({"exceptions": [rule, rule]}))["exceptions"], date(2026, 10, 23)
        )


def test_summary_appends_to_github_summary(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # Given prior summary content; When evidence is appended; Then prior content survives.
    summary = tmp_path / "summary.md"
    summary.write_text("existing\n")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert write_report([Check("scanner", (), Status.PASS, 0)], tmp_path / "out") == 0
    assert summary.read_text().startswith("existing\n")
