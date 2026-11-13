"""Docs must not state a coverage threshold that differs from pyproject.toml (#530)."""

from __future__ import annotations

from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parent.parent
DOCS = [ROOT / "docs" / "testing.md", ROOT / "docs" / "contributing.md", ROOT / "AGENTS.md"]

# `fail_under = 95`, "95% coverage threshold", "Coverage threshold: 95%"
_PATTERNS = (
    re.compile(r"fail_under\s*=\s*(\d+)"),
    re.compile(r"(\d+)%\s+coverage threshold", re.IGNORECASE),
    re.compile(r"coverage threshold:?\s*\**(\d+)%", re.IGNORECASE),
    re.compile(r"coverage at \*\*(\d+)% or above\*\*", re.IGNORECASE),
)


def _configured() -> int:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return int(data["tool"]["coverage"]["report"]["fail_under"])


def _stated(text: str) -> list[int]:
    return [int(m.group(1)) for pat in _PATTERNS for m in pat.finditer(text)]


def test_documented_thresholds_match_pyproject() -> None:
    configured = _configured()
    for doc in DOCS:
        stated = _stated(doc.read_text(encoding="utf-8"))
        assert stated, f"{doc.name} no longer states a coverage threshold the check can read"
        assert set(stated) == {configured}, f"{doc.name} says {stated}, pyproject has {configured}"


def test_checker_detects_stale_value() -> None:
    assert _stated("The project enforces a **90% coverage threshold**.") == [90]
    assert _stated("fail_under = 90") == [90]
    assert _stated("- Coverage threshold: 90%.") == [90]
