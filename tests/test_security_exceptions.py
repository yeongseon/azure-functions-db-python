"""Every pip-audit exception names an owner and reason and has a future expiry."""

from datetime import date
from pathlib import Path

IGNORE_FILE = Path(__file__).resolve().parents[1] / "security" / "pip-audit-ignore.txt"


def parse(text: str, today: date) -> list[str]:
    ids = []
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        fields = line.split(maxsplit=3)
        assert len(fields) == 4, f"expected '<ID> <owner> <expires> <reason>': {line}"
        assert date.fromisoformat(fields[2]) > today, f"{fields[0]} exception expired"
        ids.append(fields[0])
    return ids


def test_shipped_exceptions_are_valid() -> None:
    parse(IGNORE_FILE.read_text(encoding="utf-8"), date.today())


def test_expired_or_malformed_entries_are_rejected() -> None:
    for bad in ("X me 2020-01-01 why", "X me", "X me soon why"):
        try:
            parse(bad, date(2026, 1, 1))
        except (AssertionError, ValueError):
            continue
        raise AssertionError(f"accepted: {bad}")
    assert parse("X me 2999-01-01 why", date(2026, 1, 1)) == ["X"]
