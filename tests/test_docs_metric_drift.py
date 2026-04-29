"""Guards against drift between metric constants in observability.py and the
metric names cited in docs/.

The docs/11-observability.md metrics table is the single source of truth: each
row marks a metric as ``implemented`` or ``deferred``. Every implemented metric
must have a matching ``METRIC_*`` constant; every deferred metric must NOT have
a constant yet. Any other doc reference (`azfdb_*` token outside the table)
must resolve to a known constant.

Adding a new metric without updating the docs table — or marking it
``implemented`` without defining the constant — fails this test.
"""

from __future__ import annotations

from pathlib import Path
import re

from azure_functions_db import observability

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DOCS_DIR = _REPO_ROOT / "docs"
_OBSERVABILITY_DOC = _DOCS_DIR / "11-observability.md"

_METRIC_TOKEN_RE = re.compile(r"\bazfdb_[a-z0-9_]+\b")
_METRICS_TABLE_ROW_RE = re.compile(
    r"^\|\s*(?:Counter|Gauge|Histogram|Summary)\s*\|\s*"
    r"`(?P<name>azfdb_[a-z0-9_]+)`\s*\|\s*"
    r"(?P<status>implemented|deferred)\s*\|",
    re.IGNORECASE,
)


def _implemented_constants() -> set[str]:
    return {
        value
        for name, value in vars(observability).items()
        if name.startswith("METRIC_") and isinstance(value, str)
    }


def _parse_metrics_table() -> dict[str, str]:
    """Return ``{metric_name: status}`` for each row in the observability metrics table."""
    table: dict[str, str] = {}
    for raw_line in _OBSERVABILITY_DOC.read_text(encoding="utf-8").splitlines():
        match = _METRICS_TABLE_ROW_RE.match(raw_line)
        if match is None:
            continue
        table[match.group("name")] = match.group("status").lower()
    return table


def _all_doc_references() -> dict[str, set[Path]]:
    """Return ``{metric_name: {files_that_mention_it}}`` across every doc."""
    references: dict[str, set[Path]] = {}
    for doc_path in _DOCS_DIR.rglob("*.md"):
        text = doc_path.read_text(encoding="utf-8")
        for token in _METRIC_TOKEN_RE.findall(text):
            references.setdefault(token, set()).add(doc_path)
    return references


def test_metrics_table_parses() -> None:
    table = _parse_metrics_table()
    assert table, (
        f"Failed to parse any metric rows from {_OBSERVABILITY_DOC.relative_to(_REPO_ROOT)}; "
        "the regex or table format may have changed"
    )


def test_implemented_metrics_have_matching_constants() -> None:
    table = _parse_metrics_table()
    constants = _implemented_constants()

    documented_implemented = {name for name, status in table.items() if status == "implemented"}
    missing_constants = documented_implemented - constants

    assert not missing_constants, (
        "Metrics documented as 'implemented' but missing METRIC_* constant in "
        f"observability.py: {sorted(missing_constants)}"
    )


def test_deferred_metrics_have_no_constant_yet() -> None:
    table = _parse_metrics_table()
    constants = _implemented_constants()

    documented_deferred = {name for name, status in table.items() if status == "deferred"}
    leaked = documented_deferred & constants

    assert not leaked, (
        "Metrics marked 'deferred' in docs but already defined as METRIC_* constants — "
        f"flip the docs status to 'implemented': {sorted(leaked)}"
    )


def test_every_constant_appears_in_metrics_table() -> None:
    table = _parse_metrics_table()
    constants = _implemented_constants()

    undocumented = constants - set(table)

    assert not undocumented, (
        "METRIC_* constants in observability.py with no row in the metrics table "
        f"of docs/11-observability.md: {sorted(undocumented)}"
    )


def test_doc_references_resolve_to_known_metrics() -> None:
    table = _parse_metrics_table()
    constants = _implemented_constants()
    references = _all_doc_references()

    known = constants | set(table)
    unknown = {name: paths for name, paths in references.items() if name not in known}

    if unknown:
        formatted = "\n".join(
            f"  - {name} cited in {sorted(p.relative_to(_REPO_ROOT) for p in paths)}"
            for name, paths in sorted(unknown.items())
        )
        raise AssertionError(
            "Docs reference metric tokens not present in observability.py constants "
            f"and not listed in the metrics table:\n{formatted}"
        )
