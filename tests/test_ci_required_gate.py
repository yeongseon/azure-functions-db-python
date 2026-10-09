from __future__ import annotations

import pytest

from tools.ci_required_gate import unexpected_results


@pytest.mark.parametrize(
    ("results", "full_required", "docs_changed", "expected"),
    [
        (
            {"changes": "success", "docs-check": "success", "test": "skipped"},
            False,
            True,
            True,
        ),
        (
            {"changes": "success", "docs-check": "failure", "test": "skipped"},
            False,
            True,
            False,
        ),
        (
            {"changes": "success", "docs-check": "skipped", "test": "skipped"},
            True,
            False,
            False,
        ),
        (
            {"changes": "success", "docs-check": "skipped", "test": "failure"},
            True,
            False,
            False,
        ),
        (
            {"changes": "failure", "docs-check": "success", "test": "skipped"},
            False,
            True,
            False,
        ),
        (
            {"changes": "success", "docs-check": "skipped", "test": "cancelled"},
            True,
            False,
            False,
        ),
    ],
)
def test_required_gate_rejects_unexpected_results(
    results: dict[str, str], full_required: bool, docs_changed: bool, expected: bool
) -> None:
    assert (
        not unexpected_results(
            results,
            full_required=full_required,
            docs_changed=docs_changed,
        )
    ) is expected
