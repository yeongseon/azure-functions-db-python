from __future__ import annotations

import json

import pytest

from tools.ci_required_gate import main, unexpected_results

REQUIRED_JOBS = {
    "changes": "success",
    "docs-check": "skipped",
    "quality": "success",
    "test": "success",
    "minimum-dependencies": "success",
    "artifact-build": "success",
    "artifact-python310-negative": "success",
    "artifact-python311": "success",
    "shellcheck": "success",
    "host-smoke": "success",
}
DOCS_ONLY_JOBS = {
    job: "success" if job in {"changes", "docs-check"} else "skipped" for job in REQUIRED_JOBS
}


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
    baseline = REQUIRED_JOBS if full_required else DOCS_ONLY_JOBS
    assert (
        not unexpected_results(
            baseline | results,
            full_required=full_required,
            docs_changed=docs_changed,
        )
    ) is expected


@pytest.mark.parametrize("value", ["", "yes", "TRUE", "0"])
@pytest.mark.parametrize("variable", ["FULL_REQUIRED", "DOCS_CHANGED"])
def test_required_gate_rejects_missing_or_invalid_classifier_outputs(
    monkeypatch: pytest.MonkeyPatch,
    variable: str,
    value: str,
) -> None:
    # Given: successful jobs but one classifier output is not an exact boolean.
    raw_results = {job: {"result": result} for job, result in REQUIRED_JOBS.items()}
    monkeypatch.setenv("RESULTS", json.dumps(raw_results))
    monkeypatch.setenv("FULL_REQUIRED", "true")
    monkeypatch.setenv("DOCS_CHANGED", "false")
    monkeypatch.setenv(variable, value)

    # When: the required-check policy evaluates the untrusted output.
    result = main()

    # Then: malformed or absent output fails closed.
    assert result == 1


def test_required_gate_rejects_classifier_that_skips_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: the classifier succeeds but requests neither matrix nor docs checks.
    raw_results = {job: {"result": result} for job, result in DOCS_ONLY_JOBS.items()}
    monkeypatch.setenv("RESULTS", json.dumps(raw_results))
    monkeypatch.setenv("FULL_REQUIRED", "false")
    monkeypatch.setenv("DOCS_CHANGED", "false")

    # When: the required-check policy evaluates the impossible state.
    result = main()

    # Then: the policy fails instead of accepting a fully skipped CI run.
    assert result == 1


@pytest.mark.parametrize("result", ["failure", "skipped"])
def test_required_gate_rejects_unknown_unsuccessful_job(result: str) -> None:
    # Given: a newly added dependency has not been classified as skippable.
    results = DOCS_ONLY_JOBS | {"new-job": result}

    # When: a docs-only run is evaluated.
    failed = unexpected_results(results, full_required=False, docs_changed=True)

    # Then: the unknown dependency must succeed.
    assert failed == ["new-job"]
