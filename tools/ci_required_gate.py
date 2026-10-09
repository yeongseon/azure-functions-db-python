from __future__ import annotations

import json
import os
import sys

EXPECTED_JOBS = frozenset(
    {
        "changes",
        "docs-check",
        "quality",
        "test",
        "minimum-dependencies",
        "artifact-build",
        "artifact-python310-negative",
        "artifact-python311",
        "shellcheck",
        "host-smoke",
    }
)


def parse_bool(value: str) -> bool | None:
    if value not in {"true", "false"}:
        return None
    return value == "true"


def unexpected_results(
    results: dict[str, str], *, full_required: bool, docs_changed: bool
) -> list[str]:
    expected = dict.fromkeys(results, "success")
    if not full_required:
        expected.update(
            (job, "skipped") for job in EXPECTED_JOBS if job not in {"changes", "docs-check"}
        )
    expected["changes"] = "success"
    expected["docs-check"] = "success" if docs_changed else "skipped"
    failed = {job for job, result in results.items() if result != expected[job]}
    failed.update(EXPECTED_JOBS.symmetric_difference(results))
    return sorted(failed)


def main() -> int:
    raw_results: dict[str, dict[str, str]] = json.loads(os.environ["RESULTS"])
    results = {job: data["result"] for job, data in raw_results.items()}
    for job, result in sorted(results.items()):
        print(f"{job}: {result}")
    full_required = parse_bool(os.environ["FULL_REQUIRED"])
    docs_changed = parse_bool(os.environ["DOCS_CHANGED"])
    if full_required is None or docs_changed is None:
        print("::error::Classifier outputs must be exactly true or false", file=sys.stderr)
        return 1
    if not full_required and not docs_changed:
        print("::error::Classifier cannot skip both matrix and docs checks", file=sys.stderr)
        return 1
    failed = unexpected_results(
        results,
        full_required=full_required,
        docs_changed=docs_changed,
    )
    if failed:
        print(f"::error::Required checks had unexpected results: {failed}", file=sys.stderr)
        return 1
    print("All gating jobs matched their expected results.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
