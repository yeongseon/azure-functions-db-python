from __future__ import annotations

import json
import os
import sys


def unexpected_results(
    results: dict[str, str], *, full_required: bool, docs_changed: bool
) -> list[str]:
    expected = dict.fromkeys(results, "success" if full_required else "skipped")
    expected["changes"] = "success"
    expected["docs-check"] = "success" if docs_changed else "skipped"
    return sorted(job for job, result in results.items() if result != expected[job])


def main() -> int:
    raw_results: dict[str, dict[str, str]] = json.loads(os.environ["RESULTS"])
    results = {job: data["result"] for job, data in raw_results.items()}
    for job, result in sorted(results.items()):
        print(f"{job}: {result}")
    failed = unexpected_results(
        results,
        full_required=os.environ["FULL_REQUIRED"] == "true",
        docs_changed=os.environ["DOCS_CHANGED"] == "true",
    )
    if failed:
        print(f"::error::Required checks had unexpected results: {failed}", file=sys.stderr)
        return 1
    print("All gating jobs matched their expected results.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
