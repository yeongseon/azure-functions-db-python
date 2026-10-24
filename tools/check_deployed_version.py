"""Verify a deployed Azure Functions app reports the exact candidate package
version before the e2e-azure workflow records a certification (#442).

The remote Azure build installs the Function App's requirements.txt, not the
checked-out source tree. Without this check, a successful live e2e run
proves the deployed code responds correctly, but not that it is running the
exact candidate build being certified -- it could be resolving an older (or
newer) published release from PyPI instead. This module compares the
version reported by the deployed app's ``/api/version`` endpoint against the
expected candidate version, independent of any Azure access so the
comparison logic itself is unit-testable.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


class VersionMismatchError(Exception):
    """Raised when the deployed app's reported version is missing or differs
    from the expected candidate version."""


def assert_version_matches(actual: str | None, expected: str) -> None:
    """Raise ``VersionMismatchError`` unless ``actual`` exactly equals
    ``expected``. ``actual`` is ``None`` when the endpoint did not respond,
    returned no ``version`` field, or could not be parsed as JSON -- treated
    identically to a mismatch so this fails closed."""
    if actual is None:
        msg = f"deployed app did not report a version (expected {expected!r})"
        raise VersionMismatchError(msg)
    if actual != expected:
        msg = f"deployed version {actual!r} does not match candidate {expected!r}"
        raise VersionMismatchError(msg)


def fetch_deployed_version(base_url: str, *, timeout: float = 30.0) -> str | None:
    """Fetch the ``version`` field from ``<base_url>/api/version``.

    Returns ``None`` on any network error, non-200 response, or malformed/
    missing JSON payload, rather than raising -- callers decide how a
    missing version should be treated via :func:`assert_version_matches`.
    """
    url = f"{base_url.rstrip('/')}/api/version"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:  # noqa: S310  # nosec B310 # https URL of the e2e app
            if response.status != 200:  # noqa: PLR2004
                return None
            payload = json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None
    version = payload.get("version")
    return version if isinstance(version, str) else None


def main() -> int:
    base_url = os.environ["E2E_BASE_URL"]
    expected = os.environ["EXPECTED_VERSION"]
    actual = fetch_deployed_version(base_url)
    try:
        assert_version_matches(actual, expected)
    except VersionMismatchError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1
    print(f"Deployed app identity verified: version {expected} matches candidate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
