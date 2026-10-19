"""Azure-deployed e2e smoke tests.

Usage:
    E2E_BASE_URL=https://<app>.azurewebsites.net pytest tests/e2e/azure -v --no-cov

These tests run against a real Azure Functions deployment and validate
that the package works end-to-end in the cloud.
"""

from __future__ import annotations

import os
import time
from typing import Any

import pytest
import requests

BASE_URL = os.environ.get("E2E_BASE_URL", "").rstrip("/")
SKIP_REASON = "E2E_BASE_URL not set — skipping Azure e2e tests"

pytestmark = pytest.mark.azure_e2e


def _get(path: str, **kwargs: Any) -> requests.Response:
    return requests.get(f"{BASE_URL}{path}", timeout=30, **kwargs)


def _post(path: str, **kwargs: Any) -> requests.Response:
    return requests.post(f"{BASE_URL}{path}", timeout=30, **kwargs)


@pytest.fixture(scope="session", autouse=True)
def warmup() -> None:
    """Wait for cold start to finish (max 3 min for Azure)."""
    if not BASE_URL:
        return
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/api/health", timeout=10)
            if r.status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(5)
    pytest.fail("Warmup failed: /api/health did not respond within 180 s")


@pytest.fixture(scope="session", autouse=True)
def setup_table() -> None:
    """Create the e2e_items table on the Azure-deployed app."""
    if not BASE_URL:
        return
    r = _post("/api/setup")
    assert r.status_code == 200, f"Setup failed: {r.text}"


@pytest.mark.skipif(not BASE_URL, reason=SKIP_REASON)
def test_health() -> None:
    r = _get("/api/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.skipif(not BASE_URL, reason=SKIP_REASON)
def test_crud_via_bindings() -> None:
    """Insert via output binding, read back via input binding."""
    # Insert
    payload = {"id": 9001, "name": "azure-e2e", "status": "deployed"}
    r = _post("/api/items", json=payload)
    assert r.status_code == 201

    # Read back by PK
    r = _get("/api/items/9001")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "azure-e2e"

    # List all
    r = _get("/api/items")
    assert r.status_code == 200
    items = r.json()
    assert isinstance(items, list)
    assert any(item["id"] == 9001 for item in items)


@pytest.mark.skipif(not BASE_URL, reason=SKIP_REASON)
def test_inject_reader_writer() -> None:
    """Verify inject_reader and inject_writer decorators work in Azure."""
    # Write
    payload = {"id": 9002, "name": "azure-inject", "status": "active"}
    r = _post("/api/writer", json=payload)
    assert r.status_code == 201

    # Read
    r = _get("/api/reader/9002")
    assert r.status_code == 200
    data = r.json()
    assert data["name"] == "azure-inject"


@pytest.mark.skipif(not BASE_URL, reason=SKIP_REASON)
def test_version_endpoint_reports_a_version_string() -> None:
    """Smoke-check the identity endpoint the e2e-azure workflow's deployed-
    version gate relies on (#442) -- the hard version-match assertion itself
    lives in the workflow step (tools/check_deployed_version.py), not here."""
    r = _get("/api/version")
    assert r.status_code == 200
    assert isinstance(r.json().get("version"), str)
    assert r.json()["version"]
