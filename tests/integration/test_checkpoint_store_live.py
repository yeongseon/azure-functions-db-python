from __future__ import annotations

from collections.abc import Generator
from datetime import datetime, timezone
import os
from typing import Any
import uuid

import pytest

pytest.importorskip("azure.storage.blob")

from azure_functions_db.state.errors import LeaseConflictError

AZURITE_CONN_STR = os.environ.get("TEST_AZURITE_CONN_STR")

pytestmark = [pytest.mark.integration, pytest.mark.azurite]


@pytest.fixture
def checkpoint_store() -> Generator[Any, None, None]:
    if AZURITE_CONN_STR is None:
        pytest.skip("TEST_AZURITE_CONN_STR not set")

    pytest.importorskip("azure.storage.blob")
    from azure.storage.blob import ContainerClient

    container_name = f"test-{uuid.uuid4().hex[:8]}"
    container_client = ContainerClient.from_connection_string(
        conn_str=AZURITE_CONN_STR,
        container_name=container_name,
    )
    container_client.create_container()

    from azure_functions_db.state.blob import BlobCheckpointStore

    store = BlobCheckpointStore(
        container_client=container_client,
        source_fingerprint=f"test-fingerprint-{uuid.uuid4().hex[:8]}",
    )

    yield store

    try:
        container_client.delete_container()
    except Exception:
        pass


def test_save_and_load_checkpoint(checkpoint_store: Any) -> None:
    poller_name = "poller-save-load"
    lease_id = checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)
    expected = {"cursor": 42, "batch_id": "batch-1"}

    checkpoint_store.commit_checkpoint(poller_name, expected, lease_id)
    loaded = checkpoint_store.load_checkpoint(poller_name)

    assert loaded == expected


def test_load_nonexistent_checkpoint(checkpoint_store: Any) -> None:
    result = checkpoint_store.load_checkpoint("poller-missing")
    assert result == {}


def test_overwrite_checkpoint(checkpoint_store: Any) -> None:
    poller_name = "poller-overwrite"
    lease_id = checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)

    checkpoint_store.commit_checkpoint(poller_name, {"cursor": 1}, lease_id)
    checkpoint_store.commit_checkpoint(
        poller_name,
        {"cursor": 2, "status": "updated"},
        lease_id,
    )

    loaded = checkpoint_store.load_checkpoint(poller_name)
    assert loaded == {"cursor": 2, "status": "updated"}


def test_acquire_lease(checkpoint_store: Any) -> None:
    lease_id = checkpoint_store.acquire_lease("poller-acquire", ttl_seconds=30)
    assert lease_id
    assert ":" in lease_id


def test_release_lease(checkpoint_store: Any) -> None:
    poller_name = "poller-release"
    lease_id = checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)
    checkpoint_store.release_lease(poller_name, lease_id)


def test_lease_prevents_double_acquire(checkpoint_store: Any) -> None:
    poller_name = "poller-conflict"
    checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)

    with pytest.raises(LeaseConflictError):
        checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)


@pytest.mark.parametrize(
    "cursor_value",
    [
        123,
        "cursor-abc",
        datetime.now(timezone.utc).isoformat(),
    ],
)
def test_checkpoint_with_various_values(checkpoint_store: Any, cursor_value: object) -> None:
    poller_name = "poller-various-values"
    lease_id = checkpoint_store.acquire_lease(poller_name, ttl_seconds=30)
    checkpoint = {"cursor": cursor_value}

    checkpoint_store.commit_checkpoint(poller_name, checkpoint, lease_id)
    loaded = checkpoint_store.load_checkpoint(poller_name)

    assert loaded == checkpoint
