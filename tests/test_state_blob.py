from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from azure_functions_db.state.blob import BlobCheckpointStore, _effective_grace
from azure_functions_db.state.errors import (
    FingerprintMismatchError,
    LeaseConflictError,
    StateStoreError,
)
from azure_functions_db.trigger.errors import LostLeaseError
from azure_functions_db.trigger.runner import StateStore

# ---------------------------------------------------------------------------
# Faked Azure SDK classes
# ---------------------------------------------------------------------------


class _FakeDownloader:
    def __init__(self, content: bytes, etag: str) -> None:
        self._content = content
        self.properties = MagicMock()
        self.properties.etag = etag

    def readall(self) -> bytes:
        return self._content


class _FakeBlobClient:
    def __init__(self) -> None:
        self.content: bytes | None = None
        self.etag: str | None = None
        self._etag_counter: int = 0
        self.download_error: Exception | None = None
        self.upload_error: Exception | None = None
        self._mutate_after_download: bool = False

    def download_blob(self, **kwargs: Any) -> _FakeDownloader:
        if self.download_error is not None:
            raise self.download_error
        if self.content is None:
            from azure.core.exceptions import ResourceNotFoundError

            raise ResourceNotFoundError("Blob not found")
        downloader = _FakeDownloader(self.content, self.etag or "")
        if self._mutate_after_download:
            # Simulate another writer modifying the blob between read and write
            self._etag_counter += 1
            self.etag = f"etag-{self._etag_counter}"
            self._mutate_after_download = False
        return downloader

    def upload_blob(
        self,
        data: bytes | str,
        *,
        overwrite: bool = False,
        etag: str | None = None,
        match_condition: Any = None,
    ) -> dict[str, str]:
        from azure.core import MatchConditions
        from azure.core.exceptions import (
            HttpResponseError,
            ResourceExistsError,
        )

        if self.upload_error is not None:
            raise self.upload_error

        data_bytes = data if isinstance(data, bytes) else data.encode()

        if match_condition == MatchConditions.IfMissing:
            if self.content is not None:
                raise ResourceExistsError("Blob already exists")
        elif match_condition == MatchConditions.IfNotModified:
            if self.etag != etag:
                resp = MagicMock()
                resp.status_code = 412
                resp.reason = "Precondition Failed"
                raise HttpResponseError(message="Precondition Failed", response=resp)

        self._etag_counter += 1
        self.etag = f"etag-{self._etag_counter}"
        self.content = data_bytes
        return {"etag": self.etag}


class _FakeContainerClient:
    def __init__(self) -> None:
        self._blobs: dict[str, _FakeBlobClient] = {}

    def get_blob_client(self, blob: str) -> _FakeBlobClient:
        if blob not in self._blobs:
            self._blobs[blob] = _FakeBlobClient()
        return self._blobs[blob]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_store(
    container: _FakeContainerClient | None = None,
    fingerprint: str = "fp_test",
) -> tuple[BlobCheckpointStore, _FakeContainerClient]:
    container = container or _FakeContainerClient()
    store = BlobCheckpointStore(
        container_client=container,  # type: ignore[arg-type]
        source_fingerprint=fingerprint,
    )
    return store, container


def _seed_blob(
    container: _FakeContainerClient,
    poller_name: str,
    state: dict[str, Any],
) -> None:
    blob = container.get_blob_client(f"state/{poller_name}.json")
    blob.content = json.dumps(state).encode()
    blob._etag_counter += 1
    blob.etag = f"etag-{blob._etag_counter}"


def _read_blob_state(container: _FakeContainerClient, poller_name: str) -> dict[str, Any]:
    blob = container.get_blob_client(f"state/{poller_name}.json")
    assert blob.content is not None
    result: dict[str, Any] = json.loads(blob.content)
    return result


def _make_state(
    *,
    poller_name: str = "test_poller",
    fingerprint: str = "fp_test",
    fencing_token: int = 1,
    owner_id: str = "old-owner",
    expires_at: datetime | None = None,
    checkpoint: dict[str, object] | None = None,
) -> dict[str, Any]:
    if expires_at is None:
        expires_at = datetime.now(UTC) + timedelta(hours=1)
    now_str = datetime.now(UTC).isoformat()
    return {
        "version": 1,
        "poller_name": poller_name,
        "source_fingerprint": fingerprint,
        "checkpoint": checkpoint or {},
        "lease": {
            "owner_id": owner_id,
            "fencing_token": fencing_token,
            "acquired_at": now_str,
            "heartbeat_at": now_str,
            "expires_at": expires_at.isoformat(),
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBlobCheckpointStoreAcquireLease:
    def test_acquire_creates_blob_when_not_found(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        assert ":" in lease_id
        owner_id, token = lease_id.rsplit(":", 1)
        assert len(owner_id) == 32
        assert token == "1"

        state = _read_blob_state(container, "test_poller")
        assert state["lease"]["fencing_token"] == 1
        assert state["source_fingerprint"] == "fp_test"

    def test_acquire_succeeds_when_lease_expired(self) -> None:
        store, container = _make_store()
        expired_at = datetime.now(UTC) - timedelta(minutes=10)
        _seed_blob(
            container,
            "test_poller",
            _make_state(fencing_token=3, expires_at=expired_at),
        )

        lease_id = store.acquire_lease("test_poller", 120)
        _owner_id, token = lease_id.rsplit(":", 1)
        assert token == "4"

    def test_acquire_raises_conflict_when_lease_active(self) -> None:
        store, container = _make_store()
        active_at = datetime.now(UTC) + timedelta(hours=1)
        _seed_blob(
            container,
            "test_poller",
            _make_state(expires_at=active_at),
        )

        with pytest.raises(LeaseConflictError, match="Active lease"):
            store.acquire_lease("test_poller", 120)

    def test_acquire_raises_conflict_on_cas_race(self) -> None:
        store, container = _make_store()

        # First acquire creates the blob
        store.acquire_lease("test_poller", 120)

        # Expire the lease so acquire tries to steal
        state = _read_blob_state(container, "test_poller")
        state["lease"]["expires_at"] = (
            datetime.now(UTC) - timedelta(minutes=10)
        ).isoformat()
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        # Simulate another writer between our read and write
        blob._mutate_after_download = True

        with pytest.raises(LeaseConflictError, match="CAS conflict"):
            store.acquire_lease("test_poller", 120)

        # Expire the lease and simulate another CAS race
        state = _read_blob_state(container, "test_poller")
        state["lease"]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()
        blob._mutate_after_download = True

        with pytest.raises(LeaseConflictError, match="CAS conflict"):
            store.acquire_lease("test_poller", 120)

    def test_acquire_increments_fencing_token(self) -> None:
        store, container = _make_store()
        expired_at = datetime.now(UTC) - timedelta(minutes=10)
        _seed_blob(
            container,
            "test_poller",
            _make_state(fencing_token=5, expires_at=expired_at),
        )

        lease_id = store.acquire_lease("test_poller", 120)
        state = _read_blob_state(container, "test_poller")
        assert state["lease"]["fencing_token"] == 6
        assert lease_id.endswith(":6")

    def test_acquire_uses_grace_period(self) -> None:
        store, container = _make_store()
        # TTL=120, grace=5s. Lease expired 3s ago → still within grace.
        expired_at = datetime.now(UTC) - timedelta(seconds=3)
        _seed_blob(
            container,
            "test_poller",
            _make_state(expires_at=expired_at),
        )

        with pytest.raises(LeaseConflictError, match="Active lease"):
            store.acquire_lease("test_poller", 120)

    def test_acquire_grace_scales_for_short_ttl(self) -> None:
        store, container = _make_store()
        # TTL=4, grace=min(4*0.5, 5)=2s. Lease expired 3s ago → past grace.
        expired_at = datetime.now(UTC) - timedelta(seconds=3)
        _seed_blob(
            container,
            "test_poller",
            _make_state(expires_at=expired_at),
        )

        lease_id = store.acquire_lease("test_poller", 4)
        assert ":" in lease_id


class TestBlobCheckpointStoreRenewLease:
    def test_renew_extends_expiry(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        store.renew_lease("test_poller", lease_id, 120)

        state = _read_blob_state(container, "test_poller")
        expires_at = datetime.fromisoformat(state["lease"]["expires_at"])
        assert expires_at > datetime.now(UTC) + timedelta(seconds=60)

    def test_renew_raises_lost_lease_on_wrong_owner(self) -> None:
        store, container = _make_store()
        store.acquire_lease("test_poller", 120)

        state = _read_blob_state(container, "test_poller")
        real_token = state["lease"]["fencing_token"]
        fake_lease_id = f"wrong-owner:{real_token}"

        with pytest.raises(LostLeaseError, match="owner mismatch"):
            store.renew_lease("test_poller", fake_lease_id, 120)

    def test_renew_raises_lost_lease_on_wrong_token(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        owner_id = lease_id.rsplit(":", 1)[0]
        wrong_lease_id = f"{owner_id}:999"

        with pytest.raises(LostLeaseError, match="token mismatch"):
            store.renew_lease("test_poller", wrong_lease_id, 120)

    def test_renew_raises_lost_lease_on_expired(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        # Manually expire the lease in the blob
        state = _read_blob_state(container, "test_poller")
        state["lease"]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        with pytest.raises(LostLeaseError, match="expired"):
            store.renew_lease("test_poller", lease_id, 120)

    def test_renew_raises_lost_lease_on_412(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        # Simulate another writer between our read and write
        blob = container.get_blob_client("state/test_poller.json")
        blob._mutate_after_download = True

        with pytest.raises(LostLeaseError, match="CAS conflict"):
            store.renew_lease("test_poller", lease_id, 120)


class TestBlobCheckpointStoreReleaseLease:
    def test_release_sets_expires_to_now(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        store.release_lease("test_poller", lease_id)

        state = _read_blob_state(container, "test_poller")
        expires_at = datetime.fromisoformat(state["lease"]["expires_at"])
        # Should be approximately now (within 2 seconds tolerance)
        assert abs((expires_at - datetime.now(UTC)).total_seconds()) < 2

    def test_release_preserves_fencing_token(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        store.release_lease("test_poller", lease_id)

        state = _read_blob_state(container, "test_poller")
        assert state["lease"]["fencing_token"] == 1

    def test_release_raises_lost_lease_on_wrong_owner(self) -> None:
        store, container = _make_store()
        store.acquire_lease("test_poller", 120)

        with pytest.raises(LostLeaseError, match="owner mismatch"):
            store.release_lease("test_poller", "wrong-owner:1")

    def test_release_succeeds_when_lease_expired(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        # Manually expire the lease — release should still work
        state = _read_blob_state(container, "test_poller")
        state["lease"]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        store.release_lease("test_poller", lease_id)

    def test_release_raises_lost_lease_on_412(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        blob._mutate_after_download = True

        with pytest.raises(LostLeaseError, match="CAS conflict"):
            store.release_lease("test_poller", lease_id)


class TestBlobCheckpointStoreLoadCheckpoint:
    def test_load_returns_empty_dict_when_blob_not_found(self) -> None:
        store, _container = _make_store()
        result = store.load_checkpoint("test_poller")
        assert result == {}

    def test_load_returns_checkpoint_data(self) -> None:
        store, container = _make_store()
        checkpoint = {"cursor": 42, "batch_id": "batch-1"}
        _seed_blob(
            container,
            "test_poller",
            _make_state(checkpoint=checkpoint),
        )

        result = store.load_checkpoint("test_poller")
        assert result == checkpoint

    def test_load_is_side_effect_free(self) -> None:
        store, container = _make_store()
        _seed_blob(
            container,
            "test_poller",
            _make_state(checkpoint={"cursor": 100}),
        )

        blob = container.get_blob_client("state/test_poller.json")
        content_before = blob.content

        store.load_checkpoint("test_poller")
        store.load_checkpoint("test_poller")

        assert blob.content == content_before


class TestBlobCheckpointStoreCommitCheckpoint:
    def test_commit_updates_checkpoint(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        new_checkpoint: dict[str, object] = {"cursor": 100, "batch_id": "b1"}
        store.commit_checkpoint("test_poller", new_checkpoint, lease_id)

        result = store.load_checkpoint("test_poller")
        assert result["cursor"] == 100
        assert result["batch_id"] == "b1"

    def test_commit_raises_lost_lease_on_wrong_owner(self) -> None:
        store, container = _make_store()
        store.acquire_lease("test_poller", 120)

        with pytest.raises(LostLeaseError, match="owner mismatch"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, "wrong-owner:1")

    def test_commit_raises_lost_lease_on_expired(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        state = _read_blob_state(container, "test_poller")
        state["lease"]["expires_at"] = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        with pytest.raises(LostLeaseError, match="expired"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)

    def test_commit_raises_lost_lease_on_412(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        # Simulate another writer between our read and write
        blob = container.get_blob_client("state/test_poller.json")
        blob._mutate_after_download = True

        with pytest.raises(LostLeaseError, match="CAS conflict"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)


class TestBlobCheckpointStoreFingerprint:
    def test_fingerprint_match_succeeds(self) -> None:
        store, container = _make_store(fingerprint="fp_test")
        _seed_blob(
            container,
            "test_poller",
            _make_state(fingerprint="fp_test"),
        )

        result = store.load_checkpoint("test_poller")
        assert isinstance(result, dict)

    def test_fingerprint_mismatch_raises(self) -> None:
        store, container = _make_store(fingerprint="fp_new")
        _seed_blob(
            container,
            "test_poller",
            _make_state(fingerprint="fp_old"),
        )

        with pytest.raises(FingerprintMismatchError, match="fingerprint mismatch"):
            store.load_checkpoint("test_poller")


class TestBlobCheckpointStoreProtocolConformance:
    def test_implements_state_store_protocol(self) -> None:
        store, _container = _make_store()
        assert isinstance(store, StateStore)


class TestBlobCheckpointStoreErrorMapping:
    def test_auth_error_maps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ClientAuthenticationError

        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.download_error = ClientAuthenticationError("auth failed")

        with pytest.raises(StateStoreError, match="Failed to read"):
            store.load_checkpoint("test_poller")

    def test_transport_error_maps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ServiceRequestError

        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.download_error = ServiceRequestError("network error")

        with pytest.raises(StateStoreError, match="Failed to read"):
            store.load_checkpoint("test_poller")


class TestEffectiveGrace:
    def test_short_ttl(self) -> None:
        assert _effective_grace(4) == 2.0

    def test_long_ttl(self) -> None:
        assert _effective_grace(120) == 5.0

    def test_boundary_ttl(self) -> None:
        assert _effective_grace(10) == 5.0


class TestParseLeaseId:
    def test_missing_colon_raises_value_error(self) -> None:
        from azure_functions_db.state.blob import _parse_lease_id

        with pytest.raises(ValueError, match="Invalid lease_id format"):
            _parse_lease_id("no-colon-here")

    def test_non_integer_token_raises_value_error(self) -> None:
        from azure_functions_db.state.blob import _parse_lease_id

        with pytest.raises(ValueError, match="Invalid fencing_token"):
            _parse_lease_id("owner:abc")

    def test_valid_lease_id(self) -> None:
        from azure_functions_db.state.blob import _parse_lease_id

        owner, token = _parse_lease_id("my-owner:42")
        assert owner == "my-owner"
        assert token == 42


class TestMalformedLeaseIdInMethods:
    def test_renew_with_malformed_lease_id(self) -> None:
        store, _container = _make_store()
        with pytest.raises(ValueError, match="Invalid lease_id format"):
            store.renew_lease("test_poller", "bad-lease-id", 120)

    def test_release_with_malformed_lease_id(self) -> None:
        store, _container = _make_store()
        with pytest.raises(ValueError, match="Invalid lease_id format"):
            store.release_lease("test_poller", "bad-lease-id")

    def test_commit_with_malformed_lease_id(self) -> None:
        store, _container = _make_store()
        with pytest.raises(ValueError, match="Invalid lease_id format"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, "bad-lease-id")


class TestCorruptState:
    def test_unexpected_download_error_wraps_to_state_store_error(self) -> None:
        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.download_error = RuntimeError("unexpected SDK error")

        with pytest.raises(StateStoreError, match="Unexpected error reading"):
            store.load_checkpoint("test_poller")

    def test_auth_error_on_write_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ClientAuthenticationError

        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = ClientAuthenticationError("auth failed on write")

        with pytest.raises(StateStoreError, match="Failed to write"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)

    def test_transport_error_on_write_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ServiceRequestError

        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = ServiceRequestError("network error on write")

        with pytest.raises(StateStoreError, match="Failed to write"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)

    def test_unexpected_upload_error_wraps_to_state_store_error(self) -> None:
        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = RuntimeError("unexpected upload error")

        with pytest.raises(StateStoreError, match="Unexpected error writing"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)

    def test_auth_error_on_create_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ClientAuthenticationError

        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = ClientAuthenticationError("auth failed on create")

        with pytest.raises(StateStoreError, match="Failed to create"):
            store.acquire_lease("test_poller", 120)

    def test_transport_error_on_create_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import ServiceRequestError

        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = ServiceRequestError("network error on create")

        with pytest.raises(StateStoreError, match="Failed to create"):
            store.acquire_lease("test_poller", 120)

    def test_unexpected_error_on_create_wraps_to_state_store_error(self) -> None:
        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = RuntimeError("unexpected create error")

        with pytest.raises(StateStoreError, match="Unexpected error creating"):
            store.acquire_lease("test_poller", 120)


class TestVerifyLeaseEdgeCases:
    def test_verify_raises_when_no_lease_in_state(self) -> None:
        store, container = _make_store()
        _seed_blob(container, "test_poller", _make_state())
        state = _read_blob_state(container, "test_poller")
        del state["lease"]
        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        with pytest.raises(LostLeaseError, match="No lease present"):
            store.renew_lease("test_poller", "owner:1", 120)


class TestMissingSourceFingerprint:
    def test_load_succeeds_when_stored_fingerprint_is_none(self) -> None:
        store, container = _make_store(fingerprint="fp_test")
        state = _make_state(fingerprint="fp_test")
        del state["source_fingerprint"]
        _seed_blob(container, "test_poller", state)

        blob = container.get_blob_client("state/test_poller.json")
        blob.content = json.dumps(state).encode()

        result = store.load_checkpoint("test_poller")
        assert isinstance(result, dict)


class TestStateStoreErrorFallbackPaths:
    def test_acquire_non_412_http_error_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        store, container = _make_store()
        expired_at = datetime.now(UTC) - timedelta(minutes=10)
        _seed_blob(container, "test_poller", _make_state(expires_at=expired_at))

        blob = container.get_blob_client("state/test_poller.json")
        resp = MagicMock()
        resp.status_code = 500
        resp.reason = "Internal Server Error"
        blob.upload_error = HttpResponseError(
            message="Internal Server Error", response=resp
        )

        with pytest.raises(StateStoreError, match="Failed to acquire"):
            store.acquire_lease("test_poller", 120)

    def test_renew_non_412_http_error_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        resp = MagicMock()
        resp.status_code = 500
        resp.reason = "Internal Server Error"
        blob.upload_error = HttpResponseError(
            message="Internal Server Error", response=resp
        )

        with pytest.raises(StateStoreError, match="Failed to renew"):
            store.renew_lease("test_poller", lease_id, 120)

    def test_release_non_412_http_error_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        resp = MagicMock()
        resp.status_code = 500
        resp.reason = "Internal Server Error"
        blob.upload_error = HttpResponseError(
            message="Internal Server Error", response=resp
        )

        with pytest.raises(StateStoreError, match="Failed to release"):
            store.release_lease("test_poller", lease_id)

    def test_commit_non_412_http_error_wraps_to_state_store_error(self) -> None:
        from azure.core.exceptions import HttpResponseError

        store, container = _make_store()
        lease_id = store.acquire_lease("test_poller", 120)

        blob = container.get_blob_client("state/test_poller.json")
        resp = MagicMock()
        resp.status_code = 500
        resp.reason = "Internal Server Error"
        blob.upload_error = HttpResponseError(
            message="Internal Server Error", response=resp
        )

        with pytest.raises(StateStoreError, match="Failed to commit"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, lease_id)

    def test_renew_on_missing_blob_raises_lost_lease(self) -> None:
        store, _container = _make_store()
        with pytest.raises(LostLeaseError, match="State blob not found"):
            store.renew_lease("test_poller", "owner:1", 120)

    def test_release_on_missing_blob_raises_lost_lease(self) -> None:
        store, _container = _make_store()
        with pytest.raises(LostLeaseError, match="State blob not found"):
            store.release_lease("test_poller", "owner:1")

    def test_commit_on_missing_blob_raises_lost_lease(self) -> None:
        store, _container = _make_store()
        with pytest.raises(LostLeaseError, match="State blob not found"):
            store.commit_checkpoint("test_poller", {"cursor": 1}, "owner:1")


class TestAcquireCreateRace:
    def test_acquire_create_conflict_raises_lease_conflict(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        store, container = _make_store()
        blob = container.get_blob_client("state/test_poller.json")
        blob.upload_error = ResourceExistsError("Already exists")

        with pytest.raises(LeaseConflictError, match="Another instance"):
            store.acquire_lease("test_poller", 120)
