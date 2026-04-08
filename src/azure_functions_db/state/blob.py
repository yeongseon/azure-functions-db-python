from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import logging
from typing import Any
import uuid

from azure.core import MatchConditions
from azure.core.exceptions import (
    ClientAuthenticationError,
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
    ServiceRequestError,
)
from azure.storage.blob import ContainerClient

from azure_functions_db.state.errors import (
    FingerprintMismatchError,
    LeaseConflictError,
    StateStoreError,
)
from azure_functions_db.trigger.errors import LostLeaseError

logger = logging.getLogger(__name__)

_STATE_VERSION = 1


def _effective_grace(ttl_seconds: int) -> float:
    """Return the grace period before a lease can be stolen.

    Scales with TTL: ``min(ttl_seconds * 0.5, 5.0)``.
    """
    return min(ttl_seconds * 0.5, 5.0)


def _parse_lease_id(lease_id: str) -> tuple[str, int]:
    """Split ``owner_id:fencing_token`` into its parts."""
    parts = lease_id.rsplit(":", maxsplit=1)
    if len(parts) != 2:  # noqa: PLR2004
        msg = f"Invalid lease_id format: {lease_id!r}"
        raise ValueError(msg)
    owner_id, token_str = parts
    try:
        fencing_token = int(token_str)
    except ValueError:
        msg = f"Invalid fencing_token in lease_id: {lease_id!r}"
        raise ValueError(msg) from None
    return owner_id, fencing_token


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class BlobCheckpointStore:
    """StateStore implementation backed by Azure Blob Storage.

    Uses a single JSON blob per poller with ETag-based CAS (compare-and-swap)
    for all state mutations.  Lease ownership is verified via owner_id and
    monotonically-increasing fencing tokens.
    """

    def __init__(
        self,
        *,
        container_client: ContainerClient,
        source_fingerprint: str,
    ) -> None:
        self._container_client = container_client
        self._source_fingerprint = source_fingerprint

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _blob_path(poller_name: str) -> str:
        return f"state/{poller_name}.json"

    def _read_state(self, poller_name: str) -> tuple[dict[str, Any], str] | None:
        """Download the state blob and return ``(state, etag)``.

        Returns ``None`` when the blob does not exist.
        Raises ``FingerprintMismatchError`` when the stored fingerprint
        does not match the expected one.
        """
        blob_client = self._container_client.get_blob_client(self._blob_path(poller_name))
        try:
            downloader = blob_client.download_blob()
            raw = downloader.readall()
            etag: str = downloader.properties.etag
        except ResourceNotFoundError:
            return None
        except (ClientAuthenticationError, ServiceRequestError) as exc:
            raise StateStoreError(f"Failed to read state blob for poller '{poller_name}'") from exc
        except Exception as exc:
            raise StateStoreError(
                f"Unexpected error reading state blob for poller '{poller_name}'"
            ) from exc

        state: dict[str, Any] = json.loads(raw)

        stored_fp = state.get("source_fingerprint")
        if stored_fp is not None and stored_fp != self._source_fingerprint:
            msg = (
                f"Source fingerprint mismatch for poller '{poller_name}': "
                f"expected '{self._source_fingerprint}', found '{stored_fp}'"
            )
            raise FingerprintMismatchError(msg)

        return state, etag

    def _write_state_conditional(
        self,
        poller_name: str,
        state: dict[str, Any],
        etag: str,
    ) -> str:
        """CAS-write the state blob.  Returns the new etag on success."""
        blob_client = self._container_client.get_blob_client(self._blob_path(poller_name))
        data = json.dumps(state, indent=2).encode()
        try:
            resp = blob_client.upload_blob(
                data,
                overwrite=True,
                etag=etag,
                match_condition=MatchConditions.IfNotModified,
            )
        except (ClientAuthenticationError, ServiceRequestError) as exc:
            raise StateStoreError(f"Failed to write state blob for poller '{poller_name}'") from exc
        except HttpResponseError:
            raise  # let caller handle 412
        except Exception as exc:
            raise StateStoreError(
                f"Unexpected error writing state blob for poller '{poller_name}'"
            ) from exc
        new_etag: str = resp["etag"]
        return new_etag

    def _write_state_create(
        self,
        poller_name: str,
        state: dict[str, Any],
    ) -> str:
        """Create the state blob (fails if it already exists).

        Returns the new etag on success.
        """
        blob_client = self._container_client.get_blob_client(self._blob_path(poller_name))
        data = json.dumps(state, indent=2).encode()
        try:
            resp = blob_client.upload_blob(
                data,
                overwrite=False,
                match_condition=MatchConditions.IfMissing,
            )
        except ResourceExistsError:
            raise  # let caller handle
        except (ClientAuthenticationError, ServiceRequestError) as exc:
            raise StateStoreError(
                f"Failed to create state blob for poller '{poller_name}'"
            ) from exc
        except Exception as exc:
            raise StateStoreError(
                f"Unexpected error creating state blob for poller '{poller_name}'"
            ) from exc
        new_etag: str = resp["etag"]
        return new_etag

    @staticmethod
    def _verify_lease(
        state: dict[str, Any],
        owner_id: str,
        fencing_token: int,
    ) -> None:
        """Verify that the caller is the current lease holder.

        Raises ``LostLeaseError`` on any mismatch or if the lease has expired.
        """
        lease = state.get("lease")
        if lease is None:
            raise LostLeaseError("No lease present in state")

        if lease.get("owner_id") != owner_id:
            raise LostLeaseError(
                f"Lease owner mismatch: expected '{owner_id}', found '{lease.get('owner_id')}'"
            )

        if lease.get("fencing_token") != fencing_token:
            raise LostLeaseError(
                f"Fencing token mismatch: expected {fencing_token}, "
                f"found {lease.get('fencing_token')}"
            )

        expires_at_str = lease.get("expires_at")
        if expires_at_str is not None:
            expires_at = datetime.fromisoformat(expires_at_str)
            if _now_utc() > expires_at:
                raise LostLeaseError("Lease has expired")

    @staticmethod
    def _is_lease_expired(lease: dict[str, Any], grace_seconds: float) -> bool:
        """Return ``True`` if the lease is past ``expires_at + grace``."""
        expires_at_str = lease.get("expires_at")
        if expires_at_str is None:
            return True
        expires_at = datetime.fromisoformat(expires_at_str)
        return _now_utc() > expires_at + timedelta(seconds=grace_seconds)

    @staticmethod
    def _make_initial_state(
        poller_name: str,
        source_fingerprint: str,
        owner_id: str,
        ttl_seconds: int,
    ) -> dict[str, Any]:
        now = _now_utc()
        return {
            "version": _STATE_VERSION,
            "poller_name": poller_name,
            "source_fingerprint": source_fingerprint,
            "checkpoint": {},
            "lease": {
                "owner_id": owner_id,
                "fencing_token": 1,  # nosec B105
                "acquired_at": _iso(now),
                "heartbeat_at": _iso(now),
                "expires_at": _iso(now + timedelta(seconds=ttl_seconds)),
            },
        }

    # ------------------------------------------------------------------
    # StateStore Protocol methods
    # ------------------------------------------------------------------

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        """Acquire a lease on the poller's state blob.

        Creates the blob if it does not exist.  If the blob exists and the
        lease has expired (past ``expires_at + grace``), the lease is stolen
        with an incremented fencing token.

        Returns a lease_id string in the format ``{owner_id}:{fencing_token}``.

        Raises ``LeaseConflictError`` when a lease is already active or
        another instance won the CAS race.
        """
        owner_id = uuid.uuid4().hex
        grace = _effective_grace(ttl_seconds)
        result = self._read_state(poller_name)

        if result is None:
            # Blob doesn't exist — create with fencing_token=1
            state = self._make_initial_state(
                poller_name, self._source_fingerprint, owner_id, ttl_seconds
            )
            try:
                self._write_state_create(poller_name, state)
            except ResourceExistsError:
                raise LeaseConflictError(
                    f"Another instance created the state blob for poller '{poller_name}' first"
                ) from None
            lease_id = f"{owner_id}:1"
            logger.info(
                "Poller '%s': lease acquired (new blob), lease_id=%s",
                poller_name,
                lease_id,
            )
            return lease_id

        state, etag = result
        lease = state.get("lease", {})

        if not self._is_lease_expired(lease, grace):
            raise LeaseConflictError(
                f"Active lease on poller '{poller_name}' held by '{lease.get('owner_id')}'"
            )

        # Lease expired — steal it with incremented fencing token
        old_token = lease.get("fencing_token", 0)
        new_token = old_token + 1
        now = _now_utc()
        state["lease"] = {
            "owner_id": owner_id,
            "fencing_token": new_token,
            "acquired_at": _iso(now),
            "heartbeat_at": _iso(now),
            "expires_at": _iso(now + timedelta(seconds=ttl_seconds)),
        }

        try:
            self._write_state_conditional(poller_name, state, etag)
        except HttpResponseError as exc:
            if exc.status_code == 412:  # noqa: PLR2004
                raise LeaseConflictError(
                    f"CAS conflict acquiring lease for poller '{poller_name}'"
                ) from exc
            raise StateStoreError(f"Failed to acquire lease for poller '{poller_name}'") from exc

        lease_id = f"{owner_id}:{new_token}"
        logger.info(
            "Poller '%s': lease acquired (expired steal), lease_id=%s, fencing_token=%d",
            poller_name,
            lease_id,
            new_token,
        )
        return lease_id

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        """Renew an existing lease by extending its expiry.

        Raises ``LostLeaseError`` if the lease is not held by this caller
        or the CAS write fails.
        """
        owner_id, fencing_token = _parse_lease_id(lease_id)
        result = self._read_state(poller_name)

        if result is None:
            raise LostLeaseError(f"State blob not found for poller '{poller_name}'")

        state, etag = result
        self._verify_lease(state, owner_id, fencing_token)

        now = _now_utc()
        state["lease"]["heartbeat_at"] = _iso(now)
        state["lease"]["expires_at"] = _iso(now + timedelta(seconds=ttl_seconds))

        try:
            self._write_state_conditional(poller_name, state, etag)
        except HttpResponseError as exc:
            if exc.status_code == 412:  # noqa: PLR2004
                raise LostLeaseError(
                    f"CAS conflict renewing lease for poller '{poller_name}'"
                ) from exc
            raise StateStoreError(f"Failed to renew lease for poller '{poller_name}'") from exc

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        """Release the lease by setting ``expires_at`` to now.

        Preserves the fencing token so the next acquisition increments it.
        Only owner_id and fencing_token are verified — expiry is intentionally
        skipped so that a holder can still release a lease that has nominally
        expired but has not yet been stolen.

        Raises ``LostLeaseError`` if owner/token do not match or CAS fails.
        """
        owner_id, fencing_token = _parse_lease_id(lease_id)
        result = self._read_state(poller_name)

        if result is None:
            raise LostLeaseError(f"State blob not found for poller '{poller_name}'")

        state, etag = result

        lease = state.get("lease")
        if lease is None:
            raise LostLeaseError("No lease present in state")
        if lease.get("owner_id") != owner_id:
            raise LostLeaseError(
                f"Lease owner mismatch: expected '{owner_id}', found '{lease.get('owner_id')}'"
            )
        if lease.get("fencing_token") != fencing_token:
            raise LostLeaseError(
                f"Fencing token mismatch: expected {fencing_token}, "
                f"found {lease.get('fencing_token')}"
            )

        state["lease"]["expires_at"] = _iso(_now_utc())

        try:
            self._write_state_conditional(poller_name, state, etag)
        except HttpResponseError as exc:
            if exc.status_code == 412:  # noqa: PLR2004
                raise LostLeaseError(
                    f"CAS conflict releasing lease for poller '{poller_name}'"
                ) from exc
            raise StateStoreError(f"Failed to release lease for poller '{poller_name}'") from exc

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        """Load the checkpoint for the given poller.

        Returns an empty dict if the state blob does not exist.
        This method is read-only and has no side effects.
        """
        result = self._read_state(poller_name)
        if result is None:
            return {}
        state, _etag = result
        checkpoint: dict[str, object] = state.get("checkpoint", {})
        return checkpoint

    def commit_checkpoint(
        self,
        poller_name: str,
        checkpoint: dict[str, object],
        lease_id: str,
    ) -> None:
        """Commit a new checkpoint under the protection of the current lease.

        Raises ``LostLeaseError`` if the lease is not held by this caller
        or the CAS write fails.
        """
        owner_id, fencing_token = _parse_lease_id(lease_id)
        result = self._read_state(poller_name)

        if result is None:
            raise LostLeaseError(f"State blob not found for poller '{poller_name}'")

        state, etag = result
        self._verify_lease(state, owner_id, fencing_token)

        state["checkpoint"] = dict(checkpoint)

        try:
            self._write_state_conditional(poller_name, state, etag)
        except HttpResponseError as exc:
            if exc.status_code == 412:  # noqa: PLR2004
                raise LostLeaseError(
                    f"CAS conflict committing checkpoint for poller '{poller_name}'"
                ) from exc
            raise StateStoreError(
                f"Failed to commit checkpoint for poller '{poller_name}'"
            ) from exc
