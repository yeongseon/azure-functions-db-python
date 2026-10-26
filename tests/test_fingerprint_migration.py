import json

from azure.core import MatchConditions
import pytest

from azure_functions_db import SqlAlchemySource
from azure_functions_db.state.errors import FingerprintMismatchError
from tests.test_state_blob import _make_store


def test_verified_host_migration_preserves_checkpoint_and_rollback() -> None:
    # Given stopped pollers with a released lease and a backed-up state blob.
    old, container = _make_store(fingerprint="old-host")
    lease = old.acquire_lease("orders", 30)
    old.commit_checkpoint("orders", {"cursor": [100, 2]}, lease)
    old.release_lease("orders", lease)
    blob = container.get_blob_client("state/orders.json")
    backup = blob.download_blob().readall()
    new, _ = _make_store(container, fingerprint="verified-new-host")
    with pytest.raises(FingerprintMismatchError):
        new.load_checkpoint("orders")
    # When an operator has independently verified dataset identity and changes only the fingerprint.
    state = json.loads(backup)
    state["source_fingerprint"] = "verified-new-host"
    blob.upload_blob(
        json.dumps(state),
        overwrite=True,
        etag=blob.etag,
        match_condition=MatchConditions.IfNotModified,
    )
    # Then the checkpoint resumes, while the old configuration is rejected.
    assert new.load_checkpoint("orders") == {"cursor": [100, 2]}
    with pytest.raises(FingerprintMismatchError):
        old.load_checkpoint("orders")
    blob.upload_blob(
        backup, overwrite=True, etag=blob.etag, match_condition=MatchConditions.IfNotModified
    )
    assert old.load_checkpoint("orders") == {"cursor": [100, 2]}
    with pytest.raises(FingerprintMismatchError):
        new.load_checkpoint("orders")


@pytest.mark.parametrize("change", ["password", "host", "username", "database", "query"])
def test_connection_identity_changes_have_documented_fingerprints(change: str) -> None:
    base = "postgresql://user:old@original/db"
    urls = {
        "password": "postgresql://user:new@original/db",
        "host": "postgresql://user:old@replacement/db",
        "username": "postgresql://other:old@original/db",
        "database": "postgresql://user:old@original/other",
        "query": "postgresql://user:old@original/db?application_name=other",
    }
    # Given otherwise identical source configurations, no DB connection is opened.
    original = SqlAlchemySource(url=base, table="orders", cursor_column="id", pk_columns=["id"])
    changed = SqlAlchemySource(
        url=urls[change], table="orders", cursor_column="id", pk_columns=["id"]
    )
    # When fingerprints are compared, then only the URL password is excluded.
    assert (original.source_descriptor.fingerprint == changed.source_descriptor.fingerprint) is (
        change == "password"
    )
