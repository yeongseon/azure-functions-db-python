from __future__ import annotations

import pytest

from azure_functions_db.state.errors import (
    FingerprintMismatchError,
    LeaseConflictError,
    StateStoreError,
)
from azure_functions_db.trigger.errors import PollerError


class TestStateStoreErrorHierarchy:
    def test_state_store_error_is_exception(self) -> None:
        assert issubclass(StateStoreError, Exception)

    @pytest.mark.parametrize(
        "error_cls",
        [LeaseConflictError, FingerprintMismatchError],
    )
    def test_inherits_state_store_error(self, error_cls: type[StateStoreError]) -> None:
        assert issubclass(error_cls, StateStoreError)

    def test_catch_state_store_error_catches_subtypes(self) -> None:
        with pytest.raises(StateStoreError):
            raise LeaseConflictError("conflict")

    def test_error_message(self) -> None:
        err = FingerprintMismatchError("source config changed")
        assert str(err) == "source config changed"

    def test_not_subclass_of_poller_error(self) -> None:
        assert not issubclass(StateStoreError, PollerError)
