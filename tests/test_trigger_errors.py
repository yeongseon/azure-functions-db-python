import pytest

from azure_functions_db.trigger.errors import (
    CommitError,
    FetchError,
    HandlerError,
    LeaseAcquireError,
    LostLeaseError,
    PollerError,
    SerializationError,
    SourceConfigurationError,
)


class TestPollerErrorHierarchy:
    @pytest.mark.parametrize(
        "error_cls",
        [
            LeaseAcquireError,
            SourceConfigurationError,
            FetchError,
            HandlerError,
            CommitError,
            LostLeaseError,
            SerializationError,
        ],
    )
    def test_inherits_poller_error(self, error_cls: type[PollerError]) -> None:
        assert issubclass(error_cls, PollerError)

    def test_poller_error_is_exception(self) -> None:
        assert issubclass(PollerError, Exception)

    def test_catch_poller_error_catches_subtypes(self) -> None:
        with pytest.raises(PollerError):
            raise FetchError("fetch failed")

    def test_error_message(self) -> None:
        err = LostLeaseError("lease expired")
        assert str(err) == "lease expired"
