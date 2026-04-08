from azure_functions_db import __all__, __version__


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_public_api_exports() -> None:
    expected = {
        "__version__",
        "CommitError",
        "CursorPart",
        "CursorValue",
        "DbConnectionError",
        "DbError",
        "EventNormalizer",
        "FetchError",
        "HandlerError",
        "JsonScalar",
        "JsonValue",
        "LeaseAcquireError",
        "LostLeaseError",
        "NotFoundError",
        "PollContext",
        "PollRunner",
        "PollerError",
        "QueryError",
        "RawRecord",
        "RetryPolicy",
        "RowChange",
        "SerializationError",
        "SourceAdapter",
        "SourceConfigurationError",
        "SourceDescriptor",
        "StateStore",
        "WriteError",
    }
    assert set(__all__) == expected


def test_imports_resolve() -> None:
    from azure_functions_db import (
        PollContext,
        PollRunner,
        RetryPolicy,
        RowChange,
        SourceDescriptor,
    )

    assert PollContext is not None
    assert PollRunner is not None
    assert RetryPolicy is not None
    assert RowChange is not None
    assert SourceDescriptor is not None
