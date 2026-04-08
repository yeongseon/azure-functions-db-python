from azure_functions_db import __all__, __version__


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_public_api_exports() -> None:
    expected = {
        "__version__",
        "BlobCheckpointStore",
        "CommitError",
        "CursorPart",
        "CursorValue",
        "DbConnectionError",
        "DbError",
        "EventNormalizer",
        "FetchError",
        "FingerprintMismatchError",
        "HandlerError",
        "JsonScalar",
        "JsonValue",
        "LeaseAcquireError",
        "LeaseConflictError",
        "LostLeaseError",
        "NotFoundError",
        "PollContext",
        "PollTrigger",
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
        "SqlAlchemySource",
        "StateStore",
        "StateStoreError",
        "WriteError",
        "db",
        "default_normalizer",
        "make_normalizer",
    }
    assert set(__all__) == expected


def test_imports_resolve() -> None:
    from azure_functions_db import (
        BlobCheckpointStore,
        PollContext,
        PollRunner,
        PollTrigger,
        RetryPolicy,
        RowChange,
        SourceDescriptor,
        SqlAlchemySource,
        StateStoreError,
        db,
        default_normalizer,
        make_normalizer,
    )

    assert BlobCheckpointStore is not None
    assert PollContext is not None
    assert PollTrigger is not None
    assert PollRunner is not None
    assert RetryPolicy is not None
    assert RowChange is not None
    assert SourceDescriptor is not None
    assert SqlAlchemySource is not None
    assert StateStoreError is not None
    assert db is not None
    assert default_normalizer is not None
    assert make_normalizer is not None
