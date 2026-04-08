from __future__ import annotations

from azure_functions_db import __all__, __version__


def test_version() -> None:
    assert __version__ == "0.1.0"


def test_public_api_exports() -> None:
    expected = {
        "__version__",
        "BlobCheckpointStore",
        "CommitError",
        "ConfigurationError",
        "CursorPart",
        "CursorSerializationError",
        "CursorValue",
        "DbBindings",
        "DbConnectionError",
        "DbError",
        "DbReader",
        "DbWriter",
        "EngineProvider",
        "FetchError",
        "HandlerError",
        "LeaseAcquireError",
        "LostLeaseError",
        "MetricsCollector",
        "NotFoundError",
        "OutputResult",
        "PollContext",
        "PollTrigger",
        "QueryError",
        "RetryPolicy",
        "RowChange",
        "SqlAlchemySource",
        "WriteError",
    }
    assert set(__all__) == expected


def test_imports_resolve() -> None:
    from azure_functions_db import (
        BlobCheckpointStore,
        CommitError,
        ConfigurationError,
        CursorPart,
        CursorSerializationError,
        CursorValue,
        DbBindings,
        DbConnectionError,
        DbError,
        DbReader,
        DbWriter,
        EngineProvider,
        FetchError,
        HandlerError,
        LeaseAcquireError,
        LostLeaseError,
        MetricsCollector,
        NotFoundError,
        OutputResult,
        PollContext,
        PollTrigger,
        QueryError,
        RetryPolicy,
        RowChange,
        SqlAlchemySource,
        WriteError,
    )

    assert BlobCheckpointStore is not None
    assert CommitError is not None
    assert ConfigurationError is not None
    assert CursorPart is not None
    assert CursorSerializationError is not None
    assert CursorValue is not None
    assert DbBindings is not None
    assert DbConnectionError is not None
    assert DbError is not None
    assert DbReader is not None
    assert DbWriter is not None
    assert EngineProvider is not None
    assert FetchError is not None
    assert HandlerError is not None
    assert LeaseAcquireError is not None
    assert LostLeaseError is not None
    assert MetricsCollector is not None
    assert NotFoundError is not None
    assert OutputResult is not None
    assert PollContext is not None
    assert PollTrigger is not None
    assert QueryError is not None
    assert RetryPolicy is not None
    assert RowChange is not None
    assert SqlAlchemySource is not None
    assert WriteError is not None
