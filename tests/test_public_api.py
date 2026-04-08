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
        "DbConfig",
        "DbConnectionError",
        "DbError",
        "DbReader",
        "DbWriter",
        "EngineProvider",
        "EventNormalizer",
        "FetchError",
        "FingerprintMismatchError",
        "HandlerError",
        "JsonScalar",
        "JsonValue",
        "LeaseAcquireError",
        "LeaseConflictError",
        "LostLeaseError",
        "METRIC_BATCHES_TOTAL",
        "METRIC_BATCH_SIZE",
        "METRIC_COMMIT_DURATION_MS",
        "METRIC_EVENTS_TOTAL",
        "METRIC_FAILURES_TOTAL",
        "METRIC_FETCH_DURATION_MS",
        "METRIC_HANDLER_DURATION_MS",
        "METRIC_LAG_SECONDS",
        "METRIC_LAST_SUCCESS_TIMESTAMP",
        "MetricsCollector",
        "NoOpCollector",
        "NotFoundError",
        "parse_checkpoint_cursor",
        "PollContext",
        "PollTrigger",
        "PollRunner",
        "PollerError",
        "QueryError",
        "RawRecord",
        "RetryPolicy",
        "resolve_env_vars",
        "Row",
        "RowDict",
        "RowChange",
        "serialize_cursor_part",
        "SerializationError",
        "SourceAdapter",
        "SourceConfigurationError",
        "SourceDescriptor",
        "SqlAlchemySource",
        "StateStore",
        "StateStoreError",
        "WriteError",
        "build_log_fields",
        "DbFunctionApp",
        "default_normalizer",
        "make_normalizer",
    }
    assert set(__all__) == expected


def test_imports_resolve() -> None:
    from azure_functions_db import (
        METRIC_BATCHES_TOTAL,
        BlobCheckpointStore,
        ConfigurationError,
        CursorSerializationError,
        DbConfig,
        DbFunctionApp,
        DbReader,
        DbWriter,
        EngineProvider,
        MetricsCollector,
        NoOpCollector,
        PollContext,
        PollRunner,
        PollTrigger,
        RetryPolicy,
        Row,
        RowChange,
        RowDict,
        SourceDescriptor,
        SqlAlchemySource,
        StateStoreError,
        build_log_fields,
        default_normalizer,
        make_normalizer,
        parse_checkpoint_cursor,
        resolve_env_vars,
        serialize_cursor_part,
    )

    assert BlobCheckpointStore is not None
    assert ConfigurationError is not None
    assert CursorSerializationError is not None
    assert DbConfig is not None
    assert DbFunctionApp is not None
    assert DbReader is not None
    assert DbWriter is not None
    assert EngineProvider is not None
    assert METRIC_BATCHES_TOTAL is not None
    assert MetricsCollector is not None
    assert NoOpCollector is not None
    assert PollContext is not None
    assert PollTrigger is not None
    assert PollRunner is not None
    assert RetryPolicy is not None
    assert Row is not None
    assert RowDict is not None
    assert RowChange is not None
    assert SourceDescriptor is not None
    assert SqlAlchemySource is not None
    assert StateStoreError is not None
    assert build_log_fields is not None
    assert default_normalizer is not None
    assert make_normalizer is not None
    assert parse_checkpoint_cursor is not None
    assert resolve_env_vars is not None
    assert serialize_cursor_part is not None
