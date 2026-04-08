from __future__ import annotations

__version__ = "0.1.0"

from .adapter import SqlAlchemySource
from .binding import DbReader, DbWriter
from .core.config import DbConfig, resolve_env_vars
from .core.engine import EngineProvider
from .core.errors import (
    ConfigurationError,
    CursorSerializationError,
    DbConnectionError,
    DbError,
    NotFoundError,
    QueryError,
    WriteError,
)
from .core.serializers import parse_checkpoint_cursor, serialize_cursor_part
from .core.types import (
    CursorPart,
    CursorValue,
    JsonScalar,
    JsonValue,
    RawRecord,
    Row,
    RowDict,
    SourceDescriptor,
)
from .decorator import DbFunctionApp
from .observability import (
    METRIC_BATCH_SIZE,
    METRIC_BATCHES_TOTAL,
    METRIC_COMMIT_DURATION_MS,
    METRIC_EVENTS_TOTAL,
    METRIC_FAILURES_TOTAL,
    METRIC_FETCH_DURATION_MS,
    METRIC_HANDLER_DURATION_MS,
    METRIC_LAG_SECONDS,
    METRIC_LAST_SUCCESS_TIMESTAMP,
    MetricsCollector,
    NoOpCollector,
    build_log_fields,
)
from .state import (
    BlobCheckpointStore,
    FingerprintMismatchError,
    LeaseConflictError,
    StateStoreError,
)
from .trigger.context import PollContext
from .trigger.errors import (
    CommitError,
    FetchError,
    HandlerError,
    LeaseAcquireError,
    LostLeaseError,
    PollerError,
    SerializationError,
    SourceConfigurationError,
)
from .trigger.events import RowChange
from .trigger.normalizers import default_normalizer, make_normalizer
from .trigger.poll import PollTrigger
from .trigger.retry import RetryPolicy
from .trigger.runner import (
    EventNormalizer,
    PollRunner,
    SourceAdapter,
    StateStore,
)

__all__ = [
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
]
