from __future__ import annotations

import sys
import warnings

__version__ = "0.7.2"

from .adapter import SqlAlchemySource
from .binding import DbReader, DbWriter
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
from .core.types import CursorPart, CursorValue
from .decorator import DbBindings, DbOut, get_db_metadata
from .observability import (
    MetricsCollector,
)
from .state import BlobCheckpointStore
from .trigger.context import PollContext
from .trigger.errors import (
    CommitError,
    FetchError,
    HandlerError,
    LeaseAcquireError,
    LostLeaseError,
)
from .trigger.events import RowChange
from .trigger.poll import PollTrigger
from .trigger.retry import RetryPolicy

__all__ = [
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
    "DbOut",
    "get_db_metadata",
    "PollContext",
    "PollTrigger",
    "QueryError",
    "RetryPolicy",
    "RowChange",
    "SqlAlchemySource",
    "WriteError",
]


if sys.version_info < (3, 11):
    warnings.warn(
        "azure-functions-db will drop support for Python 3.10 in its next minor release. "
        "Python 3.10 reaches end of life in October 2026; upgrade to Python 3.11 "
        "or newer to keep receiving updates.",
        FutureWarning,
        stacklevel=2,
    )
