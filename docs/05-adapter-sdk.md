# Adapter SDK

Most applications should use `SqlAlchemySource` with a SQLAlchemy connection
URL. PostgreSQL, MySQL and SQL Server are the bundled driver paths; SQLite is
also exercised by the integration suite. Other SQLAlchemy dialects are BYOD,
not certified backends. See [database operation support](30-database-operation-support.md).

Custom sources are an **experimental extension point**, not shipped MongoDB,
Kafka, REST or CDC integrations. This project maintains no such adapters and
does not certify their ordering or delivery behavior. Readers and writers
remain SQLAlchemy-based regardless of the trigger source.

## Supported protocol

The shipped protocol is exported from `azure_functions_db.trigger` (not the
package root). It has exactly two members:

```python
from collections.abc import Sequence
from typing import Protocol

from azure_functions_db.core.types import CursorValue, RawRecord, SourceDescriptor


class SourceAdapter(Protocol):
    @property
    def source_descriptor(self) -> SourceDescriptor: ...

    def fetch(self, cursor: CursorValue | None, batch_size: int) -> Sequence[RawRecord]: ...
```

`RawRecord` is `dict[str, object]`, not an envelope with `row` or
`next_checkpoint` fields. `SourceDescriptor` is an immutable value containing
`name`, `kind` and `fingerprint`. `CursorValue` contains JSON scalar values or
a tuple of scalar parts. The runner reloads the checkpoint and passes its
parsed cursor to `fetch`; the source returns at most `batch_size` raw records
strictly after that cursor, in deterministic cursor/tiebreaker order.

The earlier `validate/open/close/fetch_changes/capability`, `FetchResult` and
`AdapterCapability` descriptions were design sketches, **not shipped APIs**.
Do not implement them as a second protocol. No public API is removed here.
The source owner manages any resources it acquires; the runner does not call
source lifecycle methods.

## Executable example

[`examples/custom_source.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/examples/custom_source.py)
contains `NumberSource`, a deliberately finite, in-memory source using integer
cursors. It is a protocol demonstration, not a production ingestion adapter.
Its `source_descriptor` property and `fetch(cursor, batch_size)` method are
type-checked by a `SourceAdapter` assignment in
[`tests/test_custom_source_example.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/tests/test_custom_source_example.py).
That test runs the actual `PollRunner` over successive batches and resumes
from a committed checkpoint using a fresh source. It uses the existing
in-memory state-store fixture, not Azure storage or a new service stack.

## Normalization and checkpoint ownership

An `EventNormalizer` converts each raw record plus its descriptor into a
`RowChange`. For `PollRunner` this is supplied explicitly; `PollTrigger` uses
the package's SQLAlchemy-oriented normalizer by default. A custom source must
supply compatible records or an appropriate normalizer; implementing the two
protocol members alone does not certify semantic compatibility.

The runner derives the next checkpoint from the last normalized event and
commits it **only after the entire handler batch succeeds**, under the current
lease. Sources must not advance shared checkpoints in `fetch`. Preserve:

- Stable total ordering, including a primary-key tiebreaker for equal cursors.
- Monotonic cursor and commit-visibility preconditions.
- JSON-serializable cursor parts and a stable source fingerprint.
- Lease renewal, fencing and conditional checkpoint commits in the state store.
- Idempotent handlers: delivery is at-least-once under the
  [source preconditions](03-semantics.md#12-source-preconditions), not exactly-once.

## SQLAlchemy source

Use `table=` for reflected-table polling or the documented typed query-binding
contract for query mode. The implementation owns dialect quoting, parameter
binding, cursor predicates and stable ordering. Do not interpolate user values
or identifiers into SQL: bind values and allow-list identifiers. Cursor-column
polling cannot detect hard deletes or reconstruct every intermediate update.
Use soft deletion or a polled outbox when those semantics are needed.

Backend-specific tests live in `tests/integration/db/test_source_live.py`;
unit coverage is in `tests/test_adapter_sqlalchemy.py`. An in-memory custom
source test does not establish any non-SQL backend support. Native change
streams, resume tokens and partitioning would require separately specified
semantics and verification; none are promised by this protocol.
