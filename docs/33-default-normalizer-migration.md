# Default normalizer and checkpoint progress

`PollTrigger(normalizer=default_normalizer, ...)` now selects the same
source-aware normalizer as omitting `normalizer`. The source must expose
`cursor_column` and `pk_columns`, as `SqlAlchemySource` does. The checkpoint
contains the cursor followed by every primary-key tiebreaker; replaying an
unchanged cursor/key pair produces the same event ID for the same source.

`SourceDescriptor` contains only `name`, `kind`, and `fingerprint`; it cannot
identify cursor or key columns. Direct calls to `default_normalizer(record,
descriptor)` now raise `ConfigurationError` instead of returning `cursor=None`
and a random event ID. This intentionally changes the unsafe direct-call
behavior without changing its signature or removing the exported symbol.

For direct calls, custom adapters without metadata, and `PollRunner`, construct
an explicit normalizer:

```python
from azure_functions_db.trigger.normalizers import make_normalizer

normalizer = make_normalizer(cursor_column="updated_at", pk_columns=["tenant_id", "id"])
```

Pass this callable as `normalizer=normalizer`. A wrapper around
`default_normalizer` cannot supply the missing metadata; migrate that wrapper
to the factory too. Explicit custom normalizers remain responsible for
returning a usable cursor and stable identity.
