# Event Model

## 1. Goal

Enable handlers to consume results from different databases in a common, unified way.

## 2. Standard Event Object

```python
@dataclass
class RowChange:
    event_id: str
    op: str
    source: SourceDescriptor
    cursor: CursorValue
    pk: dict[str, object]
    before: dict[str, object] | None
    after: dict[str, object] | None
    metadata: dict[str, object]
```

## 2.1 Normalization Responsibility Separation

The adapter returns a `RawRecord` (row dict + cursor + pk),
and core's `EventNormalizer` transforms it into a `RowChange`.

Normalization process:
1. `RawRecord.row` → `after` dict (before=None in cursor polling)
2. `source_descriptor` + `cursor` + `pk` → generate `event_id`
3. Determine `op` based on adapter capability
4. Normalize types: datetime, Decimal, UUID, etc.

This separation allows the adapter to focus solely on DB access,
while event structure and identifier generation logic are managed consistently in core.

## 3. Field Semantics

### event_id
Stable identifier for deduplication.

Default generation rule:
```text
sha256(
  poller_name +
  source_fingerprint +
  op +
  normalized_cursor +
  normalized_pk
)
```

### op
- `insert` — new row inserted (deterministic in CDC/outbox strategies)
- `update` — existing row changed (deterministic in CDC strategy)
- `upsert` — insert or update, but indistinguishable (default in cursor polling)
- `delete` — row deleted (CDC/soft-delete strategies only)
- `unknown` — cannot be determined

MVP cursor polling uses `upsert` in most cases.
`can_distinguish_insert_update: bool` must exist in the adapter capability
for `insert`/`update` to be emitted separately.

### source
```json
{
  "kind": "sqlalchemy",
  "dialect": "postgresql",
  "driver": "psycopg",
  "database": "orders",
  "schema": "public",
  "object": "orders"
}
```

### cursor
```json
{
  "kind": "timestamp+pk",
  "value": "2026-04-07T01:23:45.123456Z",
  "tiebreaker": {"id": 12093}
}
```

### pk
Primary key dict of the original row.

### before / after
- `before=None` is common in polling strategies
- Native CDC strategies may support `before`
- `after=None` is possible for deletes

### metadata
Examples:
- batch_id
- fetched_at
- source_query
- raw_resume_token
- attempt
- partition_id

## 4. Serialization Rules

Events must be JSON-serializable.

Normalization:
- datetime → RFC3339 UTC string
- Decimal → string
- UUID → string
- bytes → base64
- enum → string

## 5. Payload Size Policy

MVP defaults to delivering full row payloads.
Future options:
- Projected columns
- Key-only mode
- Custom serializer

## 6. Pydantic Mapping

Optional feature:

```python
class OrderChange(BaseModel):
    id: int
    status: str
    updated_at: datetime

# Future: Pydantic model mapping for trigger events
# @db.trigger(arg_name="events", source=source, checkpoint_store=store, model=OrderChange)
# def handle(events: list[OrderChange], context):
#     ...
```

Note:
- Model validation failure defaults to batch failure
- `drop_invalid=True` option under consideration for the future

## 7. Downstream Propagation Rules

Envelope for Service Bus/Event Hub relay:

```json
{
  "specversion": "azure-functions-db/1.0",
  "type": "db.row.change",
  "id": "evt_...",
  "time": "2026-04-07T01:23:46Z",
  "source": "azure-functions-db://orders",
  "data": {
    "...": "RowChange payload"
  }
}
```

CloudEvents compatibility is a future option.
