# Late commits and safe polling cursors

## Decide before deploying

Need no-miss delivery? Use a source with **documented commit-visibility ordering**,
or use CDC / a durable outbox consumption design that cannot skip unpublished
work. An increasing timestamp, identity, sequence, or version number alone is
not that guarantee. At-least-once delivery applies only to eligible changes
visible beyond the checkpoint under the source preconditions in
[Semantics](03-semantics.md#12-source-preconditions).

## The late-commit failure

1. Transaction A obtains cursor 10 but remains uncommitted.
2. Transaction B obtains cursor 20 and commits first.
3. The poller sees B, handles it, and persists checkpoint `(20, B-key)`.
4. A commits. Its cursor 10 is below the checkpoint and is never fetched.

The built-in adapter filters strictly after the complete cursor in both table
and raw-query mode. Primary-key tiebreakers handle already-visible equal-cursor
rows across batch boundaries; they do not fix late visibility below the saved
tuple (including an equal cursor with a lower key). Timestamps allocated before
commit and PostgreSQL sequence values can both have this problem. Updates can
also become invisible this way; hard deletes are not detected by this adapter.

`tests/test_late_commit_visibility.py` deterministically simulates two logical
transactions' allocation and reversed visibility using committed SQLite writes.
It exercises real polling in both adapter modes. It is not a claim that SQLite
supports simultaneous uncommitted writers, nor live PostgreSQL evidence.

## Safe designs and their conditions

- **CDC:** consume a database's documented committed change stream using an
  appropriate custom adapter/consumer. This package's built-in strategy is only
  cursor polling; it does not implement CDC.
- **Transactional outbox:** write business state and an immutable outbox event
  in the same transaction. Consume pending events with durable acknowledgment,
  or relay them into a stream with proven publication ordering. Simply polling
  an outbox's pre-commit sequence with `WHERE id > checkpoint` has the same bug.
- **Serialized publication:** a single coordinated publisher assigns the polling
  sequence only while publishing already-committed work, and completes each
  publication commit before assigning/publishing the next value. The test shows
  B then A reaching the poller with publication cursors 1 then 2. Production
  implementations must preserve this invariant across crashes and failover and
  retain durable deduplication/acknowledgment; the test is not a turnkey relay.

Do not solve this by merely polling faster, waiting a fixed interval, changing
isolation level, or using a larger batch. None establishes a bound on late
commit visibility. A custom overlapping look-back can be evaluated separately
only with a proven lateness bound, durable version-aware deduplication, and safe
update handling. Unbounded lateness defeats every finite overlap. There is no
default rewind/overlap mechanism in this package.

## Startup warning decision

No datatype-based late-commit warning is added: neither SQLAlchemy reflection
nor `cursor_column` identifies the application's transaction/publication order.
The same integer/timestamp column may be safe behind a coordinated publisher
or unsafe under concurrent pre-commit allocation. Warning on all such columns
would be guesswork. Verify the ordering invariant with the source owner before
deployment; if it cannot be established, choose one of the designs above. The
existing missing-index warning concerns query performance, not delivery safety.
