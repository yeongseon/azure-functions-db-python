# Transaction Rollback for Failed Handlers

`DbWriter.transaction()` groups several writes into one SQL transaction. This
recipe shows, with a temporary SQLite database, what is and is not rolled back
when a handler fails. It is an offline learning example: no Azure account,
server or credentials are involved.

## What the example shows

```python
from azure_functions_db import DbWriter

with DbWriter(url=url, table="orders") as writer, writer.transaction():
    writer.insert(data={"id": 1, "status": "new"})
    writer.insert(data={"id": 2, "status": "new"})
```

| Scenario | Result on SQLite |
|---|---|
| Block exits normally | Both rows are committed together. |
| Handler body raises after the first insert | The insert is rolled back; no partial write is committed. |
| A later statement fails (`WriteError`, e.g. duplicate primary key) | Earlier writes in the same block are rolled back too. |
| A write made **outside** the block, then a failure inside it | The outside write stays committed: each `DbWriter` call outside `transaction()` is its own short transaction. |

Claims are limited to this API and SQLite. Other databases and drivers have
their own transaction details, so verify them separately.

## Where the transaction scope ends

The scope is the `with writer.transaction():` block of one writer: the SQL
statements it executed on one connection. It does not include the trigger's
checkpoint commit, other systems your handler calls, or a retry of the
handler. A handler that succeeds and then fails to commit the trigger
checkpoint is delivered again, so rollback inside the block does **not** make
trigger delivery exactly-once; delivery stays at-least-once and handlers must be
idempotent. See [Polling Runtime & Failure Scenarios](../24-polling-runtime-semantics.md)
(sections 5.5, 5.7 and 9).

## Run it

```bash
pip install -e .
python examples/transaction_rollback.py
python -m pytest tests/test_transaction_rollback_example.py --no-cov -q
```

Expected output of the script:

```text
after_commit: [1, 2]
after_handler_failure: [1, 2]
after_write_error: [1, 2]
after_outside_write_then_failure: [1, 2, 30]
```

Source:
[`examples/transaction_rollback.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/examples/transaction_rollback.py).
See also [Output Binding](output_binding.md) and
[Binding Semantics](../22-binding-semantics.md#42-transaction-boundaries).
