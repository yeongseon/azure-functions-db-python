-- Source table polled by the @db.trigger.
--
-- The cursor column is `updated_at`. MySQL supports
-- `DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)` directly
-- on DATETIME columns, so unlike the PostgreSQL example we do **not** need
-- a `BEFORE INSERT OR UPDATE` trigger to maintain it — the server does it
-- for us on every INSERT and every UPDATE.
--
-- We use DATETIME(6) (microsecond precision) rather than TIMESTAMP:
--   * TIMESTAMP is stored as UTC but converted on read using the session
--     time zone, which introduces non-determinism into the cursor
--     comparison the framework relies on.
--   * DATETIME is timezone-naive and returns exactly what was written, so
--     if the server always writes UTC (see `--default-time-zone=+00:00`
--     in docker-compose.yml) the cursor is monotonically non-decreasing
--     in real wall-clock UTC.
--
-- utf8mb4 / utf8mb4_0900_ai_ci is the MySQL 8 default and safely handles
-- 4-byte UTF-8 (unlike the legacy `utf8` alias, which is 3-byte only).
CREATE TABLE IF NOT EXISTS orders (
    id            BIGINT         NOT NULL AUTO_INCREMENT,
    customer_name VARCHAR(255)   NOT NULL,
    amount        DECIMAL(12, 2) NOT NULL,
    status        VARCHAR(32)    NOT NULL DEFAULT 'pending',
    created_at    DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at    DATETIME(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                                          ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY orders_updated_at_id_idx (updated_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Strictly-idempotent destination table.
-- The composite primary key (order_id, source_cursor) ensures that a
-- replay of the same RowChange (at-least-once delivery) collides on the
-- exact same row and is a no-op upsert. Keying on order_id alone would
-- be a latest-state projection: replays still hit the same row, but an
-- out-of-order replay of an older event could overwrite a newer state.
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id      BIGINT         NOT NULL,
    source_cursor DATETIME(6)    NOT NULL,
    customer_name VARCHAR(255)   NOT NULL,
    amount        DECIMAL(12, 2) NOT NULL,
    status        VARCHAR(32)    NOT NULL,
    processed_at  DATETIME(6)    NOT NULL,
    PRIMARY KEY (order_id, source_cursor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
