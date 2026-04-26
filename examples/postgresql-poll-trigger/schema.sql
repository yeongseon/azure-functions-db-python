-- Source table polled by the @db.trigger.
-- The cursor column is updated_at; a BEFORE INSERT OR UPDATE trigger
-- guarantees it is monotonically non-decreasing on every mutation.
CREATE TABLE IF NOT EXISTS orders (
    id            BIGSERIAL PRIMARY KEY,
    customer_name TEXT      NOT NULL,
    amount        NUMERIC(12, 2) NOT NULL,
    status        TEXT      NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_updated_at_id_idx
    ON orders (updated_at, id);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS orders_set_updated_at ON orders;
CREATE TRIGGER orders_set_updated_at
    BEFORE INSERT OR UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Strictly-idempotent destination table.
-- The composite primary key (order_id, source_cursor) ensures that a
-- replay of the same RowChange (at-least-once delivery) collides on the
-- exact same row and is a no-op upsert. Keying on order_id alone would
-- be a latest-state projection: replays still hit the same row, but an
-- out-of-order replay of an older event could overwrite a newer state.
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id      BIGINT NOT NULL,
    source_cursor TIMESTAMPTZ NOT NULL,
    customer_name TEXT NOT NULL,
    amount        NUMERIC(12, 2) NOT NULL,
    status        TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (order_id, source_cursor)
);
