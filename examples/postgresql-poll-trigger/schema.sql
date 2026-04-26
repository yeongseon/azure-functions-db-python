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

-- Idempotent destination table.
-- The trigger handler upserts on order_id so re-delivery (at-least-once)
-- collapses into a no-op write of identical data.
CREATE TABLE IF NOT EXISTS processed_orders (
    order_id      BIGINT PRIMARY KEY,
    customer_name TEXT NOT NULL,
    amount        NUMERIC(12, 2) NOT NULL,
    status        TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL
);
