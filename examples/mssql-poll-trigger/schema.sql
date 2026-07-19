-- SQL Server schema for the poll-trigger example.
--
-- Run against a database named `app` (see README for `CREATE DATABASE app`).
-- `GO` batch separators are sqlcmd directives; if you apply this file with a
-- different client, split on `GO` or execute each batch separately.

-- Source table polled by the @db.trigger.
-- The cursor column is `updated_at`; an AFTER INSERT, UPDATE trigger
-- guarantees it is refreshed to the current UTC time on every mutation.
-- SQL Server runs with RECURSIVE_TRIGGERS OFF by default, so the trigger's
-- own UPDATE does not re-fire the trigger.
IF OBJECT_ID('dbo.orders', 'U') IS NULL
CREATE TABLE dbo.orders (
    id            BIGINT IDENTITY(1, 1) PRIMARY KEY,
    customer_name NVARCHAR(200)  NOT NULL,
    amount        DECIMAL(12, 2) NOT NULL,
    status        NVARCHAR(50)   NOT NULL
        CONSTRAINT DF_orders_status DEFAULT 'pending',
    created_at    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_orders_created_at DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2(3)   NOT NULL
        CONSTRAINT DF_orders_updated_at DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes WHERE name = 'orders_updated_at_id_idx'
      AND object_id = OBJECT_ID('dbo.orders')
)
CREATE INDEX orders_updated_at_id_idx ON dbo.orders (updated_at, id);
GO

CREATE OR ALTER TRIGGER dbo.orders_set_updated_at
    ON dbo.orders
    AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE o
       SET updated_at = SYSUTCDATETIME()
      FROM dbo.orders o
      INNER JOIN inserted i ON o.id = i.id;
END;
GO

-- Strictly-idempotent destination table.
-- The composite primary key (order_id, source_cursor) makes a replay of the
-- same RowChange (at-least-once delivery) collide on the exact same row, so
-- the handler can treat the resulting primary-key violation as a no-op.
-- Keying on order_id alone would be a latest-state projection: an out-of-order
-- replay of an older event could overwrite a newer state.
IF OBJECT_ID('dbo.processed_orders', 'U') IS NULL
CREATE TABLE dbo.processed_orders (
    order_id      BIGINT         NOT NULL,
    source_cursor DATETIME2(3)   NOT NULL,
    customer_name NVARCHAR(200)  NOT NULL,
    amount        DECIMAL(12, 2) NOT NULL,
    status        NVARCHAR(50)   NOT NULL,
    processed_at  DATETIME2(3)   NOT NULL,
    CONSTRAINT PK_processed_orders PRIMARY KEY (order_id, source_cursor)
);
GO
