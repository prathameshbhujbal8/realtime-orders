-- =============================================================================
-- trigger.sql
-- Run this once against your PostgreSQL database before starting the server.
--
-- What this file does:
--   1. Creates the `orders` table
--   2. Creates the trigger function that fires on INSERT / UPDATE / DELETE
--   3. Attaches the trigger to the table
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 1. Orders table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id            SERIAL PRIMARY KEY,
    customer_name VARCHAR(120)  NOT NULL,
    product_name  VARCHAR(120)  NOT NULL,
    status        VARCHAR(20)   NOT NULL DEFAULT 'pending'
                      CHECK (status IN ('pending', 'shipped', 'delivered')),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Index the most-queried column so REST list fetches are fast
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders (updated_at DESC);

COMMENT ON TABLE  orders IS 'Customer orders tracked through fulfilment lifecycle.';
COMMENT ON COLUMN orders.status IS 'One of: pending | shipped | delivered';


-- ---------------------------------------------------------------------------
-- 2. Trigger function — builds a JSON payload and sends a NOTIFY
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION notify_orders_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    payload   JSON;
    record_data JSON;
BEGIN
    -- For DELETE we only have OLD; for INSERT/UPDATE we use NEW.
    IF (TG_OP = 'DELETE') THEN
        record_data := row_to_json(OLD);
    ELSE
        record_data := row_to_json(NEW);
    END IF;

    -- Build the notification envelope
    payload := json_build_object(
        'channel',    'orders_updates',
        'operation',  TG_OP,          -- 'INSERT' | 'UPDATE' | 'DELETE'
        'table',      TG_TABLE_NAME,
        'record',     record_data,
        'timestamp',  to_char(NOW() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
    );

    -- NOTIFY wakes all connections that called LISTEN on this channel.
    -- The payload is limited to 8000 bytes; a single orders row is well
    -- within that limit.  For very large rows, send only the PK and let
    -- the client fetch the full record via REST.
    PERFORM pg_notify('orders_updates', payload::TEXT);

    -- Trigger functions must return the affected row (or NULL for AFTER triggers)
    IF (TG_OP = 'DELETE') THEN
        RETURN OLD;
    ELSE
        RETURN NEW;
    END IF;
END;
$$;

COMMENT ON FUNCTION notify_orders_change() IS
    'Fires on INSERT/UPDATE/DELETE of orders and sends a NOTIFY on orders_updates channel.';


-- ---------------------------------------------------------------------------
-- 3. Attach trigger to orders table
-- ---------------------------------------------------------------------------
-- Drop first so re-running this file is idempotent
DROP TRIGGER IF EXISTS orders_change_trigger ON orders;

CREATE TRIGGER orders_change_trigger
AFTER INSERT OR UPDATE OR DELETE
ON orders
FOR EACH ROW
EXECUTE FUNCTION notify_orders_change();

COMMENT ON TRIGGER orders_change_trigger ON orders IS
    'Calls notify_orders_change() after every row-level mutation.';
