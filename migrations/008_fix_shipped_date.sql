-- Fix shipped_date values that were stored as millisecond epoch timestamps
-- Convert them to RFC3339 format to match order_date format
-- Only match pure numeric strings (NOT GLOB '*[^0-9]*' rejects any string with non-digit chars)
UPDATE orders
SET shipped_date = strftime('%Y-%m-%dT%H:%M:%SZ', shipped_date / 1000, 'unixepoch')
WHERE shipped_date IS NOT NULL
  AND shipped_date GLOB '[0-9]*'
  AND shipped_date NOT GLOB '*[^0-9]*';

-- Re-backfill any values corrupted to 1970 by the previous buggy GLOB pattern
UPDATE orders
SET shipped_date = (
    SELECT strftime('%Y-%m-%dT%H:%M:%SZ', email_date / 1000, 'unixepoch')
    FROM email_events
    WHERE email_events.order_id = orders.id
    AND email_events.event_type = 'shipping'
    ORDER BY email_date ASC
    LIMIT 1
)
WHERE shipped_date LIKE '1970-%';
