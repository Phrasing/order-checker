-- Backfill existing shipped/delivered orders from email_events
-- (Column addition is handled by optional_columns in db/mod.rs)
UPDATE orders SET shipped_date = (
    SELECT email_date FROM email_events
    WHERE email_events.order_id = orders.id
    AND email_events.event_type = 'shipping'
    ORDER BY email_date ASC
    LIMIT 1
)
WHERE shipped_date IS NULL
AND status IN ('shipped', 'delivered');
