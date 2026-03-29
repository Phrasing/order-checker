-- Fix order dates that defaulted to Utc::now() when HTML date extraction failed.
-- Uses the confirmation email's gmail_date (stored in email_events.email_date as millis)
-- to correct orders where order_date is >30 days newer than the actual email date.
-- This is idempotent: re-running won't change already-correct orders.

UPDATE orders SET order_date = (
    SELECT strftime('%Y-%m-%dT%H:%M:%S+00:00',
           CAST(ee.email_date AS INTEGER) / 1000, 'unixepoch')
    FROM email_events ee
    WHERE ee.order_id = orders.id AND ee.event_type = 'confirmation'
    ORDER BY ee.email_date ASC LIMIT 1
)
WHERE EXISTS (
    SELECT 1 FROM email_events ee
    WHERE ee.order_id = orders.id AND ee.event_type = 'confirmation'
    AND (julianday(orders.order_date) -
         julianday(datetime(CAST(ee.email_date AS INTEGER) / 1000, 'unixepoch'))) > 30
);
