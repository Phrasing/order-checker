-- Clear raw email bodies that have already been processed.
-- The parsed data lives in orders/line_items/email_events.
-- Raw HTML can be re-fetched from Gmail by gmail_id if ever needed.
UPDATE raw_emails SET raw_body = '' WHERE processing_status IN ('processed', 'skipped', 'failed');
