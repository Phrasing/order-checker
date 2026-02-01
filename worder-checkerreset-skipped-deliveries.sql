-- Reset skipped delivery emails to pending so they'll be re-processed
UPDATE raw_emails
SET status = 'pending', processed_at = NULL
WHERE status = 'skipped'
AND event_type = 'delivery'
AND subject LIKE '%Arrived%Pokemon%'
LIMIT 25;

-- Check how many were reset
SELECT 'Emails reset to pending:' as info, COUNT(*) as count
FROM raw_emails
WHERE status = 'pending'
AND event_type = 'delivery'
AND subject LIKE '%Arrived%Pokemon%';
