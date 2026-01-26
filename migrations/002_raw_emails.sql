-- Raw emails staging table for Gmail sync
-- Stores raw email data BEFORE parsing/reconciliation
-- This allows us to replay the parsing step if needed

CREATE TABLE IF NOT EXISTS raw_emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Gmail message ID (unique identifier from Google)
    gmail_id TEXT NOT NULL UNIQUE,

    -- Email metadata from Gmail API
    thread_id TEXT,
    subject TEXT,
    snippet TEXT,  -- Short preview text from Gmail
    sender TEXT,

    -- Raw MIME content (base64 decoded)
    raw_body TEXT NOT NULL,

    -- Inferred event type based on subject/snippet (preliminary classification)
    -- Values: unknown, confirmation, cancellation, shipping, delivery
    event_type TEXT NOT NULL DEFAULT 'unknown',

    -- Gmail internal date (when email was received)
    gmail_date TEXT,

    -- Processing status
    -- pending: not yet processed by reconciler
    -- processed: successfully parsed and applied to orders
    -- failed: parsing failed (see error_message)
    -- skipped: not a relevant email
    processing_status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,

    -- Timestamps
    fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at TEXT
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_raw_emails_gmail_id ON raw_emails(gmail_id);
CREATE INDEX IF NOT EXISTS idx_raw_emails_status ON raw_emails(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_emails_event_type ON raw_emails(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_emails_gmail_date ON raw_emails(gmail_date);

-- Composite index for faster pending email fetch (covers WHERE and ORDER BY)
CREATE INDEX IF NOT EXISTS idx_raw_emails_pending_lookup ON raw_emails(processing_status, gmail_date, event_type);
