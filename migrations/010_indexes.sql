-- Indexes on optional columns (account_id, etc.)
-- This migration runs AFTER optional_columns adds the columns in db/mod.rs

-- Basic account_id foreign key indexes (moved from migration 005)
CREATE INDEX IF NOT EXISTS idx_raw_emails_account ON raw_emails(account_id);
CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id);

-- Compound index for filtered dashboard queries (account + date range)
CREATE INDEX IF NOT EXISTS idx_orders_account_date ON orders(account_id, order_date);

-- Index for email_events backfill queries on shipped_date
CREATE INDEX IF NOT EXISTS idx_email_events_order_type ON email_events(order_id, event_type);
