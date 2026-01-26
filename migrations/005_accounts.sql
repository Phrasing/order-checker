-- Migration 005: Multi-account support
-- Adds accounts table and account_id foreign keys to enable multiple Gmail accounts

-- Accounts table: stores Gmail account information
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Gmail email address (unique identifier)
    email TEXT NOT NULL UNIQUE,

    -- Display name (optional, for UI)
    display_name TEXT,

    -- Path to token cache file for this account
    token_cache_path TEXT NOT NULL,

    -- Whether this account is active (can be disabled without deleting)
    is_active INTEGER NOT NULL DEFAULT 1,

    -- Last successful sync timestamp
    last_sync_at TEXT,

    -- Metadata
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Note: account_id columns are added to raw_emails and orders tables
-- via ALTER TABLE in db/mod.rs (with duplicate column handling)
-- SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_accounts_email ON accounts(email);
CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(is_active);
CREATE INDEX IF NOT EXISTS idx_raw_emails_account ON raw_emails(account_id);
CREATE INDEX IF NOT EXISTS idx_orders_account ON orders(account_id);

-- Trigger to update the updated_at timestamp on accounts
CREATE TRIGGER IF NOT EXISTS accounts_updated_at
    AFTER UPDATE ON accounts
BEGIN
    UPDATE accounts SET updated_at = datetime('now') WHERE id = NEW.id;
END;
