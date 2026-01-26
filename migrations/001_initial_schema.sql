-- Walmart Order Reconciler - Initial Schema
-- This schema supports event-sourcing where order state is derived from multiple email events

-- Orders table: Core order information
CREATE TABLE IF NOT EXISTS orders (
    -- Normalized ID (hyphens stripped for consistency across email types)
    id TEXT PRIMARY KEY NOT NULL,

    -- Order timestamp (from confirmation email)
    order_date TEXT NOT NULL,  -- ISO8601 format

    -- Total cost (nullable - may not be present in cancellation emails)
    total_cost REAL,

    -- Current computed status: confirmed, shipped, canceled, partially_canceled, delivered
    status TEXT NOT NULL DEFAULT 'confirmed',

    -- Metadata
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Line items table: Individual products within an order
CREATE TABLE IF NOT EXISTS line_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Foreign key to orders table
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,

    -- Product details
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price REAL,  -- Per-item price, nullable
    image_url TEXT,

    -- Item status: ordered, shipped, canceled
    status TEXT NOT NULL DEFAULT 'ordered',

    -- Metadata
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Email events table: Raw event log for debugging and reprocessing
-- This supports the event-sourcing model where we can replay events to reconstruct state
CREATE TABLE IF NOT EXISTS email_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- The order this event applies to (normalized ID)
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,

    -- Event type: confirmation, cancellation, shipping, delivery
    event_type TEXT NOT NULL,

    -- Raw email data for reprocessing
    email_subject TEXT,
    email_date TEXT NOT NULL,
    raw_html TEXT,  -- Store original HTML for re-parsing if needed

    -- Parsed data snapshot (JSON)
    parsed_data TEXT,

    -- Processing metadata
    processed_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_line_items_order ON line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_status ON line_items(status);
CREATE INDEX IF NOT EXISTS idx_email_events_order ON email_events(order_id);
CREATE INDEX IF NOT EXISTS idx_email_events_type ON email_events(event_type);

-- Trigger to update the updated_at timestamp on orders
CREATE TRIGGER IF NOT EXISTS orders_updated_at
    AFTER UPDATE ON orders
BEGIN
    UPDATE orders SET updated_at = datetime('now') WHERE id = NEW.id;
END;

-- Trigger to update the updated_at timestamp on line_items
CREATE TRIGGER IF NOT EXISTS line_items_updated_at
    AFTER UPDATE ON line_items
BEGIN
    UPDATE line_items SET updated_at = datetime('now') WHERE id = NEW.id;
END;
