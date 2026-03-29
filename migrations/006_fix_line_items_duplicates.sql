-- Migration 006: Fix line_items duplicates
-- Adds UNIQUE constraint on (order_id, name) to make INSERT OR IGNORE work correctly

-- Step 1: Create new table with UNIQUE constraint
CREATE TABLE IF NOT EXISTS line_items_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    price REAL,
    image_url TEXT,
    status TEXT NOT NULL DEFAULT 'ordered',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(order_id, name)
);

-- Step 2: Copy deduplicated data (keep one row per order_id+name, prefer highest status)
-- Status priority: delivered > shipped > canceled > ordered
INSERT INTO line_items_new (order_id, name, quantity, price, image_url, status, created_at, updated_at)
SELECT
    order_id,
    name,
    quantity,
    price,
    image_url,
    -- Pick the "best" status when there are duplicates
    CASE
        WHEN MAX(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) = 1 THEN 'delivered'
        WHEN MAX(CASE WHEN status = 'shipped' THEN 1 ELSE 0 END) = 1 THEN 'shipped'
        WHEN MAX(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) = 1 THEN 'canceled'
        ELSE 'ordered'
    END as status,
    MIN(created_at),
    MAX(updated_at)
FROM line_items
GROUP BY order_id, name;

-- Step 3: Drop old table and rename
DROP TABLE line_items;
ALTER TABLE line_items_new RENAME TO line_items;

-- Step 4: Recreate indexes
CREATE INDEX IF NOT EXISTS idx_line_items_order ON line_items(order_id);
CREATE INDEX IF NOT EXISTS idx_line_items_status ON line_items(status);

-- Step 5: Recreate trigger
CREATE TRIGGER IF NOT EXISTS line_items_updated_at
    AFTER UPDATE ON line_items
BEGIN
    UPDATE line_items SET updated_at = datetime('now') WHERE id = NEW.id;
END;
