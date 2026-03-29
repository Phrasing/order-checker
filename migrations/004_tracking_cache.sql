-- Migration 004: Tracking cache tables
-- Caches tracking status from 17track.net to avoid excessive API calls

-- Tracking status cache
CREATE TABLE IF NOT EXISTS tracking_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT REFERENCES orders(id) ON DELETE CASCADE,
    tracking_number TEXT NOT NULL UNIQUE,
    carrier TEXT NOT NULL,
    carrier_code INTEGER NOT NULL,
    state TEXT NOT NULL,  -- label_created, picked, delivering, delivered, failed, returned, unknown
    state_description TEXT,
    is_delivered INTEGER NOT NULL DEFAULT 0,
    delivery_date TEXT,
    last_fetched_at TEXT NOT NULL,
    last_updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    fetch_count INTEGER NOT NULL DEFAULT 1,
    last_error TEXT,
    consecutive_errors INTEGER NOT NULL DEFAULT 0
);

-- Tracking events history
CREATE TABLE IF NOT EXISTS tracking_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tracking_cache_id INTEGER NOT NULL REFERENCES tracking_cache(id) ON DELETE CASCADE,
    event_time TEXT,
    event_time_iso TEXT,
    description TEXT NOT NULL,
    location TEXT,
    stage TEXT,
    sub_status TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(tracking_cache_id, event_time, description)
);

CREATE INDEX IF NOT EXISTS idx_tracking_cache_order ON tracking_cache(order_id);
CREATE INDEX IF NOT EXISTS idx_tracking_cache_state ON tracking_cache(state);
CREATE INDEX IF NOT EXISTS idx_tracking_cache_tracking ON tracking_cache(tracking_number);
CREATE INDEX IF NOT EXISTS idx_tracking_events_cache ON tracking_events(tracking_cache_id);
