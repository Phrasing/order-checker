CREATE TABLE IF NOT EXISTS images (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    image_bytes BLOB NOT NULL,
    content_type TEXT,
    is_transparent INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_images_url ON images(url);
