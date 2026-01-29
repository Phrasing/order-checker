CREATE TABLE IF NOT EXISTS image_thumbnails (
    image_id TEXT PRIMARY KEY,
    thumb_bytes BLOB NOT NULL,
    content_type TEXT,
    width INTEGER,
    height INTEGER,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_image_thumbnails_image_id ON image_thumbnails(image_id);
