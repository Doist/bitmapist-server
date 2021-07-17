PRAGMA journal_mode=WAL;
PRAGMA synchronous=normal;
CREATE TABLE IF NOT EXISTS bitmaps(
    name TEXT PRIMARY KEY NOT NULL CHECK(name!=''),
    expireat INTEGER NOT NULL DEFAULT 0,
    bytes BLOB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bitmaps_expireat ON bitmaps(expireat);