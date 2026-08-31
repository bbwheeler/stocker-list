CREATE TABLE IF NOT EXISTS companies (
    symbol        TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    exchange      TEXT NOT NULL DEFAULT 'TSX',
    currency      TEXT NOT NULL DEFAULT 'CAD',
    last_updated  TIMESTAMPTZ NOT NULL DEFAULT now()
);
