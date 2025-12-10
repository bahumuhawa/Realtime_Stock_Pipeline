CREATE TABLE IF NOT EXISTS ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC(20, 6) NOT NULL,
    volume BIGINT,
    source TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    received_time TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_event_time
    ON ticks (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS symbol_metrics (
    symbol TEXT PRIMARY KEY,
    last_price NUMERIC(20, 6),
    moving_avg_1min NUMERIC(20, 6),
    vol_1min BIGINT,
    updated_at TIMESTAMPTZ DEFAULT now()
);
