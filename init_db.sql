CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    raw JSONB,
    UNIQUE (symbol, ts)
);