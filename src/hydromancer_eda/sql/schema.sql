CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS orderflow_events (
    id BIGSERIAL,
    ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    wallet TEXT NOT NULL,
    event_type TEXT NOT NULL,
    side TEXT NOT NULL,
    aggressor TEXT NOT NULL,
    price NUMERIC,
    size NUMERIC,
    notional NUMERIC,
    leverage NUMERIC,
    margin_mode TEXT NOT NULL,
    order_id TEXT,
    trade_id TEXT,
    position_id TEXT,
    tx_index BIGINT NOT NULL,
    realized_pnl NUMERIC,
    liquidation BOOLEAN NOT NULL DEFAULT FALSE,
    raw JSONB NOT NULL,
    PRIMARY KEY (id, ts)
);

SELECT create_hypertable('orderflow_events', by_range('ts'), if_not_exists => TRUE, migrate_data => TRUE);

-- Migration for databases created before trade_id was introduced.
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS trade_id TEXT;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS tx_index BIGINT;
DO $$
BEGIN
    -- A market-wide fill emits one entry per participant. Replace the old
    -- timestamp/transaction watermark index, which lost counterparties.
    IF to_regclass('orderflow_events_dedupe_idx') IS NOT NULL
       AND pg_get_indexdef(to_regclass('orderflow_events_dedupe_idx')) NOT LIKE '%wallet%'
    THEN
        DROP INDEX orderflow_events_dedupe_idx;
    END IF;
END $$;
-- Keep every participant at (time, txIndex); tid is the documented fill ID.
-- oid and side make the identity stable when a fill ID is absent.
CREATE UNIQUE INDEX IF NOT EXISTS orderflow_events_dedupe_idx
ON orderflow_events (ts, tx_index, wallet, COALESCE(trade_id, ''), COALESCE(order_id, ''), side)
WHERE tx_index IS NOT NULL;

-- Exactly one simple durable resume row is kept per WebSocket channel.
CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    channel TEXT PRIMARY KEY,
    session_id TEXT,
    cursor TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Malformed upstream entries are retained atomically with cursor progress so a
-- poison entry cannot either disappear silently or halt market-wide ingestion.
CREATE TABLE IF NOT EXISTS ingestion_rejects (
    id BIGSERIAL PRIMARY KEY,
    channel TEXT NOT NULL,
    cursor TEXT,
    reason TEXT NOT NULL,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ingestion_rejects_created_at_idx
ON ingestion_rejects (created_at DESC);

CREATE INDEX IF NOT EXISTS orderflow_events_wallet_ts_idx ON orderflow_events (wallet, ts DESC);
CREATE INDEX IF NOT EXISTS orderflow_events_symbol_ts_idx ON orderflow_events (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS orderflow_events_type_ts_idx ON orderflow_events (event_type, ts DESC);

CREATE TABLE IF NOT EXISTS wallet_flow_labels (
    bucket_ts TIMESTAMPTZ NOT NULL,
    wallet TEXT NOT NULL,

    taker_markout DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    liquidation_count BIGINT,
    taker_ratio DOUBLE PRECISION,
    cancel_to_fill_ratio DOUBLE PRECISION,
    message_cadence_per_min DOUBLE PRECISION,
    holding_time_sec_median DOUBLE PRECISION,
    liquidity_provision_score DOUBLE PRECISION,
    round_number_notional_ratio DOUBLE PRECISION,
    round_number_size_ratio DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    cross_margin_ratio DOUBLE PRECISION,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (bucket_ts, wallet)
);

-- Migration for databases created before token-size roundness was introduced.
ALTER TABLE wallet_flow_labels
ADD COLUMN IF NOT EXISTS round_number_size_ratio DOUBLE PRECISION;

SELECT create_hypertable('wallet_flow_labels', by_range('bucket_ts'), if_not_exists => TRUE, migrate_data => TRUE);
CREATE INDEX IF NOT EXISTS wallet_flow_labels_wallet_ts_idx ON wallet_flow_labels (wallet, bucket_ts DESC);
