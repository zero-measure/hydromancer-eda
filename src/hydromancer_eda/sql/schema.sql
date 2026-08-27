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

-- Labels are derived current state rather than event history. Schema changes to
-- this table use the explicit sql/reset_wallet_labels.sql operation; startup
-- schema initialization never truncates or migrates existing label rows.
DO $$
DECLARE
    labels_table REGCLASS := to_regclass('wallet_flow_labels');
BEGIN
    IF labels_table IS NOT NULL
       AND (
           EXISTS (
               SELECT 1
               FROM unnest(ARRAY[
                   'wallet', 'bucket_ts', 'trade_count', 'markout_sample_count',
                   'markout_p10', 'markout_p20', 'markout_p30', 'markout_p40',
                   'markout_p50', 'markout_p60', 'markout_p70', 'markout_p80',
                   'markout_p90', 'realized_pnl', 'liquidation_ratio',
                   'taker_ratio', 'cancel_to_fill_ratio',
                   'message_cadence_per_min', 'holding_time_sec_median',
                   'liquidity_provision_score', 'round_number_notional_ratio',
                   'round_number_size_ratio', 'max_leverage',
                   'cross_margin_ratio', 'updated_at'
               ]) required(column_name)
               WHERE NOT EXISTS (
                   SELECT 1
                   FROM pg_attribute
                   WHERE attrelid = labels_table
                     AND attname = required.column_name
                     AND NOT attisdropped
               )
           )
           OR EXISTS (
               SELECT 1
               FROM pg_attribute
               WHERE attrelid = labels_table
                 AND attname IN ('taker_markout', 'liquidation_count')
                 AND NOT attisdropped
           )
           OR NOT EXISTS (
               SELECT 1
               FROM pg_constraint
               WHERE conrelid = labels_table
                 AND contype = 'p'
                 AND conkey = ARRAY[
                     (SELECT attnum
                      FROM pg_attribute
                      WHERE attrelid = labels_table
                        AND attname = 'wallet'
                        AND NOT attisdropped)
                 ]::smallint[]
           )
       )
    THEN
        RAISE EXCEPTION
            'wallet_flow_labels has an obsolete schema; run sql/reset_wallet_labels.sql before startup';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS wallet_flow_labels (
    wallet TEXT PRIMARY KEY,
    bucket_ts TIMESTAMPTZ NOT NULL,

    trade_count BIGINT,
    markout_sample_count BIGINT,
    markout_p10 DOUBLE PRECISION,
    markout_p20 DOUBLE PRECISION,
    markout_p30 DOUBLE PRECISION,
    markout_p40 DOUBLE PRECISION,
    markout_p50 DOUBLE PRECISION,
    markout_p60 DOUBLE PRECISION,
    markout_p70 DOUBLE PRECISION,
    markout_p80 DOUBLE PRECISION,
    markout_p90 DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    liquidation_ratio DOUBLE PRECISION,
    taker_ratio DOUBLE PRECISION,
    cancel_to_fill_ratio DOUBLE PRECISION,
    message_cadence_per_min DOUBLE PRECISION,
    holding_time_sec_median DOUBLE PRECISION,
    liquidity_provision_score DOUBLE PRECISION,
    round_number_notional_ratio DOUBLE PRECISION,
    round_number_size_ratio DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    cross_margin_ratio DOUBLE PRECISION,

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
