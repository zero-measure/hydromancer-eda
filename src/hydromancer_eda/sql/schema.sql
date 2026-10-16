CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Version 2 deliberately requires the explicit analytical reset. Startup never
-- drops or rewrites analytical history behind the operator's back.
DO $$
DECLARE
    current_version INTEGER;
BEGIN
    IF to_regclass('analytics_schema_meta') IS NOT NULL THEN
        EXECUTE 'SELECT MAX(version) FROM analytics_schema_meta' INTO current_version;
    END IF;
    IF to_regclass('orderflow_events') IS NOT NULL
       AND current_version IS DISTINCT FROM 2
    THEN
        RAISE EXCEPTION
            'obsolete analytics schema; stop ingestion and run sql/reset_analytics.sql';
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS analytics_schema_meta (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    version INTEGER NOT NULL
);
INSERT INTO analytics_schema_meta (singleton, version)
VALUES (TRUE, 2)
ON CONFLICT (singleton) DO UPDATE SET version = EXCLUDED.version;

CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    channel TEXT PRIMARY KEY,
    session_id TEXT,
    cursor TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_gaps (
    id BIGSERIAL PRIMARY KEY,
    channel TEXT NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (end_ts >= start_ts)
);
CREATE INDEX IF NOT EXISTS ingestion_gaps_channel_time_idx
ON ingestion_gaps (channel, start_ts, end_ts);

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

CREATE TABLE IF NOT EXISTS orderflow_events (
    id BIGSERIAL,
    ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    wallet TEXT NOT NULL,
    side TEXT NOT NULL,
    aggressor TEXT NOT NULL,
    price NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    notional NUMERIC NOT NULL,
    order_id TEXT,
    trade_id TEXT,
    tx_index BIGINT NOT NULL,
    realized_pnl NUMERIC,
    liquidation BOOLEAN NOT NULL DEFAULT FALSE,
    liquidation_mark_price NUMERIC,
    liquidation_method TEXT,
    PRIMARY KEY (id, ts)
);
SELECT create_hypertable(
    'orderflow_events', by_range('ts'), if_not_exists => TRUE, migrate_data => TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS orderflow_events_dedupe_idx
ON orderflow_events (
    ts, tx_index, wallet, COALESCE(trade_id, ''), COALESCE(order_id, ''), side
);
CREATE INDEX IF NOT EXISTS orderflow_events_wallet_ts_idx
ON orderflow_events (wallet, ts DESC);
CREATE INDEX IF NOT EXISTS orderflow_events_symbol_ts_idx
ON orderflow_events (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS orderflow_events_order_id_idx
ON orderflow_events (order_id, ts DESC) WHERE order_id IS NOT NULL;

-- Additive nullable direction fields: old rows remain explicitly unclassified.
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS direction TEXT;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS start_position NUMERIC;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS direction_provenance TEXT;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS start_position_provenance TEXT;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS direction_reconstructed_at TIMESTAMPTZ;
ALTER TABLE orderflow_events ADD COLUMN IF NOT EXISTS start_position_reconstructed_at TIMESTAMPTZ;
DO $$
BEGIN
    ALTER TABLE orderflow_events ADD CONSTRAINT orderflow_direction_provenance_check
        CHECK (direction_provenance IS NULL OR direction_provenance IN ('allFills', 'userFillsByTime'))
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$
BEGIN
    ALTER TABLE orderflow_events ADD CONSTRAINT orderflow_start_position_provenance_check
        CHECK (start_position_provenance IS NULL OR start_position_provenance IN ('allFills', 'userFillsByTime'))
        NOT VALID;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS direction_backfill_runs (
    id BIGSERIAL PRIMARY KEY,
    wallet TEXT NOT NULL,
    start_ts TIMESTAMPTZ NOT NULL,
    end_ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL CHECK (source = 'userFillsByTime'),
    source_limitations TEXT NOT NULL,
    fetched_count BIGINT NOT NULL,
    matched_count BIGINT NOT NULL,
    local_candidate_count BIGINT NOT NULL DEFAULT 0,
    unusable_count BIGINT NOT NULL DEFAULT 0,
    rejected_count BIGINT NOT NULL DEFAULT 0,
    coverage NUMERIC,
    source_complete BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    failure_type TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'running';
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS failure_type TEXT;
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS local_candidate_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS unusable_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS rejected_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS coverage NUMERIC;
ALTER TABLE direction_backfill_runs ADD COLUMN IF NOT EXISTS source_complete BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS l2_event_counters (
    symbol TEXT PRIMARY KEY,
    last_event_no BIGINT NOT NULL
);

-- Current reconstruction state is durable so replay can continue across restarts.
CREATE TABLE IF NOT EXISTS l2_book_state (
    symbol TEXT PRIMARY KEY,
    epoch TEXT NOT NULL,
    source_seq BIGINT NOT NULL,
    height BIGINT NOT NULL,
    event_no BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS l2_book_levels (
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('bid', 'ask')),
    price NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    order_count INTEGER NOT NULL,
    PRIMARY KEY (symbol, side, price)
);

-- One row is persisted after each atomic per-coin l2BookDiff is applied.
CREATE TABLE IF NOT EXISTS l2_book_ticks (
    ts TIMESTAMPTZ NOT NULL,
    symbol TEXT NOT NULL,
    height BIGINT NOT NULL,
    epoch TEXT NOT NULL,
    source_seq BIGINT NOT NULL,
    event_no BIGINT NOT NULL,
    best_bid NUMERIC NOT NULL,
    best_ask NUMERIC NOT NULL,
    mid_price NUMERIC NOT NULL,
    spread_bps DOUBLE PRECISION NOT NULL,
    bid_size NUMERIC NOT NULL,
    ask_size NUMERIC NOT NULL,
    imbalance DOUBLE PRECISION NOT NULL,
    trailing_event_volatility DOUBLE PRECISION,
    PRIMARY KEY (ts, symbol, event_no)
);
SELECT create_hypertable(
    'l2_book_ticks', by_range('ts'), if_not_exists => TRUE, migrate_data => TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS l2_book_ticks_symbol_event_idx
ON l2_book_ticks (symbol, event_no, ts);
CREATE INDEX IF NOT EXISTS l2_book_ticks_symbol_ts_idx
ON l2_book_ticks (symbol, ts DESC);

-- Raw L4 lifecycle updates are short-lived; wallet aggregates below are durable.
CREATE TABLE IF NOT EXISTS l4_order_events (
    ts TIMESTAMPTZ NOT NULL,
    height BIGINT NOT NULL,
    event_index INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    order_id TEXT NOT NULL,
    wallet TEXT,
    event_type TEXT NOT NULL,
    side TEXT NOT NULL,
    price NUMERIC,
    size NUMERIC,
    resting_seconds DOUBLE PRECISION,
    PRIMARY KEY (ts, height, event_index)
);
SELECT create_hypertable(
    'l4_order_events', by_range('ts'), if_not_exists => TRUE, migrate_data => TRUE
);
CREATE INDEX IF NOT EXISTS l4_order_events_wallet_ts_idx
ON l4_order_events (wallet, ts DESC) WHERE wallet IS NOT NULL;

CREATE TABLE IF NOT EXISTS wallet_order_aggregates (
    wallet TEXT NOT NULL,
    symbol TEXT NOT NULL,
    new_count BIGINT NOT NULL DEFAULT 0,
    update_count BIGINT NOT NULL DEFAULT 0,
    remove_count BIGINT NOT NULL DEFAULT 0,
    round_size_count BIGINT NOT NULL DEFAULT 0,
    total_resting_seconds DOUBLE PRECISION NOT NULL DEFAULT 0,
    resting_sample_count BIGINT NOT NULL DEFAULT 0,
    first_event_ts TIMESTAMPTZ NOT NULL,
    last_event_ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (wallet, symbol)
);

CREATE TABLE IF NOT EXISTS leverage_updates (
    ts TIMESTAMPTZ NOT NULL,
    wallet TEXT NOT NULL,
    symbol TEXT NOT NULL,
    update_type TEXT NOT NULL,
    leverage DOUBLE PRECISION,
    is_cross BOOLEAN,
    is_buy BOOLEAN,
    ntli NUMERIC,
    tx_index BIGINT NOT NULL,
    PRIMARY KEY (ts, wallet, symbol, tx_index, update_type)
);

CREATE TABLE IF NOT EXISTS wallet_leverage_state (
    wallet TEXT NOT NULL,
    symbol TEXT NOT NULL,
    current_leverage DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    update_count BIGINT NOT NULL DEFAULT 1,
    cross_update_count BIGINT NOT NULL DEFAULT 0,
    isolated_margin_update_count BIGINT NOT NULL DEFAULT 0,
    first_update_ts TIMESTAMPTZ NOT NULL,
    last_update_ts TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (wallet, symbol)
);

-- Matured samples survive the seven-day compact-book retention window.
CREATE TABLE IF NOT EXISTS wallet_markout_samples (
    fill_id BIGINT NOT NULL,
    fill_ts TIMESTAMPTZ NOT NULL,
    wallet TEXT NOT NULL,
    symbol TEXT NOT NULL,
    horizon_events INTEGER NOT NULL,
    elapsed_seconds DOUBLE PRECISION NOT NULL,
    raw_log_markout DOUBLE PRECISION NOT NULL,
    normalized_markout DOUBLE PRECISION NOT NULL,
    volatility_scale DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fill_id, horizon_events)
);
CREATE INDEX IF NOT EXISTS wallet_markout_samples_wallet_horizon_idx
ON wallet_markout_samples (wallet, horizon_events);

CREATE TABLE IF NOT EXISTS wallet_flow_labels (
    wallet TEXT PRIMARY KEY,
    bucket_ts TIMESTAMPTZ NOT NULL,
    trade_count BIGINT NOT NULL,
    taker_ratio DOUBLE PRECISION,
    liquidation_ratio DOUBLE PRECISION,
    round_number_notional_ratio DOUBLE PRECISION,
    round_number_size_ratio DOUBLE PRECISION,
    message_cadence_per_min DOUBLE PRECISION,
    realized_pnl DOUBLE PRECISION,
    max_leverage DOUBLE PRECISION,
    current_max_leverage DOUBLE PRECISION,
    leverage_update_count BIGINT NOT NULL DEFAULT 0,
    isolated_margin_update_count BIGINT NOT NULL DEFAULT 0,
    order_new_count BIGINT NOT NULL DEFAULT 0,
    order_update_count BIGINT NOT NULL DEFAULT 0,
    order_remove_count BIGINT NOT NULL DEFAULT 0,
    order_remove_ratio DOUBLE PRECISION,
    round_order_size_ratio DOUBLE PRECISION,
    average_order_lifetime_seconds DOUBLE PRECISION,
    markout_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
