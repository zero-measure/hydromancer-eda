-- Live ingestion inspection queries for hydromancer-eda.
--
-- Run every query once from the repository root:
--   docker exec -i hydromancer-timescaledb \
--     psql --username postgres --dbname hydromancer \
--     < sql/inspect_live.sql
--
-- For interactive use, connect with psql, paste an individual query, and then
-- enter `\watch 2` to repeat it every two seconds. Press Ctrl-C to stop watching.

\pset pager off
\timing on

\echo ''
\echo '=== Tables ==='
\dt

\echo ''
\echo '=== orderflow_events definition ==='
\d orderflow_events

\echo ''
\echo '=== Overall ingestion progress ==='
SELECT
    NOW() AS observed_at,
    COUNT(*) AS total_rows,
    MAX(ts) AS latest_event,
    NOW() - MAX(ts) AS event_lag
FROM orderflow_events;

\echo ''
\echo '=== Latest normalized participant rows ==='
SELECT
    ts,
    symbol,
    wallet,
    event_type,
    side,
    aggressor,
    price,
    size,
    notional,
    trade_id,
    order_id,
    tx_index
FROM orderflow_events
ORDER BY ts DESC, tx_index DESC
LIMIT 20;

\echo ''
\echo '=== Participant rows grouped by transaction identity ==='
SELECT
    ts,
    tx_index,
    trade_id,
    symbol,
    COUNT(*) AS participant_rows,
    COUNT(DISTINCT wallet) AS distinct_wallets,
    ARRAY_AGG(wallet ORDER BY wallet) AS wallets
FROM orderflow_events
GROUP BY ts, tx_index, trade_id, symbol
ORDER BY ts DESC, tx_index DESC
LIMIT 20;

\echo ''
\echo '=== Durable WebSocket checkpoint ==='
SELECT
    channel,
    session_id,
    cursor,
    updated_at,
    NOW() - updated_at AS checkpoint_age
FROM ingestion_checkpoints;

\echo ''
\echo '=== Latest quarantined malformed entries ==='
SELECT
    id,
    channel,
    cursor,
    reason,
    created_at,
    raw
FROM ingestion_rejects
ORDER BY created_at DESC
LIMIT 20;

\echo ''
\echo '=== Reject count ==='
SELECT COUNT(*) AS rejected_rows
FROM ingestion_rejects;

\echo ''
\echo '=== Latest raw upstream payload ==='
SELECT
    ts,
    wallet,
    jsonb_pretty(raw) AS raw_payload
FROM orderflow_events
ORDER BY ts DESC, tx_index DESC
LIMIT 1;

\echo ''
\echo '=== Ingestion volume by symbol over the last minute ==='
SELECT
    symbol,
    COUNT(*) AS rows_last_minute,
    MAX(ts) AS latest_event
FROM orderflow_events
WHERE ts >= NOW() - INTERVAL '1 minute'
GROUP BY symbol
ORDER BY symbol;

\echo ''
\echo '=== Wallet activity and latest labels over the last 24 hours ==='
WITH activity_24h AS (
    SELECT
        wallet,
        COUNT(*) AS event_count_24h,
        COUNT(*) FILTER (
            WHERE aggressor = 'taker'
              AND event_type IN ('trade', 'fill')
        ) AS taker_event_count_24h,
        COUNT(*) FILTER (WHERE liquidation) AS liquidation_count_24h,
        (COUNT(*) FILTER (WHERE liquidation))::float8
            / NULLIF(COUNT(*), 0) AS liquidation_frequency_24h,
        MIN(ts) AS first_event_24h,
        MAX(ts) AS latest_event_24h
    FROM orderflow_events
    WHERE ts >= NOW() - INTERVAL '24 hours'
    GROUP BY wallet
),
selected_wallets AS (
    (SELECT wallet, 'most liquidations'::text AS selection
     FROM activity_24h
     ORDER BY liquidation_count_24h DESC, event_count_24h DESC
     LIMIT 10)
    UNION
    (SELECT wallet, 'most taker activity'::text AS selection
     FROM activity_24h
     ORDER BY taker_event_count_24h DESC, event_count_24h DESC
     LIMIT 10)
)
SELECT
    selected.selection,
    labels.bucket_ts,
    activity.wallet,
    activity.event_count_24h,
    activity.taker_event_count_24h,
    activity.liquidation_count_24h,
    activity.liquidation_frequency_24h,
    activity.first_event_24h,
    activity.latest_event_24h,
    labels.taker_markout,
    labels.realized_pnl,
    labels.taker_ratio,
    labels.cancel_to_fill_ratio,
    labels.message_cadence_per_min,
    labels.holding_time_sec_median,
    labels.liquidity_provision_score,
    labels.round_number_notional_ratio,
    labels.round_number_size_ratio,
    labels.max_leverage,
    labels.cross_margin_ratio
FROM selected_wallets selected
JOIN activity_24h activity USING (wallet)
JOIN LATERAL (
    SELECT latest.*
    FROM wallet_flow_labels latest
    WHERE latest.wallet = activity.wallet
    ORDER BY latest.bucket_ts DESC
    LIMIT 1
) labels ON TRUE
ORDER BY
    selected.selection,
    activity.liquidation_count_24h DESC,
    activity.taker_event_count_24h DESC;

\echo ''
\echo '=== TimescaleDB hypertables ==='
SELECT
    hypertable_name,
    num_dimensions,
    num_chunks
FROM timescaledb_information.hypertables
ORDER BY hypertable_name;

\echo ''
\echo '=== Combined ingestion health ==='
SELECT
    (SELECT COUNT(*) FROM orderflow_events) AS events,
    (SELECT MAX(ts) FROM orderflow_events) AS latest_event,
    (SELECT COUNT(*) FROM ingestion_rejects) AS rejects,
    (SELECT MAX(updated_at) FROM ingestion_checkpoints) AS latest_checkpoint,
    (SELECT MAX(bucket_ts) FROM wallet_flow_labels) AS latest_label_snapshot;
