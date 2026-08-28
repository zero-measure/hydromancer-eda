\set ON_ERROR_STOP on
\pset pager off
\x auto
\timing on

\echo ''
\echo '=== Safe ingestion health (no sessions or cursors) ==='
SELECT
    channel,
    updated_at
FROM ingestion_checkpoints
ORDER BY channel;

\echo ''
\echo '=== Analytical row counts and freshness ==='
SELECT
    (SELECT COUNT(*) FROM orderflow_events) AS fills,
    (SELECT MAX(ts) FROM orderflow_events) AS latest_fill,
    (SELECT COUNT(*) FROM l2_book_ticks) AS compact_l2_ticks,
    (SELECT MAX(ts) FROM l2_book_ticks) AS latest_l2_tick,
    (SELECT COUNT(*) FROM l4_order_events) AS retained_l4_events,
    (SELECT MAX(ts) FROM l4_order_events) AS latest_l4_event,
    (SELECT COUNT(*) FROM leverage_updates) AS leverage_updates,
    (SELECT MAX(ts) FROM leverage_updates) AS latest_leverage_update,
    (SELECT COUNT(*) FROM wallet_flow_labels) AS labeled_wallets,
    (SELECT MAX(bucket_ts) FROM wallet_flow_labels) AS latest_label_snapshot;

\echo ''
\echo '=== Latest participant fills ==='
SELECT
    ts,
    symbol,
    wallet,
    side,
    aggressor,
    price,
    size,
    notional,
    realized_pnl,
    liquidation,
    liquidation_mark_price,
    liquidation_method
FROM orderflow_events
ORDER BY ts DESC, id DESC
LIMIT 20;

\echo ''
\echo '=== Compact L2 state by coin ==='
SELECT DISTINCT ON (symbol)
    symbol,
    ts,
    event_no,
    best_bid,
    best_ask,
    mid_price,
    spread_bps,
    imbalance,
    trailing_event_volatility
FROM l2_book_ticks
ORDER BY symbol, event_no DESC;

\echo ''
\echo '=== Reject counts by channel ==='
SELECT channel, COUNT(*) AS rejected_rows, MAX(created_at) AS latest_reject
FROM ingestion_rejects
GROUP BY channel
ORDER BY channel;
