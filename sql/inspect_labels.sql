\set ON_ERROR_STOP on
\pset pager off
\x auto
\timing on

\echo ''
\echo '=== Label freshness ==='
SELECT
    NOW() AS checked_at,
    (SELECT COUNT(*) FROM orderflow_events) AS fill_rows,
    (SELECT COUNT(*) FROM wallet_flow_labels) AS labeled_wallets,
    (SELECT MAX(bucket_ts) FROM wallet_flow_labels) AS latest_label_snapshot;

\echo ''
\echo '=== Wallets with most normalized 50-event markout samples ==='
SELECT
    wallet,
    trade_count,
    taker_ratio,
    liquidation_ratio,
    max_leverage,
    order_remove_ratio,
    markout_metrics -> '50' AS markout_50,
    markout_metrics -> '100' AS markout_100,
    markout_metrics -> '1000' AS markout_1000,
    bucket_ts
FROM wallet_flow_labels
ORDER BY COALESCE((markout_metrics -> '50' ->> 'sample_count')::bigint, 0) DESC
LIMIT 20;

\echo ''
\echo '=== Candidate toxic taker flow ==='
SELECT
    wallet,
    trade_count,
    taker_ratio,
    liquidation_ratio,
    round_number_size_ratio,
    max_leverage,
    markout_metrics -> '50' ->> 'p50' AS markout_50_p50,
    markout_metrics -> '100' ->> 'p50' AS markout_100_p50,
    markout_metrics -> '1000' ->> 'p50' AS markout_1000_p50
FROM wallet_flow_labels
WHERE trade_count >= 20
  AND taker_ratio >= 0.7
ORDER BY (markout_metrics -> '50' ->> 'p50')::float8 DESC NULLS LAST
LIMIT 20;

\echo ''
\echo '=== Highest leverage wallets ==='
SELECT
    wallet,
    trade_count,
    max_leverage,
    current_max_leverage,
    leverage_update_count,
    liquidation_ratio,
    taker_ratio
FROM wallet_flow_labels
ORDER BY max_leverage DESC NULLS LAST, trade_count DESC
LIMIT 20;
