"""Compute toxic-flow wallet labels from executions and book-event outcomes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import asyncpg

from hydromancer_eda.config import Settings
from hydromancer_eda.models import WalletLabelSnapshot


class FlowLabeler:
    """Materialize event-time markouts and compute current wallet features."""

    def __init__(self, settings: Settings) -> None:
        """Store labeling configuration."""
        self._settings = settings

    async def materialize_markout_samples(
        self, pool: asyncpg.Pool, as_of: datetime
    ) -> int:
        """Persist newly matured 50/100/1000-book-event markout samples."""
        query = """
        WITH horizons(horizon_events, max_elapsed_seconds) AS (
            VALUES (50, 300), (100, 1800), (1000, 3600)
        ),
        candidates AS (
            SELECT
                fill.id,
                fill.ts,
                fill.wallet,
                fill.symbol,
                fill.side,
                fill.price,
                base.event_no,
                base.epoch,
                base.trailing_event_volatility
            FROM orderflow_events fill
            JOIN LATERAL (
                SELECT tick.event_no, tick.epoch, tick.trailing_event_volatility
                FROM l2_book_ticks tick
                WHERE tick.symbol = fill.symbol
                  AND tick.ts < fill.ts
                ORDER BY tick.ts DESC, tick.event_no DESC
                LIMIT 1
            ) base ON TRUE
            WHERE fill.ts <= $1::timestamptz
              AND fill.aggressor = 'taker'
              AND fill.side IN ('buy', 'sell')
              AND fill.price > 0
              AND base.trailing_event_volatility > 0
        )
        INSERT INTO wallet_markout_samples (
            fill_id, fill_ts, wallet, symbol, horizon_events, elapsed_seconds,
            raw_log_markout, normalized_markout, volatility_scale
        )
        SELECT
            candidate.id,
            candidate.ts,
            candidate.wallet,
            candidate.symbol,
            horizon.horizon_events,
            EXTRACT(EPOCH FROM (future.ts - candidate.ts))::float8,
            signed.raw_markout,
            signed.raw_markout / (
                candidate.trailing_event_volatility * SQRT(horizon.horizon_events::float8)
            ),
            candidate.trailing_event_volatility
        FROM candidates candidate
        CROSS JOIN horizons horizon
        JOIN l2_book_ticks future
          ON future.symbol = candidate.symbol
         AND future.epoch = candidate.epoch
         AND future.event_no = candidate.event_no + horizon.horizon_events
        CROSS JOIN LATERAL (
            SELECT CASE
                WHEN candidate.side = 'buy'
                    THEN LN((future.mid_price / candidate.price)::float8)
                ELSE LN((candidate.price / future.mid_price)::float8)
            END AS raw_markout
        ) signed
        WHERE future.ts <= $1::timestamptz
          AND NOT EXISTS (
              SELECT 1
              FROM wallet_markout_samples existing
              WHERE existing.fill_id = candidate.id
                AND existing.horizon_events = horizon.horizon_events
          )
          AND EXTRACT(EPOCH FROM (future.ts - candidate.ts))
              BETWEEN 0 AND horizon.max_elapsed_seconds
        ON CONFLICT (fill_id, horizon_events) DO NOTHING
        """
        async with pool.acquire() as conn:
            result = await conn.execute(query, as_of)
        return int(result.rsplit(" ", 1)[-1])

    async def compute_snapshots(
        self, pool: asyncpg.Pool, as_of: datetime
    ) -> list[WalletLabelSnapshot]:
        """Compute one current classifier feature row per observed wallet."""
        await self.materialize_markout_samples(pool, as_of)
        query = """
        WITH base AS (
            SELECT
                wallet,
                COUNT(*) AS trade_count,
                (COUNT(*) FILTER (WHERE aggressor = 'taker'))::float8
                    / NULLIF(COUNT(*), 0) AS taker_ratio,
                (COUNT(*) FILTER (WHERE liquidation))::float8
                    / NULLIF(COUNT(*), 0) AS liquidation_ratio,
                (COUNT(*) FILTER (
                    WHERE MOD(ABS(notional), 10::numeric) = 0
                ))::float8 / NULLIF(COUNT(*), 0) AS round_number_notional_ratio,
                (COUNT(*) FILTER (
                    WHERE MOD(ABS(size), 0.1::numeric) = 0
                ))::float8 / NULLIF(COUNT(*), 0) AS round_number_size_ratio,
                COUNT(*)::float8 / NULLIF(
                    EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 60.0, 0
                ) AS message_cadence_per_min,
                SUM(COALESCE(realized_pnl, 0))::float8 AS realized_pnl
            FROM orderflow_events
            WHERE ts <= $1::timestamptz
            GROUP BY wallet
        ),
        markout_horizon AS (
            SELECT
                wallet,
                horizon_events,
                COUNT(*) AS sample_count,
                percentile_cont(0.1) WITHIN GROUP (ORDER BY normalized_markout) AS p10,
                percentile_cont(0.2) WITHIN GROUP (ORDER BY normalized_markout) AS p20,
                percentile_cont(0.3) WITHIN GROUP (ORDER BY normalized_markout) AS p30,
                percentile_cont(0.4) WITHIN GROUP (ORDER BY normalized_markout) AS p40,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY normalized_markout) AS p50,
                percentile_cont(0.6) WITHIN GROUP (ORDER BY normalized_markout) AS p60,
                percentile_cont(0.7) WITHIN GROUP (ORDER BY normalized_markout) AS p70,
                percentile_cont(0.8) WITHIN GROUP (ORDER BY normalized_markout) AS p80,
                percentile_cont(0.9) WITHIN GROUP (ORDER BY normalized_markout) AS p90,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY elapsed_seconds)
                    AS median_elapsed_seconds
            FROM wallet_markout_samples
            WHERE fill_ts <= $1::timestamptz
            GROUP BY wallet, horizon_events
        ),
        markout AS (
            SELECT
                wallet,
                jsonb_object_agg(
                    horizon_events::text,
                    jsonb_build_object(
                        'sample_count', sample_count,
                        'p10', p10, 'p20', p20, 'p30', p30,
                        'p40', p40, 'p50', p50, 'p60', p60,
                        'p70', p70, 'p80', p80, 'p90', p90,
                        'median_elapsed_seconds', median_elapsed_seconds
                    )
                ) AS metrics
            FROM markout_horizon
            GROUP BY wallet
        ),
        leverage AS (
            SELECT
                wallet,
                MAX(max_leverage) AS max_leverage,
                MAX(current_leverage) AS current_max_leverage,
                SUM(update_count) AS leverage_update_count,
                SUM(isolated_margin_update_count) AS isolated_margin_update_count
            FROM wallet_leverage_state
            GROUP BY wallet
        ),
        orders AS (
            SELECT
                wallet,
                SUM(new_count) AS order_new_count,
                SUM(update_count) AS order_update_count,
                SUM(remove_count) AS order_remove_count,
                SUM(remove_count)::float8 / NULLIF(SUM(new_count), 0)
                    AS order_remove_ratio,
                SUM(round_size_count)::float8 / NULLIF(SUM(new_count), 0)
                    AS round_order_size_ratio,
                SUM(total_resting_seconds) / NULLIF(SUM(resting_sample_count), 0)
                    AS average_order_lifetime_seconds
            FROM wallet_order_aggregates
            GROUP BY wallet
        )
        SELECT
            $1::timestamptz AS bucket_ts,
            base.wallet,
            base.trade_count,
            base.taker_ratio,
            base.liquidation_ratio,
            base.round_number_notional_ratio,
            base.round_number_size_ratio,
            base.message_cadence_per_min,
            base.realized_pnl,
            leverage.max_leverage,
            leverage.current_max_leverage,
            COALESCE(leverage.leverage_update_count, 0) AS leverage_update_count,
            COALESCE(leverage.isolated_margin_update_count, 0)
                AS isolated_margin_update_count,
            COALESCE(orders.order_new_count, 0) AS order_new_count,
            COALESCE(orders.order_update_count, 0) AS order_update_count,
            COALESCE(orders.order_remove_count, 0) AS order_remove_count,
            orders.order_remove_ratio,
            orders.round_order_size_ratio,
            orders.average_order_lifetime_seconds,
            COALESCE(markout.metrics, '{}'::jsonb)::text AS markout_metrics
        FROM base
        LEFT JOIN markout ON markout.wallet = base.wallet
        LEFT JOIN leverage ON leverage.wallet = base.wallet
        LEFT JOIN orders ON orders.wallet = base.wallet
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, as_of)
        snapshots: list[WalletLabelSnapshot] = []
        for row in rows:
            values: dict[str, Any] = dict(row)
            metrics = values.get("markout_metrics")
            if isinstance(metrics, str):
                values["markout_metrics"] = json.loads(metrics)
            snapshots.append(WalletLabelSnapshot.model_validate(values))
        return snapshots
