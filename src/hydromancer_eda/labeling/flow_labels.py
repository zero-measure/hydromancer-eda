"""Compute observed-lifetime wallet-flow labels from persisted events."""

from __future__ import annotations

from datetime import datetime

import asyncpg

from hydromancer_eda.config import Settings
from hydromancer_eda.models import WalletLabelSnapshot


class FlowLabeler:
    """Compute behavioral and performance features for observed wallets."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the labeler with markout settings.

        Args:
            settings (Settings): Markout horizon configuration.

        """
        self._settings = settings

    async def compute_snapshots(self, pool: asyncpg.Pool, as_of: datetime) -> list[WalletLabelSnapshot]:
        """Compute observed-lifetime wallet metrics as of a timestamp.

        Taker markouts use ``label_markout_horizon_seconds`` for post-trade
        price selection. A notional is considered round when it is exactly
        divisible by at least one of $10, $100, $1,000, or $10,000. Token size
        is considered round when it is an exact multiple of 0.1 tokens, which
        includes whole-token sizes.

        Args:
            pool (asyncpg.Pool): Connected PostgreSQL pool containing events.
            as_of (datetime): Timezone-aware snapshot cutoff and timestamp.

        Returns:
            list[WalletLabelSnapshot]: One validated snapshot per observed wallet.

        Raises:
            asyncpg.PostgresError: If the analytics query fails.
            pydantic.ValidationError: If a query row cannot form a snapshot.

        """
        query = """
        WITH round_notional_moduli(modulus) AS (
            VALUES
                (10::numeric),
                (100::numeric),
                (1000::numeric),
                (10000::numeric)
        ),
        observed AS (
            SELECT *
            FROM orderflow_events
            WHERE ts <= $1::timestamptz
        ),
        base AS (
            SELECT
                wallet,
                COUNT(*) FILTER (
                    WHERE event_type IN ('trade', 'fill', 'liquidation')
                ) AS trade_count,
                SUM(COALESCE(realized_pnl, 0))::float8 AS realized_pnl,
                (COUNT(*) FILTER (
                    WHERE liquidation
                      AND event_type IN ('trade', 'fill', 'liquidation')
                ))::float8
                    / NULLIF(COUNT(*) FILTER (
                        WHERE event_type IN ('trade', 'fill', 'liquidation')
                    ), 0) AS liquidation_ratio,
                (COUNT(*) FILTER (
                    WHERE aggressor = 'taker'
                      AND event_type IN ('trade', 'fill', 'liquidation')
                ))::float8
                    / NULLIF(COUNT(*) FILTER (
                        WHERE event_type IN ('trade', 'fill', 'liquidation')
                    ), 0) AS taker_ratio,
                (COUNT(*) FILTER (WHERE event_type = 'cancel'))::float8
                    / NULLIF(COUNT(*) FILTER (WHERE event_type = 'fill'), 0)
                    AS cancel_to_fill_ratio,
                COUNT(*)::float8
                    / NULLIF(EXTRACT(EPOCH FROM (MAX(ts) - MIN(ts))) / 60.0, 0)
                    AS message_cadence_per_min,
                (SUM(COALESCE(notional, 0)) FILTER (WHERE aggressor = 'maker'))::float8
                    / NULLIF((SUM(COALESCE(notional, 0)) FILTER (
                        WHERE aggressor IN ('maker', 'taker')
                    )), 0)::float8 AS liquidity_provision_score,
                (COUNT(*) FILTER (
                    WHERE notional IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM round_notional_moduli r
                          WHERE MOD(ABS(notional), r.modulus) = 0
                      )
                ))::float8
                    / NULLIF(COUNT(*) FILTER (WHERE notional IS NOT NULL), 0)
                    AS round_number_notional_ratio,
                (COUNT(*) FILTER (
                    WHERE size IS NOT NULL
                      AND MOD(ABS(size), 0.1::numeric) = 0
                ))::float8
                    / NULLIF(COUNT(*) FILTER (WHERE size IS NOT NULL), 0)
                    AS round_number_size_ratio,
                MAX(leverage)::float8 AS max_leverage,
                (COUNT(*) FILTER (
                    WHERE margin_mode = 'cross' AND leverage IS NOT NULL
                ))::float8
                    / NULLIF(COUNT(*) FILTER (WHERE leverage IS NOT NULL), 0)
                    AS cross_margin_ratio
            FROM observed
            GROUP BY wallet
        ),
        taker_markout_samples AS (
            SELECT
                t.wallet,
                CASE
                    WHEN t.side = 'buy'
                        THEN ((future.price - t.price) / NULLIF(t.price, 0))::float8
                    WHEN t.side = 'sell'
                        THEN ((t.price - future.price) / NULLIF(t.price, 0))::float8
                    ELSE NULL
                END AS markout
            FROM observed t
            JOIN LATERAL (
                SELECT next_event.price
                FROM orderflow_events next_event
                WHERE next_event.symbol = t.symbol
                  AND next_event.ts >= t.ts + ($2::int || ' seconds')::interval
                  AND next_event.ts <= $1::timestamptz
                  AND next_event.price IS NOT NULL
                ORDER BY next_event.ts ASC, next_event.id ASC
                LIMIT 1
            ) future ON TRUE
            WHERE t.aggressor = 'taker'
              AND t.event_type IN ('trade', 'fill', 'liquidation')
              AND t.price IS NOT NULL
              AND t.price <> 0
              AND t.side IN ('buy', 'sell')
        ),
        taker_markout AS (
            SELECT
                wallet,
                COUNT(*) AS markout_sample_count,
                percentile_cont(0.1) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p10,
                percentile_cont(0.2) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p20,
                percentile_cont(0.3) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p30,
                percentile_cont(0.4) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p40,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p50,
                percentile_cont(0.6) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p60,
                percentile_cont(0.7) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p70,
                percentile_cont(0.8) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p80,
                percentile_cont(0.9) WITHIN GROUP (ORDER BY markout)::float8 AS markout_p90
            FROM taker_markout_samples
            WHERE markout IS NOT NULL
            GROUP BY wallet
        ),
        holding AS (
            SELECT
                opened.wallet,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (closed.ts - opened.ts))
                )::float8 AS holding_time_sec_median
            FROM observed opened
            JOIN observed closed
              ON opened.wallet = closed.wallet
             AND opened.position_id IS NOT NULL
             AND closed.position_id = opened.position_id
             AND opened.event_type = 'position_open'
             AND closed.event_type = 'position_close'
             AND closed.ts >= opened.ts
            GROUP BY opened.wallet
        )
        SELECT
            $1::timestamptz AS bucket_ts,
            base.wallet,
            base.trade_count,
            markout.markout_sample_count,
            markout.markout_p10,
            markout.markout_p20,
            markout.markout_p30,
            markout.markout_p40,
            markout.markout_p50,
            markout.markout_p60,
            markout.markout_p70,
            markout.markout_p80,
            markout.markout_p90,
            base.realized_pnl,
            base.liquidation_ratio,
            base.taker_ratio,
            base.cancel_to_fill_ratio,
            base.message_cadence_per_min,
            holding.holding_time_sec_median,
            base.liquidity_provision_score,
            base.round_number_notional_ratio,
            base.round_number_size_ratio,
            base.max_leverage,
            base.cross_margin_ratio
        FROM base
        LEFT JOIN taker_markout markout ON markout.wallet = base.wallet
        LEFT JOIN holding ON holding.wallet = base.wallet
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                as_of,
                self._settings.label_markout_horizon_seconds,
            )

        return [WalletLabelSnapshot.model_validate(dict(row)) for row in rows]
