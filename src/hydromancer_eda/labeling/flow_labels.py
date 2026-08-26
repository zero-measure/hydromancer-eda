"""Compute rolling wallet-flow labels from persisted order-flow events."""

from __future__ import annotations

from datetime import datetime

import asyncpg

from hydromancer_eda.config import Settings
from hydromancer_eda.models import WalletLabelSnapshot


class FlowLabeler:
    """Compute behavioral and performance features for observed wallets."""

    def __init__(self, settings: Settings) -> None:
        """Initialize the labeler with aggregation and markout settings.

        Args:
            settings (Settings): Label window and markout horizon configuration.

        """
        self._settings = settings

    async def compute_snapshots(self, pool: asyncpg.Pool, as_of: datetime) -> list[WalletLabelSnapshot]:
        """Compute rolling wallet metrics as of a timestamp.

        The query uses ``label_agg_window_hours`` for the observation window and
        ``label_markout_horizon_seconds`` for post-trade price selection. A
        notional is considered round when it is exactly divisible by at least
        one of $10, $100, $1,000, or $10,000. Token size is considered round
        when it is an exact multiple of 0.1 tokens, which includes whole-token
        sizes.

        Args:
            pool (asyncpg.Pool): Connected PostgreSQL pool containing events.
            as_of (datetime): Timezone-aware upper bound and snapshot timestamp.

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
        recent AS (
            SELECT *
            FROM orderflow_events
            WHERE ts >= $1::timestamptz - make_interval(hours => $2::int)
              AND ts <= $1::timestamptz
        ),
        base AS (
            SELECT
                wallet,
                SUM(COALESCE(realized_pnl, 0))::float8 AS realized_pnl,
                COUNT(*) FILTER (WHERE liquidation) AS liquidation_count,
                (COUNT(*) FILTER (WHERE aggressor = 'taker'))::float8 / NULLIF(COUNT(*) FILTER (WHERE event_type IN ('trade', 'fill')), 0) AS taker_ratio,
                (COUNT(*) FILTER (WHERE event_type = 'cancel'))::float8 / NULLIF(COUNT(*) FILTER (WHERE event_type = 'fill'), 0) AS cancel_to_fill_ratio,
                (COUNT(*))::float8 / GREATEST(($2::float8 * 60.0), 1.0) AS message_cadence_per_min,
                (SUM(COALESCE(notional,0)) FILTER (WHERE aggressor='maker'))::float8 / NULLIF((SUM(COALESCE(notional,0)) FILTER (WHERE aggressor IN ('maker','taker'))), 0)::float8 AS liquidity_provision_score,
                (COUNT(*) FILTER (
                    WHERE notional IS NOT NULL
                      AND EXISTS (
                          SELECT 1
                          FROM round_notional_moduli r
                          WHERE MOD(ABS(notional), r.modulus) = 0
                      )
                ))::float8 / NULLIF(COUNT(*) FILTER (WHERE notional IS NOT NULL), 0) AS round_number_notional_ratio,
                (COUNT(*) FILTER (
                    WHERE size IS NOT NULL
                      AND MOD(ABS(size), 0.1::numeric) = 0
                ))::float8 / NULLIF(COUNT(*) FILTER (WHERE size IS NOT NULL), 0) AS round_number_size_ratio,
                MAX(leverage)::float8 AS max_leverage,
                (COUNT(*) FILTER (WHERE margin_mode = 'cross'))::float8 / NULLIF(COUNT(*) FILTER (WHERE leverage IS NOT NULL), 0) AS cross_margin_ratio
            FROM recent
            GROUP BY wallet
        ),
        taker_markout AS (
            SELECT
                t.wallet,
                AVG(
                    CASE
                        WHEN t.side = 'buy' THEN ((f.price - t.price) / NULLIF(t.price, 0))
                        WHEN t.side = 'sell' THEN ((t.price - f.price) / NULLIF(t.price, 0))
                        ELSE NULL
                    END
                )::float8 AS taker_markout
            FROM recent t
            JOIN LATERAL (
                SELECT n.price
                FROM orderflow_events n
                WHERE n.symbol = t.symbol
                  AND n.ts >= t.ts + ($3::int || ' seconds')::interval
                  AND n.price IS NOT NULL
                ORDER BY n.ts ASC
                LIMIT 1
            ) f ON TRUE
            WHERE t.aggressor = 'taker'
              AND t.event_type IN ('trade', 'fill')
              AND t.price IS NOT NULL
            GROUP BY t.wallet
        ),
        holding AS (
            SELECT
                o.wallet,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (c.ts - o.ts)))::float8 AS holding_time_sec_median
            FROM recent o
            JOIN recent c
              ON o.wallet = c.wallet
             AND o.position_id IS NOT NULL
             AND c.position_id = o.position_id
             AND o.event_type = 'position_open'
             AND c.event_type = 'position_close'
             AND c.ts >= o.ts
            GROUP BY o.wallet
        )
        SELECT
            $1::timestamptz AS bucket_ts,
            b.wallet,
            m.taker_markout,
            b.realized_pnl,
            b.liquidation_count,
            b.taker_ratio,
            b.cancel_to_fill_ratio,
            b.message_cadence_per_min,
            h.holding_time_sec_median,
            b.liquidity_provision_score,
            b.round_number_notional_ratio,
            b.round_number_size_ratio,
            b.max_leverage,
            b.cross_margin_ratio
        FROM base b
        LEFT JOIN taker_markout m ON m.wallet = b.wallet
        LEFT JOIN holding h ON h.wallet = b.wallet
        """

        rows = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                query,
                as_of,
                self._settings.label_agg_window_hours,
                self._settings.label_markout_horizon_seconds,
            )

        return [WalletLabelSnapshot.model_validate(dict(r)) for r in rows]
