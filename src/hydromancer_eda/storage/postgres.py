"""PostgreSQL persistence for events, checkpoints, rejects, and labels."""

from __future__ import annotations

import json
from datetime import datetime
from importlib.resources import files

import asyncpg

from hydromancer_eda.config import Settings
from hydromancer_eda.models import OrderflowEvent, WalletLabelSnapshot


class PostgresStore:
    """Persist ingestion state and analytics data in PostgreSQL.

    Call :meth:`connect` before using methods that access the connection pool.
    Such methods raise ``RuntimeError`` while the store is disconnected.
    """

    def __init__(self, settings: Settings) -> None:
        """Initialize a disconnected store.

        Args:
            settings (Settings): Application settings containing the PostgreSQL DSN.

        """
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        """Create the asynchronous PostgreSQL connection pool.

        Raises:
            asyncpg.PostgresError: If the pool cannot connect to PostgreSQL.

        """
        self._pool = await asyncpg.create_pool(self._settings.pg_dsn, min_size=1, max_size=8)

    async def close(self) -> None:
        """Close the connection pool if the store is connected."""
        if self._pool:
            await self._pool.close()

    async def init_schema(self) -> None:
        """Apply the packaged idempotent database schema.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If schema execution fails.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")

        schema_sql = files("hydromancer_eda.sql").joinpath("schema.sql").read_text(encoding="utf-8")
        async with self._pool.acquire() as conn:
            await conn.execute(schema_sql)

    async def insert_events(self, events: list[OrderflowEvent]) -> int:
        """Submit normalized events for conflict-tolerant insertion.

        Args:
            events (list[OrderflowEvent]): Events to persist.

        Returns:
            int: Number of rows submitted, including rows skipped on conflict.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If insertion fails.

        """
        if not events:
            return 0
        if not self._pool:
            raise RuntimeError("Pool not initialized")

        query = """
        INSERT INTO orderflow_events (
            ts, symbol, wallet, event_type, side, aggressor,
            price, size, notional, leverage, margin_mode,
            order_id, trade_id, position_id, tx_index, realized_pnl, liquidation, raw
        ) VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11,
            $12, $13, $14, $15, $16, $17, $18::jsonb
        )
        ON CONFLICT DO NOTHING
        """

        rows = [
            (
                e.ts,
                e.symbol,
                e.wallet,
                e.event_type,
                e.side,
                e.aggressor,
                e.price,
                e.size,
                e.notional,
                e.leverage,
                e.margin_mode,
                e.order_id,
                e.trade_id,
                e.position_id,
                e.tx_index,
                e.realized_pnl,
                e.liquidation,
                json.dumps(e.raw, default=str),
            )
            for e in events
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(query, rows)
        return len(rows)

    async def load_checkpoint(self, channel: str = "allFills") -> tuple[str | None, str | None]:
        """Load the persisted session and opaque cursor for a channel.

        Args:
            channel (str): WebSocket channel checkpoint to load.

        Returns:
            tuple[str | None, str | None]: Persisted ``(session_id, cursor)``;
            both values are ``None`` when no checkpoint exists.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If the query fails.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT session_id, cursor FROM ingestion_checkpoints WHERE channel = $1", channel
            )
        return (None, None) if row is None else (row["session_id"], row["cursor"])

    async def persist_allfills_frame(
        self,
        events: list[OrderflowEvent],
        session_id: str | None,
        cursor: str | None,
        channel: str = "allFills",
        rejected_entries: list[dict[str, object]] | None = None,
    ) -> int:
        """Persist one allFills frame and checkpoint in a transaction.

        Valid events are inserted conflict-tolerantly, malformed entries are
        quarantined, and the opaque cursor advances atomically. Cursor-only and
        duplicate-only frames therefore still represent successful progress.

        Args:
            events (list[OrderflowEvent]): Valid normalized frame events.
            session_id (str | None): Current Hydromancer session identifier.
            cursor (str | None): Opaque cursor associated with the frame.
            channel (str): WebSocket channel checkpoint to update.
            rejected_entries (list[dict[str, object]] | None): Malformed entries
                and their rejection reasons.

        Returns:
            int: Number of valid event rows submitted, including conflicts.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If any transactional operation fails.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        insert = """
        INSERT INTO orderflow_events (ts, symbol, wallet, event_type, side, aggressor,
            price, size, notional, leverage, margin_mode, order_id, trade_id,
            position_id, tx_index, realized_pnl, liquidation, raw)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb)
        ON CONFLICT DO NOTHING
        """
        rows = [(e.ts, e.symbol, e.wallet, e.event_type, e.side, e.aggressor,
                 e.price, e.size, e.notional, e.leverage, e.margin_mode, e.order_id,
                 e.trade_id, e.position_id, e.tx_index, e.realized_pnl, e.liquidation,
                 json.dumps(e.raw, default=str)) for e in events]
        quarantine = """
        INSERT INTO ingestion_rejects (channel, cursor, reason, raw)
        VALUES ($1, $2, $3, $4::jsonb)
        """
        reject_rows = [
            (
                channel,
                cursor,
                str(rejected.get("reason", "unknown")),
                json.dumps(rejected.get("entry"), default=str),
            )
            for rejected in (rejected_entries or [])
        ]
        checkpoint = """
        INSERT INTO ingestion_checkpoints (channel, session_id, cursor, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (channel) DO UPDATE SET
            session_id = EXCLUDED.session_id, cursor = EXCLUDED.cursor, updated_at = NOW()
        """
        async with self._pool.acquire() as conn, conn.transaction():
            if rows:
                await conn.executemany(insert, rows)
            if reject_rows:
                await conn.executemany(quarantine, reject_rows)
            await conn.execute(checkpoint, channel, session_id, cursor)
        return len(rows)

    async def upsert_label_snapshots(self, snapshots: list[WalletLabelSnapshot]) -> int:
        """Upsert wallet label snapshots by timestamp and wallet.

        Args:
            snapshots (list[WalletLabelSnapshot]): Computed snapshots to persist.

        Returns:
            int: Number of rows submitted for upsert.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If the upsert fails.

        """
        if not snapshots:
            return 0
        if not self._pool:
            raise RuntimeError("Pool not initialized")

        query = """
        INSERT INTO wallet_flow_labels (
            bucket_ts, wallet, taker_markout, realized_pnl,
            liquidation_count, taker_ratio, cancel_to_fill_ratio,
            message_cadence_per_min, holding_time_sec_median,
            liquidity_provision_score, round_number_notional_ratio,
            max_leverage, cross_margin_ratio, updated_at
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12, $13, NOW()
        )
        ON CONFLICT (bucket_ts, wallet) DO UPDATE SET
            taker_markout = EXCLUDED.taker_markout,
            realized_pnl = EXCLUDED.realized_pnl,
            liquidation_count = EXCLUDED.liquidation_count,
            taker_ratio = EXCLUDED.taker_ratio,
            cancel_to_fill_ratio = EXCLUDED.cancel_to_fill_ratio,
            message_cadence_per_min = EXCLUDED.message_cadence_per_min,
            holding_time_sec_median = EXCLUDED.holding_time_sec_median,
            liquidity_provision_score = EXCLUDED.liquidity_provision_score,
            round_number_notional_ratio = EXCLUDED.round_number_notional_ratio,
            max_leverage = EXCLUDED.max_leverage,
            cross_margin_ratio = EXCLUDED.cross_margin_ratio,
            updated_at = NOW()
        """

        rows = [
            (
                s.bucket_ts,
                s.wallet,
                s.taker_markout,
                s.realized_pnl,
                s.liquidation_count,
                s.taker_ratio,
                s.cancel_to_fill_ratio,
                s.message_cadence_per_min,
                s.holding_time_sec_median,
                s.liquidity_provision_score,
                s.round_number_notional_ratio,
                s.max_leverage,
                s.cross_margin_ratio,
            )
            for s in snapshots
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(query, rows)
        return len(rows)

    @property
    def pool(self) -> asyncpg.Pool:
        """Return the initialized connection pool.

        Returns:
            asyncpg.Pool: Active PostgreSQL connection pool.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        return self._pool

    async def latest_event_timestamp(self, symbols: list[str]) -> datetime | None:
        """Return a common event watermark for configured symbols.

        A newly configured symbol yields ``None`` instead of inheriting an
        unrelated global watermark.

        Args:
            symbols (list[str]): Symbols that must each have at least one event.

        Returns:
            datetime | None: Earliest per-symbol latest timestamp, the global
            latest timestamp for an empty list, or ``None`` for unseen symbols.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If the query fails.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")

        async with self._pool.acquire() as conn:
            if not symbols:
                return await conn.fetchval("SELECT MAX(ts) FROM orderflow_events")
            rows = await conn.fetch(
                "SELECT symbol, MAX(ts) AS ts FROM orderflow_events "
                "WHERE symbol = ANY($1::text[]) GROUP BY symbol",
                symbols,
            )
        by_symbol = {row["symbol"]: row["ts"] for row in rows}
        if any(symbol not in by_symbol for symbol in symbols):
            return None
        return min(by_symbol.values())
