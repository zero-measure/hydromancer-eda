"""PostgreSQL persistence for events, checkpoints, rejects, and labels."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from importlib.resources import files
from typing import Any

import asyncpg

from hydromancer_eda.config import Settings
from hydromancer_eda.models import (
    L2BookTick,
    L4OrderEvent,
    LeverageUpdate,
    OrderflowEvent,
    WalletLabelSnapshot,
)


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
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtext('hydromancer-analytics-schema'))"
            )
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
            ts, symbol, wallet, side, aggressor, price, size, notional,
            order_id, trade_id, tx_index, realized_pnl, liquidation,
            liquidation_mark_price, liquidation_method
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13, $14, $15
        )
        ON CONFLICT DO NOTHING
        """

        rows = [
            (
                e.ts,
                e.symbol,
                e.wallet,
                e.side,
                e.aggressor,
                e.price,
                e.size,
                e.notional,
                e.order_id,
                e.trade_id,
                e.tx_index,
                e.realized_pnl,
                e.liquidation,
                e.liquidation_mark_price,
                e.liquidation_method,
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
        INSERT INTO orderflow_events (
            ts, symbol, wallet, side, aggressor, price, size, notional,
            order_id, trade_id, tx_index, realized_pnl, liquidation,
            liquidation_mark_price, liquidation_method
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT DO NOTHING
        """
        rows = [
            (
                event.ts, event.symbol, event.wallet, event.side, event.aggressor,
                event.price, event.size, event.notional, event.order_id,
                event.trade_id, event.tx_index, event.realized_pnl,
                event.liquidation, event.liquidation_mark_price,
                event.liquidation_method,
            )
            for event in events
        ]
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
        """Upsert one current observed-lifetime label row per wallet.

        Lifetime wallets are never pruned from a partial or empty result. The
        explicit label reset operation is responsible for removing obsolete
        schema data.

        Args:
            snapshots (list[WalletLabelSnapshot]): Current lifetime snapshots.

        Returns:
            int: Number of current rows submitted for upsert.

        Raises:
            RuntimeError: If :meth:`connect` has not initialized the pool.
            asyncpg.PostgresError: If the replacement fails.

        """
        if not self._pool:
            raise RuntimeError("Pool not initialized")

        query = """
        INSERT INTO wallet_flow_labels (
            bucket_ts, wallet, trade_count, taker_ratio, liquidation_ratio,
            round_number_notional_ratio, round_number_size_ratio,
            message_cadence_per_min, realized_pnl, max_leverage,
            current_max_leverage, leverage_update_count,
            isolated_margin_update_count, order_new_count, order_update_count, order_remove_count, order_remove_ratio,
            round_order_size_ratio, average_order_lifetime_seconds,
            markout_metrics, updated_at
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
            $19,$20::jsonb,NOW()
        )
        ON CONFLICT (wallet) DO UPDATE SET
            bucket_ts = EXCLUDED.bucket_ts,
            trade_count = EXCLUDED.trade_count,
            taker_ratio = EXCLUDED.taker_ratio,
            liquidation_ratio = EXCLUDED.liquidation_ratio,
            round_number_notional_ratio = EXCLUDED.round_number_notional_ratio,
            round_number_size_ratio = EXCLUDED.round_number_size_ratio,
            message_cadence_per_min = EXCLUDED.message_cadence_per_min,
            realized_pnl = EXCLUDED.realized_pnl,
            max_leverage = EXCLUDED.max_leverage,
            current_max_leverage = EXCLUDED.current_max_leverage,
            leverage_update_count = EXCLUDED.leverage_update_count,
            isolated_margin_update_count = EXCLUDED.isolated_margin_update_count,
            order_new_count = EXCLUDED.order_new_count,
            order_update_count = EXCLUDED.order_update_count,
            order_remove_count = EXCLUDED.order_remove_count,
            order_remove_ratio = EXCLUDED.order_remove_ratio,
            round_order_size_ratio = EXCLUDED.round_order_size_ratio,
            average_order_lifetime_seconds = EXCLUDED.average_order_lifetime_seconds,
            markout_metrics = EXCLUDED.markout_metrics,
            updated_at = NOW()
        """
        rows = [
            (
                snapshot.bucket_ts,
                snapshot.wallet,
                snapshot.trade_count,
                snapshot.taker_ratio,
                snapshot.liquidation_ratio,
                snapshot.round_number_notional_ratio,
                snapshot.round_number_size_ratio,
                snapshot.message_cadence_per_min,
                snapshot.realized_pnl,
                snapshot.max_leverage,
                snapshot.current_max_leverage,
                snapshot.leverage_update_count,
                snapshot.isolated_margin_update_count,
                snapshot.order_new_count,
                snapshot.order_update_count,
                snapshot.order_remove_count,
                snapshot.order_remove_ratio,
                snapshot.round_order_size_ratio,
                snapshot.average_order_lifetime_seconds,
                json.dumps(snapshot.markout_metrics),
            )
            for snapshot in snapshots
        ]

        if not rows:
            return 0
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.executemany(query, rows)
        return len(rows)

    async def persist_channel_cursor(
        self, channel: str, session_id: str | None, cursor: str | None
    ) -> None:
        """Persist a non-fill channel checkpoint after durable processing."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        query = """
        INSERT INTO ingestion_checkpoints (channel, session_id, cursor, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (channel) DO UPDATE SET
            session_id = EXCLUDED.session_id,
            cursor = EXCLUDED.cursor,
            updated_at = NOW()
        """
        async with self._pool.acquire() as conn:
            await conn.execute(query, channel, session_id, cursor)

    async def next_l2_event_number(self, symbol: str) -> int:
        """Return the next durable per-symbol compact-book event number."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn:
            value = await conn.fetchval(
                "SELECT COALESCE((SELECT last_event_no FROM l2_event_counters "
                "WHERE symbol = $1), 0) + 1",
                symbol,
            )
        return int(value)

    async def load_l2_book_state(self, symbol: str) -> dict[str, Any] | None:
        """Load a durable reconstructed book for replay after process restart."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn:
            state = await conn.fetchrow(
                "SELECT epoch,source_seq,height,event_no FROM l2_book_state "
                "WHERE symbol=$1",
                symbol,
            )
            if state is None:
                return None
            levels = await conn.fetch(
                "SELECT side,price,size,order_count FROM l2_book_levels "
                "WHERE symbol=$1",
                symbol,
            )
        return {
            "epoch": str(state["epoch"]),
            "source_seq": int(state["source_seq"]),
            "height": int(state["height"]),
            "event_no": int(state["event_no"]),
            "levels": [dict(row) for row in levels],
        }

    async def replace_l2_book_state(
        self,
        symbol: str,
        epoch: str,
        source_seq: int,
        height: int,
        event_no: int,
        ts: datetime,
        levels: list[tuple[str, Decimal, Decimal, int]],
    ) -> None:
        """Atomically replace one coin's reconstruction state from a snapshot."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute("DELETE FROM l2_book_levels WHERE symbol=$1", symbol)
            if levels:
                await conn.executemany(
                    "INSERT INTO l2_book_levels "
                    "(symbol,side,price,size,order_count) VALUES ($1,$2,$3,$4,$5)",
                    [(symbol, *level) for level in levels],
                )
            await conn.execute(
                """INSERT INTO l2_book_state
                   (symbol,epoch,source_seq,height,event_no,updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6)
                   ON CONFLICT (symbol) DO UPDATE SET epoch=EXCLUDED.epoch,
                   source_seq=EXCLUDED.source_seq,height=EXCLUDED.height,
                   event_no=EXCLUDED.event_no,updated_at=EXCLUDED.updated_at""",
                symbol,
                epoch,
                source_seq,
                height,
                event_no,
                ts,
            )

    async def recent_l2_mids(self, symbol: str, limit: int) -> list[float]:
        """Load recent mids in chronological order for volatility warmup."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT mid_price::float8 AS mid FROM l2_book_ticks "
                "WHERE symbol = $1 ORDER BY event_no DESC LIMIT $2",
                symbol,
                limit,
            )
        return [float(row["mid"]) for row in reversed(rows)]

    async def persist_l2_ticks(
        self,
        ticks: list[L2BookTick],
        level_changes: list[tuple[str, str, Decimal, Decimal, int]],
        states: list[tuple[str, str, int, int, int, datetime]],
        session_id: str | None,
        cursor: str | None,
    ) -> None:
        """Persist one L2 message and its channel checkpoint atomically."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        insert = """
        INSERT INTO l2_book_ticks (
            ts, symbol, height, epoch, source_seq, event_no, best_bid, best_ask,
            mid_price, spread_bps, bid_size, ask_size, imbalance,
            trailing_event_volatility
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT DO NOTHING
        """
        async with self._pool.acquire() as conn, conn.transaction():
            for symbol, side, price, size, order_count in level_changes:
                if size == 0:
                    await conn.execute(
                        "DELETE FROM l2_book_levels "
                        "WHERE symbol=$1 AND side=$2 AND price=$3",
                        symbol,
                        side,
                        price,
                    )
                else:
                    await conn.execute(
                        """INSERT INTO l2_book_levels
                           (symbol,side,price,size,order_count) VALUES ($1,$2,$3,$4,$5)
                           ON CONFLICT (symbol,side,price) DO UPDATE SET
                           size=EXCLUDED.size,order_count=EXCLUDED.order_count""",
                        symbol,
                        side,
                        price,
                        size,
                        order_count,
                    )
            if states:
                await conn.executemany(
                    """INSERT INTO l2_book_state
                       (symbol,epoch,source_seq,height,event_no,updated_at)
                       VALUES ($1,$2,$3,$4,$5,$6)
                       ON CONFLICT (symbol) DO UPDATE SET epoch=EXCLUDED.epoch,
                       source_seq=EXCLUDED.source_seq,height=EXCLUDED.height,
                       event_no=EXCLUDED.event_no,updated_at=EXCLUDED.updated_at""",
                    states,
                )
                await conn.executemany(
                    """INSERT INTO l2_event_counters (symbol,last_event_no)
                       VALUES ($1,$2) ON CONFLICT (symbol) DO UPDATE SET
                       last_event_no=GREATEST(l2_event_counters.last_event_no,
                                              EXCLUDED.last_event_no)""",
                    [(state[0], state[4]) for state in states],
                )
            if ticks:
                await conn.executemany(
                    insert,
                    [
                        (
                            tick.ts, tick.symbol, tick.height, tick.epoch,
                            tick.source_seq, tick.event_no, tick.best_bid,
                            tick.best_ask, tick.mid_price, tick.spread_bps,
                            tick.bid_size, tick.ask_size, tick.imbalance,
                            tick.trailing_event_volatility,
                        )
                        for tick in ticks
                    ],
                )
            if cursor is not None:
                await conn.execute(
                    """INSERT INTO ingestion_checkpoints
                       (channel,session_id,cursor,updated_at)
                       VALUES ('l2BookDiff',$1,$2,NOW())
                       ON CONFLICT (channel) DO UPDATE SET session_id=EXCLUDED.session_id,
                       cursor=EXCLUDED.cursor,updated_at=NOW()""",
                    session_id,
                    cursor,
                )

    async def persist_l4_events(
        self,
        events: list[L4OrderEvent],
        session_id: str | None,
        cursor: str | None,
    ) -> None:
        """Persist L4 lifecycle events and update indefinite wallet aggregates."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        insert = """
        INSERT INTO l4_order_events (
            ts,height,event_index,symbol,order_id,wallet,event_type,side,price,size,
            resting_seconds
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT DO NOTHING
        """
        aggregate = """
        INSERT INTO wallet_order_aggregates (
            wallet,symbol,new_count,update_count,remove_count,round_size_count,
            total_resting_seconds,resting_sample_count,first_event_ts,last_event_ts
        ) VALUES (
            $1,$2,($3='new')::int,($3='update')::int,($3='remove')::int,
            ($3='new' AND $4 IS NOT NULL AND MOD(ABS($4),0.1::numeric)=0)::int,
            COALESCE($5,0),($5 IS NOT NULL)::int,$6,$6
        )
        ON CONFLICT (wallet,symbol) DO UPDATE SET
            new_count=wallet_order_aggregates.new_count+EXCLUDED.new_count,
            update_count=wallet_order_aggregates.update_count+EXCLUDED.update_count,
            remove_count=wallet_order_aggregates.remove_count+EXCLUDED.remove_count,
            round_size_count=wallet_order_aggregates.round_size_count+EXCLUDED.round_size_count,
            total_resting_seconds=wallet_order_aggregates.total_resting_seconds
                +EXCLUDED.total_resting_seconds,
            resting_sample_count=wallet_order_aggregates.resting_sample_count
                +EXCLUDED.resting_sample_count,
            first_event_ts=LEAST(wallet_order_aggregates.first_event_ts,EXCLUDED.first_event_ts),
            last_event_ts=GREATEST(wallet_order_aggregates.last_event_ts,EXCLUDED.last_event_ts)
        """
        async with self._pool.acquire() as conn, conn.transaction():
            for event in events:
                result = await conn.execute(
                    insert,
                    event.ts,
                    event.height,
                    event.event_index,
                    event.symbol,
                    event.order_id,
                    event.wallet,
                    event.event_type,
                    event.side,
                    event.price,
                    event.size,
                    event.resting_seconds,
                )
                if result.endswith(" 1") and event.wallet and event.aggregate:
                    await conn.execute(
                        aggregate,
                        event.wallet,
                        event.symbol,
                        event.event_type,
                        event.size,
                        event.resting_seconds,
                        event.ts,
                    )
            if cursor is not None:
                await conn.execute(
                    """INSERT INTO ingestion_checkpoints
                       (channel,session_id,cursor,updated_at)
                       VALUES ('l4BookUpdates',$1,$2,NOW())
                       ON CONFLICT (channel) DO UPDATE SET session_id=EXCLUDED.session_id,
                       cursor=EXCLUDED.cursor,updated_at=NOW()""",
                    session_id,
                    cursor,
                )

    async def persist_leverage_updates(
        self,
        updates: list[LeverageUpdate],
        session_id: str | None,
        cursor: str | None,
    ) -> None:
        """Persist indefinite wallet leverage state and its checkpoint."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        insert = """
        INSERT INTO leverage_updates (
            ts,wallet,symbol,update_type,leverage,is_cross,is_buy,ntli,tx_index
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT DO NOTHING
        """
        upsert = """
        INSERT INTO wallet_leverage_state (
            wallet,symbol,current_leverage,max_leverage,update_count,
            cross_update_count,isolated_margin_update_count,
            first_update_ts,last_update_ts
        ) VALUES (
            $1,$2,$4,$4,1,($5 IS TRUE)::int,
            ($3 IN ('isolated_margin','top_up_isolated_margin'))::int,$6,$6
        )
        ON CONFLICT (wallet,symbol) DO UPDATE SET
            current_leverage=CASE
                WHEN EXCLUDED.current_leverage IS NOT NULL
                 AND EXCLUDED.last_update_ts >= wallet_leverage_state.last_update_ts
                THEN EXCLUDED.current_leverage
                ELSE wallet_leverage_state.current_leverage
            END,
            max_leverage=GREATEST(wallet_leverage_state.max_leverage,EXCLUDED.max_leverage),
            update_count=wallet_leverage_state.update_count+1,
            cross_update_count=wallet_leverage_state.cross_update_count
                +EXCLUDED.cross_update_count,
            isolated_margin_update_count=
                wallet_leverage_state.isolated_margin_update_count
                +EXCLUDED.isolated_margin_update_count,
            first_update_ts=LEAST(wallet_leverage_state.first_update_ts,EXCLUDED.first_update_ts),
            last_update_ts=GREATEST(wallet_leverage_state.last_update_ts,EXCLUDED.last_update_ts)
        """
        async with self._pool.acquire() as conn, conn.transaction():
            for update in updates:
                result = await conn.execute(
                    insert,
                    update.ts,
                    update.wallet,
                    update.symbol,
                    update.update_type,
                    update.leverage,
                    update.is_cross,
                    update.is_buy,
                    update.ntli,
                    update.tx_index,
                )
                if result.endswith(" 1"):
                    await conn.execute(
                        upsert,
                        update.wallet,
                        update.symbol,
                        update.update_type,
                        update.leverage,
                        update.is_cross,
                        update.ts,
                    )
            if cursor is not None:
                await conn.execute(
                    """INSERT INTO ingestion_checkpoints
                       (channel,session_id,cursor,updated_at)
                       VALUES ('allLeverageUpdates',$1,$2,NOW())
                       ON CONFLICT (channel) DO UPDATE SET session_id=EXCLUDED.session_id,
                       cursor=EXCLUDED.cursor,updated_at=NOW()""",
                    session_id,
                    cursor,
                )

    async def apply_retention(self) -> None:
        """Delete compact L2 and raw L4 rows beyond configured retention."""
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        async with self._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "DELETE FROM l2_book_ticks WHERE ts < NOW() - make_interval(hours => $1)",
                self._settings.l2_retention_hours,
            )
            await conn.execute(
                "DELETE FROM l4_order_events WHERE ts < NOW() - make_interval(hours => $1)",
                self._settings.l4_retention_hours,
            )

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
