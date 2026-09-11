"""Ingest L2 book diffs, L4 order updates, and market-wide leverage updates."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import statistics
import struct
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol, cast

import asyncpg
import msgpack
import zstandard
from websockets.exceptions import WebSocketException

from hydromancer_eda.client.ws import HydromancerWebSocket
from hydromancer_eda.config import Settings
from hydromancer_eda.ingestion.progress import ReplayProgress
from hydromancer_eda.models import L2BookTick, L4OrderEvent, LeverageUpdate, Side
from hydromancer_eda.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)
REPLAY_GRACE_SECONDS = 10.0


class ReplayStalledError(Exception):
    """A channel replay made no progress before its configured timeout."""


class SnapshotRequestError(OSError):
    """Report a snapshot HTTP status without retaining a token-bearing request."""

    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.status = status


def _timestamp(value: float) -> datetime:
    return datetime.fromtimestamp(float(value) / 1000, tz=UTC)


def decode_l2_snapshots(payload: bytes) -> list[list[Any]]:
    """Decode single-msgpack or length-prefixed multi-zstd L2 snapshots."""
    if len(payload) >= 12 and payload[8:12] == b"\x28\xb5\x2f\xfd":
        count = struct.unpack_from("<I", payload)[0]
        offset = 4
        snapshots: list[Any] = []
        decompressor = zstandard.ZstdDecompressor()
        for _ in range(count):
            if offset + 4 > len(payload):
                raise ValueError("Truncated L2 snapshot blob-length field")
            blob_length = struct.unpack_from("<I", payload, offset)[0]
            offset += 4
            blob = payload[offset : offset + blob_length]
            if len(blob) != blob_length:
                raise ValueError("Truncated L2 snapshot compressed blob")
            offset += blob_length
            with decompressor.stream_reader(blob) as reader:
                snapshots.append(msgpack.unpackb(reader.read(), raw=False))
        if offset != len(payload):
            raise ValueError("Unexpected trailing bytes in L2 snapshot response")
    else:
        unpacker = msgpack.Unpacker(raw=False)
        unpacker.feed(payload)
        objects = list(unpacker)
        if not objects or unpacker.tell() != len(payload):
            raise ValueError("Empty or truncated L2 snapshot response")
        if len(objects) > 1:
            snapshots = objects
        else:
            decoded = objects[0]
            snapshots = [decoded] if decoded and isinstance(decoded[0], str) else decoded
    if not all(
        isinstance(snapshot, list)
        and len(snapshot) == 6
        and isinstance(snapshot[0], str)
        for snapshot in snapshots
    ):
        raise ValueError("Unexpected L2 snapshot response shape")
    return cast(list[list[Any]], snapshots)


def _post_snapshot(token: str, payload: dict[str, Any]) -> bytes:
    request = urllib.request.Request(
        "https://api.hydromancer.xyz/info",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "hydromancer-eda/0.1",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        raise SnapshotRequestError(exc.code) from None


@dataclass
class BookState:
    """Maintain one reconstructed L2 book and event-volatility history."""

    epoch: str
    source_seq: int
    height: int
    event_no: int
    bids: dict[Decimal, tuple[Decimal, int]] = field(default_factory=dict)
    asks: dict[Decimal, tuple[Decimal, int]] = field(default_factory=dict)
    mids: deque[float] = field(default_factory=deque)


@dataclass(frozen=True)
class RestingOrder:
    """Retain wallet attribution for L4 updates that omit order fields."""

    ts: datetime
    symbol: str
    wallet: str
    side: Side
    price: Decimal
    size: Decimal
    tracked: bool = True


class ChannelHandler(Protocol):
    """Define durable channel processing used by the reconnect runner."""

    channel: str
    subscription: dict[str, Any]

    async def ensure_ready(self) -> None: ...

    async def process(self, payload: Any, cursor: str | None, session_id: str | None) -> None: ...

    async def reset_after_failure(self) -> None: ...


class L2BookHandler:
    """Bootstrap L2 books and persist one compact tick per per-coin diff."""

    channel = "l2BookDiff"

    def __init__(self, settings: Settings, store: PostgresStore) -> None:
        self.settings = settings
        self.store = store
        self.subscription = {"type": self.channel, "coins": settings.coins}
        self.states: dict[str, BookState] = {}

    async def ensure_ready(self) -> None:
        """Bootstrap every configured coin before applying queued diffs."""
        missing = [coin for coin in self.settings.coins if coin not in self.states]
        unrestored: list[str] = []
        for coin in missing:
            if not await self._restore(coin):
                unrestored.append(coin)
        if unrestored:
            await self._bootstrap(unrestored)

    async def _restore(self, coin: str) -> bool:
        durable = await self.store.load_l2_book_state(coin)
        if durable is None:
            return False
        mids = await self.store.recent_l2_mids(
            coin, self.settings.l2_volatility_lookback + 1
        )
        state = BookState(
            epoch=durable["epoch"],
            source_seq=durable["source_seq"],
            height=durable["height"],
            event_no=durable["event_no"],
            mids=deque(mids, maxlen=self.settings.l2_volatility_lookback + 1),
        )
        for level in durable["levels"]:
            side = state.bids if level["side"] == "bid" else state.asks
            side[Decimal(str(level["price"]))] = (
                Decimal(str(level["size"])),
                int(level["order_count"]),
            )
        if not state.bids or not state.asks:
            return False
        self.states[coin] = state
        return True

    async def _report_bootstrap_status(
        self, coins: list[str], started_at: float
    ) -> None:
        """Report that an L2 snapshot request is still in progress."""
        while True:
            await asyncio.sleep(self.settings.replay_status_interval_seconds)
            logger.info(
                "l2 re-bootstrap still waiting for snapshot for %s (%.1fs elapsed)",
                ", ".join(coins),
                time.monotonic() - started_at,
            )

    async def _bootstrap(self, coins: list[str]) -> None:
        started_at = time.monotonic()
        logger.info("l2 re-bootstrap started for %s", ", ".join(coins))
        reporter = asyncio.create_task(self._report_bootstrap_status(coins, started_at))
        installed: set[str] = set()
        try:
            raw = await asyncio.to_thread(
                _post_snapshot,
                self.settings.hydromancer_api_key,
                {"type": "l2BookDiffSnapshot", "coins": coins},
            )
            snapshots = decode_l2_snapshots(raw)
            logger.info(
                "l2 snapshot response received for %s after %.1fs; decoded %d snapshot(s)",
                ", ".join(coins),
                time.monotonic() - started_at,
                len(snapshots),
            )
            for snapshot in snapshots:
                coin, _time_ms, levels, _height, epoch, source_seq = snapshot
                coin_name = str(coin)
                event_no = await self.store.next_l2_event_number(coin_name)
                mids = await self.store.recent_l2_mids(
                    coin_name, self.settings.l2_volatility_lookback + 1
                )
                state = BookState(
                    epoch=str(epoch),
                    source_seq=int(source_seq),
                    height=int(_height),
                    event_no=event_no - 1,
                    mids=deque(mids, maxlen=self.settings.l2_volatility_lookback + 1),
                )
                for side_index, side in enumerate((state.bids, state.asks)):
                    for level in levels[side_index]:
                        if isinstance(level, dict):
                            price_value, size_value, order_count = (
                                level["px"],
                                level["sz"],
                                level["n"],
                            )
                        else:
                            price_value, size_value, order_count = level
                        side[Decimal(str(price_value))] = (
                            Decimal(str(size_value)),
                            int(order_count),
                        )
                if state.bids and state.asks:
                    snapshot_mid = float((max(state.bids) + min(state.asks)) / 2)
                    if not state.mids or state.mids[-1] != snapshot_mid:
                        state.mids.append(snapshot_mid)
                self.states[coin_name] = state
                durable_levels = [
                    (side_name, price, size, order_count)
                    for side_name, side in (("bid", state.bids), ("ask", state.asks))
                    for price, (size, order_count) in side.items()
                ]
                await self.store.replace_l2_book_state(
                    coin_name,
                    state.epoch,
                    state.source_seq,
                    state.height,
                    state.event_no,
                    _timestamp(float(_time_ms)),
                    durable_levels,
                )
                installed.add(coin_name)
                logger.info(
                    "l2 snapshot installed for %s at height %d, source sequence %d",
                    coin_name,
                    state.height,
                    state.source_seq,
                )
            missing = set(coins) - installed
            if missing:
                raise ValueError(
                    "l2 snapshot response omitted requested coin(s): "
                    + ", ".join(sorted(missing))
                )
            logger.info(
                "l2 re-bootstrap snapshot phase succeeded for %s in %.1fs",
                ", ".join(coins),
                time.monotonic() - started_at,
            )
        except Exception as exc:
            logger.error(
                "l2 re-bootstrap failed for %s after %.1fs (%s); channel will reconnect",
                ", ".join(coins),
                time.monotonic() - started_at,
                type(exc).__name__,
            )
            raise
        finally:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)

    async def reset_after_failure(self) -> None:
        """Discard uncommitted in-memory changes and restore from PostgreSQL."""
        self.states.clear()

    async def process(
        self, payload: Any, cursor: str | None, session_id: str | None
    ) -> None:
        """Apply one block batch and persist its post-diff book states."""
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(data, dict):
            return
        if data.get("type") == "resync":
            coin = str(data.get("coin", ""))
            self.states.pop(coin, None)
            if coin:
                await self._bootstrap([coin])
            await self.store.persist_channel_cursor(self.channel, session_id, cursor)
            return
        height = int(data["height"])
        ts = _timestamp(data["time"])
        ticks: list[L2BookTick] = []
        level_changes: list[tuple[str, str, Decimal, Decimal, int]] = []
        state_rows: list[tuple[str, str, int, int, int, datetime]] = []
        for diff in data.get("diffs", []):
            coin = str(diff["coin"])
            state = self.states.get(coin)
            if state is None:
                await self._bootstrap([coin])
                state = self.states[coin]
            epoch = str(diff["epoch"])
            source_seq = int(diff["seq"])
            if epoch == state.epoch and source_seq <= state.source_seq:
                continue
            if epoch != state.epoch or int(diff["prev_seq"]) != state.source_seq:
                logger.warning("l2 continuity changed for %s; re-bootstrapping", coin)
                self.states.pop(coin, None)
                await self._bootstrap([coin])
                state = self.states[coin]
                if height <= state.height:
                    logger.info(
                        "l2 continuity restored for %s: snapshot height %d already covers "
                        "incoming height %d",
                        coin,
                        state.height,
                        height,
                    )
                    continue
                if epoch != state.epoch or int(diff["prev_seq"]) != state.source_seq:
                    logger.error(
                        "l2 re-bootstrap did not align for %s: snapshot sequence %d, "
                        "incoming previous sequence %d; reconnecting",
                        coin,
                        state.source_seq,
                        int(diff["prev_seq"]),
                    )
                    raise ValueError(f"l2 snapshot did not align for {coin}")
                logger.info(
                    "l2 continuity restored for %s at height %d, source sequence %d",
                    coin,
                    state.height,
                    state.source_seq,
                )
            for side_index, side in enumerate((state.bids, state.asks)):
                side_name = "bid" if side_index == 0 else "ask"
                for level in diff["levels"][side_index]:
                    price = Decimal(str(level["px"]))
                    size = Decimal(str(level["sz"]))
                    order_count = int(level["n"])
                    level_changes.append((coin, side_name, price, size, order_count))
                    if size == 0:
                        side.pop(price, None)
                    else:
                        side[price] = (size, order_count)
            state.epoch = epoch
            state.source_seq = source_seq
            state.height = height
            state.event_no += 1
            state_rows.append(
                (coin, epoch, source_seq, height, state.event_no, ts)
            )
            if not state.bids or not state.asks:
                continue
            best_bid = max(state.bids)
            best_ask = min(state.asks)
            bid_size = state.bids[best_bid][0]
            ask_size = state.asks[best_ask][0]
            mid = (best_bid + best_ask) / 2
            mid_float = float(mid)
            state.mids.append(mid_float)
            returns = [
                math.log(current / previous)
                for previous, current in zip(state.mids, list(state.mids)[1:], strict=False)
                if previous > 0 and current > 0
            ]
            volatility = (
                statistics.pstdev(returns)
                if len(returns) >= self.settings.l2_volatility_min_samples
                else None
            )
            total_size = bid_size + ask_size
            ticks.append(
                L2BookTick(
                    ts=ts,
                    symbol=coin,
                    height=height,
                    epoch=epoch,
                    source_seq=source_seq,
                    event_no=state.event_no,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    mid_price=mid,
                    spread_bps=float((best_ask - best_bid) / mid * 10000),
                    bid_size=bid_size,
                    ask_size=ask_size,
                    imbalance=float((bid_size - ask_size) / total_size) if total_size else 0,
                    trailing_event_volatility=volatility,
                )
            )
        await self.store.persist_l2_ticks(
            ticks, level_changes, state_rows, session_id, cursor
        )


class L4BookHandler:
    """Persist short-lived L4 events and indefinite wallet order aggregates."""

    channel = "l4BookUpdates"

    def __init__(self, settings: Settings, store: PostgresStore) -> None:
        self.settings = settings
        self.store = store
        self.subscription = {"type": self.channel, "coins": settings.coins, "merge": True}
        self.orders: dict[str, RestingOrder] = {}
        self.ready = False
        self.snapshot_height = 0

    async def ensure_ready(self) -> None:
        """Seed resting-order attribution from the documented L4 snapshot."""
        if self.ready:
            return
        raw = await asyncio.to_thread(
            _post_snapshot,
            self.settings.hydromancer_api_key,
            {"type": "l4Book", "coins": self.settings.coins},
        )
        count, snapshot_height, timestamp_ms = struct.unpack_from("<IQQ", raw, 0)
        offset = 20
        decompressor = zstandard.ZstdDecompressor()
        snapshot_ts = _timestamp(timestamp_ms)
        for _ in range(count):
            blob_length = struct.unpack_from("<I", raw, offset)[0]
            offset += 4
            blob = raw[offset : offset + blob_length]
            offset += blob_length
            with decompressor.stream_reader(blob) as reader:
                decoded = msgpack.unpackb(reader.read(), raw=False)
            coin = str(decoded[0])
            for side_name, orders in (("buy", decoded[1]), ("sell", decoded[2])):
                for user, price, size, order_id in orders:
                    self.orders[str(order_id)] = RestingOrder(
                        ts=snapshot_ts,
                        symbol=coin,
                        wallet=str(user).lower(),
                        side=cast(Side, side_name),
                        price=Decimal(str(price)),
                        size=Decimal(str(size)),
                        tracked=False,
                    )
        self.snapshot_height = int(snapshot_height)
        self.ready = True

    async def reset_after_failure(self) -> None:
        """Keep state because it changes only after a successful commit."""

    async def process(
        self, payload: Any, cursor: str | None, session_id: str | None
    ) -> None:
        """Process one L4 block batch."""
        data = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(data, dict):
            return
        ts = _timestamp(data["timestamp"])
        height = int(data["height"])
        if height <= self.snapshot_height:
            await self.store.persist_l4_events([], session_id, cursor)
            return
        events: list[L4OrderEvent] = []
        orders = self.orders.copy()
        for event_index, diff in enumerate(data.get("diffs", [])):
            event_type_value = str(diff["type"])
            if event_type_value not in {"new", "update", "remove"}:
                continue
            event_type = cast(Literal["new", "update", "remove"], event_type_value)
            order_id = str(diff["oid"])
            existing = orders.get(order_id)
            if event_type == "new":
                side = cast(Side, "buy" if str(diff.get("side")) == "B" else "sell")
                existing = RestingOrder(
                    ts=ts,
                    symbol=str(diff["coin"]),
                    wallet=str(diff["user"]).lower(),
                    side=side,
                    price=Decimal(str(diff["px"])),
                    size=Decimal(str(diff["sz"])),
                )
                orders[order_id] = existing
            elif existing and event_type == "update":
                existing = RestingOrder(
                    ts=existing.ts,
                    symbol=existing.symbol,
                    wallet=existing.wallet,
                    side=existing.side,
                    price=existing.price,
                    size=Decimal(str(diff["sz"])),
                    tracked=existing.tracked,
                )
                orders[order_id] = existing
            resting = (ts - existing.ts).total_seconds() if existing and event_type == "remove" else None
            events.append(
                L4OrderEvent(
                    ts=ts,
                    height=height,
                    event_index=event_index,
                    symbol=str(diff.get("coin") or (existing.symbol if existing else "unknown")),
                    order_id=order_id,
                    wallet=existing.wallet if existing else None,
                    event_type=event_type,
                    side=existing.side if existing else "unknown",
                    price=existing.price if existing else None,
                    size=existing.size if existing else None,
                    resting_seconds=resting,
                    aggregate=existing.tracked if existing else False,
                )
            )
            if event_type == "remove":
                orders.pop(order_id, None)
        await self.store.persist_l4_events(events, session_id, cursor)
        self.orders = orders


class LeverageHandler:
    """Persist all market-wide leverage setting changes indefinitely."""

    channel = "allLeverageUpdates"

    def __init__(self, _settings: Settings, store: PostgresStore) -> None:
        self.store = store
        self.subscription = {"type": self.channel}

    async def ensure_ready(self) -> None:
        """Require no REST bootstrap for the replayable leverage stream."""

    async def reset_after_failure(self) -> None:
        """No in-memory leverage state is retained."""

    async def process(
        self, payload: Any, cursor: str | None, session_id: str | None
    ) -> None:
        """Normalize and persist leverage records from one message."""
        if isinstance(payload, dict) and "updates" in payload:
            items = payload["updates"]
        elif isinstance(payload, dict) and "update_type" in payload:
            items = [payload]
        else:
            items = payload
        updates: list[LeverageUpdate] = []
        for item in items if isinstance(items, list) else []:
            update_type_value = str(item.get("update_type"))
            if update_type_value not in {
                "leverage",
                "isolated_margin",
                "top_up_isolated_margin",
            }:
                continue
            update_type = cast(
                Literal["leverage", "isolated_margin", "top_up_isolated_margin"],
                update_type_value,
            )
            leverage_value = (
                item.get("leverage")
                if update_type == "leverage"
                else item.get("target_leverage")
            )
            is_buy_value = item.get("is_buy")
            ntli_value = item.get("ntli")
            updates.append(
                LeverageUpdate(
                    ts=_timestamp(item["time"]),
                    wallet=str(item["user"]).lower(),
                    symbol=str(item["coin"]),
                    update_type=update_type,
                    leverage=(
                        float(leverage_value) if leverage_value is not None else None
                    ),
                    is_cross=(
                        bool(item["is_cross"])
                        if "is_cross" in item
                        else False if update_type == "top_up_isolated_margin" else None
                    ),
                    is_buy=bool(is_buy_value) if is_buy_value is not None else None,
                    ntli=Decimal(str(ntli_value)) if ntli_value is not None else None,
                    tx_index=int(item["tx_index"]),
                )
            )
        await self.store.persist_leverage_updates(updates, session_id, cursor)


class ChannelRunner:
    """Run one independently checkpointed Hydromancer channel connection."""

    def __init__(self, settings: Settings, handler: ChannelHandler) -> None:
        self.settings = settings
        self.handler = handler
        self.store = handler.store  # type: ignore[attr-defined]
        self.session_id: str | None = None
        self.cursor: str | None = None
        self.last_seq = 0
        self.replay_pending = False
        self.replay_deadline = 0.0
        self.buffered_live: list[dict[str, Any]] = []
        self.progress = ReplayProgress(
            handler.channel,
            logger,
            settings.replay_status_interval_seconds,
            settings.replay_stall_timeout_seconds,
        )

    async def _process_live(self, message: dict[str, Any]) -> None:
        seq = message.get("seq")
        if isinstance(seq, int) and self.last_seq and seq != self.last_seq + 1:
            raise ValueError(f"{self.handler.channel} envelope sequence gap")
        if isinstance(seq, int):
            self.last_seq = seq
        cursor_value = message.get("cursor")
        cursor = str(cursor_value) if cursor_value is not None else None
        await self.handler.process(message, cursor, self.session_id)
        if cursor is not None:
            self.cursor = cursor

    async def _flush_live(self) -> None:
        buffered, self.buffered_live = self.buffered_live, []
        for message in buffered:
            await self._process_live(message)

    async def _finish_no_replay(self) -> None:
        """Declare recovery current when the server sends no replay within grace."""
        if not self.replay_pending or not self.progress.active:
            return
        logger.info(
            "%s sent no replay within %.1fs; cursor is current",
            self.handler.channel,
            REPLAY_GRACE_SECONDS,
        )
        self.replay_pending = False
        buffered_count = len(self.buffered_live)
        self.progress.finish(buffered_count, "no replay required")
        await self._flush_live()

    async def _backoff(self, stop_event: asyncio.Event, attempt: int) -> None:
        maximum = min(
            30.0,
            self.settings.reconnect_delay_seconds * (2 ** min(attempt, 5)),
        )
        delay = maximum + random.uniform(0, maximum * 0.2)
        logger.warning(
            "%s reconnect attempt %d starts in %.1fs from the durable cursor",
            self.handler.channel,
            attempt,
            delay,
        )
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=delay)
        except TimeoutError:
            pass

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Reconnect and consume the channel until shutdown."""
        self.session_id, self.cursor = await self.store.load_checkpoint(self.handler.channel)
        websocket = HydromancerWebSocket(self.settings.hydromancer_api_key)
        attempt = 0
        while not stop_event.is_set():
            try:
                connection = await websocket.connect(self.session_id, self.cursor)
                async with connection:
                    attempt = 0
                    monitor = asyncio.create_task(
                        self.progress.monitor(
                            connection,
                            stop_event,
                            lambda: len(self.buffered_live),
                            REPLAY_GRACE_SECONDS,
                            self._finish_no_replay,
                        )
                    )
                    try:
                        async for message in websocket.messages(connection):
                            message_type = message.get("type")
                            if message_type == "ping":
                                await websocket.send(connection, {"type": "pong"})
                                continue
                            if message_type in {"connected", "reconnected"}:
                                new_session = message.get("sessionId")
                                if isinstance(new_session, str):
                                    self.session_id = new_session
                                self.last_seq = 0
                                self.buffered_live.clear()
                                self.replay_pending = (
                                    message_type == "reconnected" or self.cursor is not None
                                )
                                self.replay_deadline = time.monotonic() + REPLAY_GRACE_SECONDS
                                if message_type == "connected":
                                    subscription = dict(self.handler.subscription)
                                    if self.cursor is not None:
                                        subscription["cursor"] = self.cursor
                                        logger.warning(
                                            "%s session expired; starting cursor-based backfill",
                                            self.handler.channel,
                                        )
                                    await websocket.send(
                                        connection,
                                        {"method": "subscribe", "subscription": subscription},
                                    )
                                await self.handler.ensure_ready()
                                await self.store.persist_channel_cursor(
                                    self.handler.channel, self.session_id, self.cursor
                                )
                                if self.replay_pending:
                                    reason = (
                                        "existing session resumed"
                                        if message_type == "reconnected"
                                        else "new session resumed from durable cursor"
                                    )
                                    self.progress.start(reason)
                                else:
                                    logger.info(
                                        "%s connected with no prior cursor; live ingestion is ready",
                                        self.handler.channel,
                                    )
                                continue
                            if message_type == "subscriptionUpdate":
                                failed = message.get("failed")
                                if isinstance(failed, list) and failed:
                                    logger.error(
                                        "%s subscription rejected: %s",
                                        self.handler.channel,
                                        failed,
                                    )
                                    raise ValueError("subscription rejected")
                            if message_type == "error":
                                logger.error(
                                    "%s WebSocket protocol error; reconnecting",
                                    self.handler.channel,
                                )
                                raise ValueError("WebSocket protocol error")
                            if (
                                self.replay_pending
                                and time.monotonic() >= self.replay_deadline
                                and message_type != "replay"
                            ):
                                await self._finish_no_replay()
                            if (
                                message_type == "replay"
                                and message.get("channel") == self.handler.channel
                            ):
                                self.replay_deadline = math.inf
                                data = message.get("data", [])
                                chunk = message.get("chunk", 1)
                                total = message.get("totalChunks", 1)
                                if not isinstance(data, list):
                                    raise TypeError("replay data is not a list")
                                if (
                                    not isinstance(chunk, int)
                                    or isinstance(chunk, bool)
                                    or not isinstance(total, int)
                                    or isinstance(total, bool)
                                    or chunk < 1
                                    or total < chunk
                                ):
                                    raise ValueError("invalid replay chunk metadata")
                                has_gap = message.get("hasGap") is True
                                if has_gap:
                                    logger.error(
                                        "%s replay reported a data gap; available replay will "
                                        "continue but market-wide REST repair is unavailable",
                                        self.handler.channel,
                                    )
                                for item in data:
                                    await self.handler.process(item, None, self.session_id)
                                cursor_value = message.get("cursor")
                                if cursor_value is not None:
                                    self.cursor = str(cursor_value)
                                    await self.store.persist_channel_cursor(
                                        self.handler.channel, self.session_id, self.cursor
                                    )
                                self.progress.advance(
                                    chunk,
                                    total,
                                    len(data),
                                    has_gap,
                                    len(self.buffered_live),
                                )
                                has_more = message.get("hasMore")
                                terminal = has_more is False or (
                                    has_more is None and chunk == total
                                )
                                if terminal:
                                    self.replay_pending = False
                                    buffered_count = len(self.buffered_live)
                                    self.progress.finish(buffered_count)
                                    await self._flush_live()
                                continue
                            if message_type == self.handler.channel:
                                if self.replay_pending:
                                    self.buffered_live.append(message)
                                else:
                                    await self._process_live(message)
                            if stop_event.is_set():
                                break
                        if monitor.done() and not monitor.cancelled():
                            monitor.result()
                        if self.progress.stalled:
                            raise ReplayStalledError
                        if not stop_event.is_set():
                            raise OSError("WebSocket connection ended")
                    finally:
                        monitor.cancel()
                        await asyncio.gather(monitor, return_exceptions=True)
            except SnapshotRequestError as exc:
                self.progress.fail(f"snapshot HTTP {exc.status}")
                logger.warning(
                    "%s snapshot request failed with HTTP status %d; retrying",
                    self.handler.channel,
                    exc.status,
                )
                await self.handler.reset_after_failure()
                attempt += 1
                await self._backoff(stop_event, attempt)
            except (
                OSError,
                ReplayStalledError,
                WebSocketException,
                asyncpg.PostgresError,
                KeyError,
                TypeError,
                ValueError,
                struct.error,
                zstandard.ZstdError,
                msgpack.UnpackException,
            ) as exc:
                error_type = type(exc).__name__
                self.progress.fail(error_type)
                logger.warning(
                    "%s connection or payload failed (%s); retrying",
                    self.handler.channel,
                    error_type,
                )
                await self.handler.reset_after_failure()
                attempt += 1
                await self._backoff(stop_event, attempt)


class MarketDataIngestionService:
    """Own independent L2, L4, and leverage channel runners."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def _retention_loop(
        self, store: PostgresStore, stop_event: asyncio.Event
    ) -> None:
        """Apply bounded raw-book retention once per hour."""
        while not stop_event.is_set():
            await store.apply_retention()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=3600)
            except TimeoutError:
                continue

    async def run_forever(self, stop_event: asyncio.Event) -> None:
        """Initialize stores and run all market-data channels concurrently."""
        stores = [PostgresStore(self.settings) for _ in range(3)]
        for store in stores:
            await store.connect()
            await store.init_schema()
        handlers: list[ChannelHandler] = [
            L2BookHandler(self.settings, stores[0]),
            L4BookHandler(self.settings, stores[1]),
            LeverageHandler(self.settings, stores[2]),
        ]
        try:
            await asyncio.gather(
                *(ChannelRunner(self.settings, handler).run_forever(stop_event) for handler in handlers),
                self._retention_loop(stores[0], stop_event),
            )
        finally:
            await asyncio.gather(*(store.close() for store in stores))
