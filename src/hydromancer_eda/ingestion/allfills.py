"""Sequential allFills replay/live ingestion state machine."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
from websockets.exceptions import InvalidURI, WebSocketException

from hydromancer_eda.client.ws import LIVE_FORMAT, HydromancerWebSocket, subscribe_frame
from hydromancer_eda.config import Settings
from hydromancer_eda.labeling.flow_labels import FlowLabeler
from hydromancer_eda.models import OrderflowEvent
from hydromancer_eda.storage.postgres import PostgresStore

logger = logging.getLogger(__name__)
FrameSender = Callable[[dict[str, Any]], Awaitable[None]]
FillIdentity = tuple[str, str | None, str | None, str]
REPLAY_GRACE_SECONDS = 2.0
MAX_BUFFERED_LIVE = 4096
MAX_BACKOFF_EXPONENT = 5


class ReplayBufferOverflow(Exception):
    """Live delivery exceeded the safe buffer before replay completed."""


class ReplayOrderError(Exception):
    """Replay chunks did not arrive in their documented order."""


class AllFillsIngestionService:
    def __init__(self, settings: Settings, store: PostgresStore | None = None) -> None:
        self._settings = settings
        self._store = store or PostgresStore(settings)
        self._labeler = FlowLabeler(settings)
        self._session_id: str | None = None
        self._cursor: str | None = None
        self._last_seen: tuple[int, int] | None = None
        self._seen_at_key: set[FillIdentity] = set()
        self._last_seq = 0
        self._replay_complete = True
        self._replay_chunks: set[int] = set()
        self._next_replay_chunk = 1
        self._replay_total: int | None = None
        self._buffered_live: list[dict[str, Any]] = []
        self._buffer_deadline: float | None = None
        self._next_label_at = datetime.now(UTC)
        self._label_task: asyncio.Task[None] | None = None
        self._reconnect_attempt = 0

    async def initialise(self) -> None:
        await self._store.connect()
        await self._store.init_schema()
        self._session_id, self._cursor = await self._store.load_checkpoint()

    async def handle_message(self, message: dict[str, Any], send: FrameSender) -> None:
        """Handle one decoded protocol frame, never accepting invented fields."""
        message_type = message.get("type")
        if message_type == "ping":
            await send({"type": "pong"})
        elif message_type == "connected":
            await self._connected(message, send)
        elif message_type == "reconnected":
            await self._reconnected(message)
        elif message_type == "replay" and message.get("channel") == "allFills":
            await self._replay(message)
        elif message_type == "allFills" and message.get("channel") == "allFills":
            await self._live(message)
        elif message_type == "subscriptionUpdate":
            failed = message.get("failed")
            if isinstance(failed, list) and failed:
                logger.warning("allFills subscription rejected: %s", failed)
            elif isinstance(message.get("warnings"), list) and message["warnings"]:
                logger.warning("allFills subscription warnings: %s", message["warnings"])
            else:
                logger.info("allFills subscription update received")
        elif message_type == "error":
            logger.warning("Hydromancer WebSocket error: %s", message.get("message", "unknown"))
        # Unknown types and additive fields are intentionally ignored.

    @staticmethod
    def _check_live_format(message: dict[str, Any]) -> None:
        if message.get("liveFormat") != LIVE_FORMAT:
            logger.warning("server did not confirm chunked-v1; large batches may drop the connection")

    async def _connected(self, message: dict[str, Any], send: FrameSender) -> None:
        session_id = message.get("sessionId")
        if not isinstance(session_id, str):
            logger.warning("Ignoring connected frame without sessionId")
            return
        self._check_live_format(message)
        if self._session_id is not None:
            # A resume attempt that became connected means session expiry/new session.
            logger.warning("resume did not reconnect; session expired or a gap may exist")
        self._session_id = session_id
        self._last_seq = 0
        self._replay_complete = self._cursor is None
        self._replay_chunks.clear()
        self._next_replay_chunk = 1
        self._replay_total = None
        self._buffered_live.clear()
        self._buffer_deadline = None
        self._reconnect_attempt = 0
        await self._commit([], self._cursor)
        # A subscription cursor recovers the short cache window after a new session.
        # Replay and live delivery can overlap, so buffer live data until replay
        # arrives (or the grace period expires), just as on a resumed session.
        await send(subscribe_frame(self._settings.coins, self._cursor))

    async def _reconnected(self, message: dict[str, Any]) -> None:
        session_id = message.get("sessionId")
        if not isinstance(session_id, str):
            logger.warning("Ignoring reconnected frame without sessionId")
            return
        self._check_live_format(message)
        self._session_id = session_id
        self._last_seq = 0
        self._replay_complete = False
        self._replay_chunks.clear()
        self._next_replay_chunk = 1
        self._replay_total = None
        self._buffered_live.clear()
        self._buffer_deadline = None
        self._reconnect_attempt = 0
        # Save the new session even before the first replay cursor arrives.
        await self._commit([], self._cursor)

    async def _replay(self, message: dict[str, Any]) -> None:
        data = message.get("data")
        cursor = message.get("cursor")
        chunk, total = message.get("chunk"), message.get("totalChunks")
        if not isinstance(data, list) or not isinstance(cursor, str):
            logger.warning("Ignoring malformed allFills replay frame")
            return
        if (not isinstance(chunk, int) or isinstance(chunk, bool) or not isinstance(total, int)
                or isinstance(total, bool) or chunk < 1 or total < chunk):
            logger.warning("Ignoring replay with invalid chunk metadata")
            return
        if self._replay_total is not None and total != self._replay_total:
            logger.warning("Ignoring replay with inconsistent chunk total")
            return
        if chunk < self._next_replay_chunk:
            logger.warning("Ignoring duplicate replay chunk %d", chunk)
            return
        if chunk > self._next_replay_chunk:
            logger.warning(
                "replay chunk order gap: expected %d, got %d; reconnecting",
                self._next_replay_chunk,
                chunk,
            )
            raise ReplayOrderError
        if message.get("hasGap") is True:
            logger.warning("allFills replay reports hasGap=true; market-wide REST repair is unavailable")
        await self._process_entries(data, cursor)
        self._replay_total = total
        self._replay_chunks.add(chunk)
        self._next_replay_chunk += 1
        if len(self._replay_chunks) == total:
            self._replay_complete = True
            self._buffer_deadline = None
            buffered, self._buffered_live = self._buffered_live, []
            for live in buffered:  # Preserve socket arrival order.
                await self._process_live(live)

    async def _live(self, message: dict[str, Any]) -> None:
        if not self._replay_complete:
            now = time.monotonic()
            self._buffer_deadline = self._buffer_deadline or now + REPLAY_GRACE_SECONDS
            if now < self._buffer_deadline:
                if len(self._buffered_live) >= MAX_BUFFERED_LIVE:
                    logger.warning("replay/live buffer limit reached; starting a cursor-based session")
                    # Force a fresh session and manual cursor replay rather than
                    # depending on the overflowing session's delivery state.
                    self._session_id = None
                    self._buffered_live.clear()
                    self._buffer_deadline = None
                    raise ReplayBufferOverflow
                self._buffered_live.append(message)
                return
            logger.warning(
                "no replay within grace after reconnected; flushing %d buffered live frames",
                len(self._buffered_live),
            )
            self._replay_complete = True
            self._buffer_deadline = None
            buffered, self._buffered_live = self._buffered_live, []
            for live in buffered:
                await self._process_live(live)
        await self._process_live(message)

    async def _process_live(self, message: dict[str, Any]) -> None:
        fills, cursor, seq = message.get("fills"), message.get("cursor"), message.get("seq")
        if (not isinstance(fills, list) or not isinstance(cursor, str) or not isinstance(seq, int)
                or isinstance(seq, bool)):
            logger.warning("Ignoring malformed allFills live frame")
            return
        # Sequence numbers increment per chunk across this subscription channel.
        # Chunks from different batchIds may interleave, but the overall stream
        # remains contiguous; a skipped value therefore indicates a lost frame.
        if self._last_seq and seq != self._last_seq + 1:
            logger.warning(
                "allFills sequence gap: expected %d, got %d",
                self._last_seq + 1,
                seq,
            )
        await self._process_entries(fills, cursor)
        self._last_seq = seq

    async def _process_entries(self, entries: list[Any], cursor: str) -> None:
        accepted: list[OrderflowEvent] = []
        rejected: list[dict[str, Any]] = []
        pending_last = self._last_seen
        pending_seen = set(self._seen_at_key)
        for entry in entries:
            if not (isinstance(entry, list) and len(entry) == 2 and isinstance(entry[0], str)
                    and isinstance(entry[1], dict)):
                reason = "invalid entry envelope"
                logger.warning("Quarantining malformed allFills entry: %s", reason)
                rejected.append({"reason": reason, "entry": entry})
                continue
            fill = dict(entry[1])
            canonical_wallet = entry[0].strip().lower()
            fill["address"] = canonical_wallet
            try:
                event_time, tx_index = fill["time"], fill["txIndex"]
                if (not isinstance(event_time, int) or isinstance(event_time, bool)
                        or not isinstance(tx_index, int) or isinstance(tx_index, bool)):
                    raise TypeError("missing time or txIndex")
                key = (event_time, tx_index)
                identity: FillIdentity = (
                    canonical_wallet, self._identity_part(fill.get("tid")),
                    self._identity_part(fill.get("oid")), str(fill.get("side", "")),
                )
                if pending_last is not None and key < pending_last:
                    continue
                if pending_last is not None and key == pending_last and identity in pending_seen:
                    continue
                event = OrderflowEvent.from_hydromancer(fill)
                if pending_last is None or key > pending_last:
                    pending_last, pending_seen = key, set()
            except (ArithmeticError, KeyError, TypeError, ValueError) as exc:
                reason = f"invalid fill: {exc}"
                logger.warning("Quarantining malformed allFills entry: %s", reason)
                rejected.append({"reason": reason, "entry": entry})
                continue
            accepted.append(event)
            pending_seen.add(identity)
        # Valid events, rejects, and the cursor advance share one transaction. A
        # malformed entry therefore remains inspectable without blocking the stream.
        await self._commit(accepted, cursor, rejected)
        # Only committed events/cursors are durable in process memory.
        self._last_seen = pending_last
        self._seen_at_key = pending_seen
        self._cursor = cursor
        await self._maybe_label()

    @staticmethod
    def _identity_part(value: Any) -> str | None:
        return None if value is None else str(value)

    async def _commit(
        self,
        events: list[OrderflowEvent],
        cursor: str | None,
        rejected_entries: list[dict[str, Any]] | None = None,
    ) -> None:
        await self._store.persist_allfills_frame(
            events, self._session_id, cursor, rejected_entries=rejected_entries
        )

    async def _maybe_label(self) -> None:
        now = datetime.now(UTC)
        if now < self._next_label_at or (self._label_task and not self._label_task.done()):
            return
        self._next_label_at = now + timedelta(seconds=self._settings.label_interval_seconds)
        self._label_task = asyncio.create_task(self._label(now))

    async def _label(self, now: datetime) -> None:
        try:
            snapshots = await self._labeler.compute_snapshots(self._store.pool, now)
            await self._store.upsert_label_snapshots(snapshots)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("wallet labeling failed")

    async def _close_on_stop(self, connection: Any, stop_event: asyncio.Event) -> None:
        await stop_event.wait()
        await connection.close()

    async def run_forever(self, stop_event: asyncio.Event | None = None) -> None:
        await self.initialise()
        websocket = HydromancerWebSocket(self._settings.hydromancer_api_key)
        stop_event = stop_event or asyncio.Event()
        try:
            while not stop_event.is_set():
                try:
                    connection = await websocket.connect(self._session_id, self._cursor)
                    async with connection:
                        stopper = asyncio.create_task(self._close_on_stop(connection, stop_event))
                        try:
                            async def send(frame: dict[str, Any], conn: Any = connection) -> None:
                                await websocket.send(conn, frame)

                            async for message in websocket.messages(connection):
                                await self.handle_message(message, send)
                                if stop_event.is_set():
                                    break
                        finally:
                            stopper.cancel()
                            await asyncio.gather(stopper, return_exceptions=True)
                except (
                    OSError,
                    InvalidURI,
                    WebSocketException,
                    asyncpg.PostgresError,
                    ReplayBufferOverflow,
                    ReplayOrderError,
                ):
                    # Transport exceptions can include the token-bearing URL; never log them.
                    logger.warning("allFills connection or database operation failed; retrying")
                    exponent = min(self._reconnect_attempt, MAX_BACKOFF_EXPONENT)
                    delay = min(self._settings.reconnect_delay_seconds * (2 ** exponent), 30.0)
                    self._reconnect_attempt += 1
                    await asyncio.sleep(random.uniform(delay / 2, delay))
        finally:
            if self._label_task:
                self._label_task.cancel()
                await asyncio.gather(self._label_task, return_exceptions=True)
            await self._store.close()
