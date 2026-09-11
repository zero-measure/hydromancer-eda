"""Observable WebSocket replay progress and stall detection."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any


class ReplayProgress:
    """Track one channel's recovery without exposing session IDs or cursors."""

    def __init__(
        self,
        channel: str,
        logger: logging.Logger,
        status_interval_seconds: float,
        stall_timeout_seconds: float,
    ) -> None:
        self.channel = channel
        self.logger = logger
        self.status_interval_seconds = status_interval_seconds
        self.stall_timeout_seconds = stall_timeout_seconds
        self.active = False
        self.stalled = False
        self.started_at = 0.0
        self.last_activity_at = 0.0
        self.chunk = 0
        self.total_chunks: int | None = None
        self.items = 0
        self.has_gap = False

    def start(self, reason: str) -> None:
        """Start a recovery attempt and report why replay is expected."""
        now = time.monotonic()
        self.active = True
        self.stalled = False
        self.started_at = now
        self.last_activity_at = now
        self.chunk = 0
        self.total_chunks = None
        self.items = 0
        self.has_gap = False
        self.logger.info(
            "%s backfill started (%s); waiting for replay chunks",
            self.channel,
            reason,
        )

    def advance(
        self,
        chunk: int,
        total_chunks: int,
        item_count: int,
        has_gap: bool,
        buffered_live: int,
    ) -> None:
        """Record and display a durably processed replay chunk."""
        self.last_activity_at = time.monotonic()
        self.chunk = chunk
        self.total_chunks = total_chunks
        self.items += item_count
        self.has_gap = self.has_gap or has_gap
        percent = min(100, int(chunk * 100 / total_chunks))
        width = 20
        complete = min(width, int(width * chunk / total_chunks))
        bar = "=" * complete + "-" * (width - complete)
        self.logger.info(
            "%s backfill progress [%s] %d/%d (%d%%); %d items persisted, "
            "%d live frames buffered",
            self.channel,
            bar,
            chunk,
            total_chunks,
            percent,
            self.items,
            buffered_live,
        )

    def finish(self, buffered_live: int, reason: str = "replay complete") -> None:
        """Mark recovery complete and report its outcome."""
        if not self.active:
            return
        elapsed = time.monotonic() - self.started_at
        level = logging.ERROR if self.has_gap else logging.INFO
        suffix = "; DATA GAP REMAINS because market-wide REST repair is unavailable" if self.has_gap else ""
        self.logger.log(
            level,
            "%s backfill finished in %.1fs (%s); %d items persisted, "
            "%d buffered live frames ready%s",
            self.channel,
            elapsed,
            reason,
            self.items,
            buffered_live,
            suffix,
        )
        self.active = False

    def fail(self, reason: str) -> None:
        """End the current attempt with an explicit recoverable failure."""
        if not self.active:
            return
        elapsed = time.monotonic() - self.started_at
        self.logger.error(
            "%s backfill attempt failed after %.1fs (%s); %d/%s chunks and %d items "
            "were persisted and the next attempt will resume from the durable cursor",
            self.channel,
            elapsed,
            reason,
            self.chunk,
            self.total_chunks if self.total_chunks is not None else "?",
            self.items,
        )
        self.active = False

    async def monitor(
        self,
        connection: Any,
        stop_event: asyncio.Event,
        buffered_live: Callable[[], int],
        no_replay_grace_seconds: float | None = None,
        on_no_replay: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        """Emit status, resolve empty replay, and close a stalled connection."""
        while not stop_event.is_set():
            await asyncio.sleep(self.status_interval_seconds)
            if not self.active:
                continue
            now = time.monotonic()
            idle = now - self.last_activity_at
            elapsed = now - self.started_at
            if (
                self.chunk == 0
                and no_replay_grace_seconds is not None
                and on_no_replay is not None
                and elapsed >= no_replay_grace_seconds
            ):
                try:
                    await on_no_replay()
                except Exception:
                    await connection.close()
                    raise
                continue
            if idle >= self.stall_timeout_seconds:
                self.stalled = True
                self.active = False
                self.logger.error(
                    "%s backfill stalled: no replay progress for %.1fs after %.1fs total; "
                    "closing the connection to retry from the durable cursor",
                    self.channel,
                    idle,
                    elapsed,
                )
                await connection.close()
                return
            chunk_status = (
                f"{self.chunk}/{self.total_chunks}"
                if self.total_chunks is not None
                else "waiting for first chunk"
            )
            self.logger.info(
                "%s backfill status: %s, %d items persisted, %d live frames buffered, "
                "%.1fs since progress",
                self.channel,
                chunk_status,
                self.items,
                buffered_live(),
                idle,
            )
