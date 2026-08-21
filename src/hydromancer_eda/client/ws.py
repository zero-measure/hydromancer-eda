"""Small, protocol-limited Hydromancer WebSocket adapter.

The URL is intentionally constructed only here and is never logged: it contains
an API token. Only documented connection query parameters are emitted.
"""
from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode

import websockets
from websockets.asyncio.client import ClientConnection

WS_ENDPOINT = "wss://api.hydromancer.xyz/ws"
LIVE_FORMAT = "chunked-v1"

# websockets logs request targets at DEBUG. The target contains the API token.
_WS_LOGGER = logging.getLogger("hydromancer.ws.silenced")
_WS_LOGGER.disabled = True


def connection_url(token: str, session_id: str | None = None, cursor: str | None = None) -> str:
    """Build the sole documented mainnet URL without exposing it to logging."""
    params: dict[str, str] = {"token": token, "liveFormat": LIVE_FORMAT}
    if session_id is not None:
        params["sessionId"] = session_id
    if cursor is not None:
        params["cursor"] = cursor
    return f"{WS_ENDPOINT}?{urlencode(params)}"


def subscribe_frame(coins: list[str], cursor: str | None = None) -> dict[str, Any]:
    """Build the one allFills subscription, retaining protocol coin casing."""
    subscription: dict[str, Any] = {"type": "allFills", "coins": coins}
    if cursor is not None:
        subscription["cursor"] = cursor
    return {"type": "subscribe", "subscription": subscription}


class HydromancerWebSocket:
    def __init__(self, token: str) -> None:
        self._token = token

    async def connect(
        self, session_id: str | None = None, cursor: str | None = None
    ) -> ClientConnection:
        # Do not add a log line here: the URL includes the secret token.
        return await websockets.connect(
            connection_url(self._token, session_id, cursor), logger=_WS_LOGGER, max_queue=4096
        )

    @staticmethod
    async def messages(connection: ClientConnection) -> AsyncIterator[dict[str, Any]]:
        async for raw in connection:
            if not isinstance(raw, str):
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(message, dict):
                yield message

    @staticmethod
    async def send(connection: ClientConnection, message: dict[str, Any]) -> None:
        await connection.send(json.dumps(message, separators=(",", ":")))
