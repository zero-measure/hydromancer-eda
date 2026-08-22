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
    """Build the documented token-bearing mainnet connection URL.

    Args:
        token (str): Hydromancer API token.
        session_id (str | None): Existing session identifier to resume, if any.
        cursor (str | None): Opaque cursor for the last durably processed frame.

    Returns:
        str: URL containing only documented connection query parameters.

    Note:
        The returned URL contains ``token`` and must never be logged.

    """
    params: dict[str, str] = {"token": token, "liveFormat": LIVE_FORMAT}
    if session_id is not None:
        params["sessionId"] = session_id
    if cursor is not None:
        params["cursor"] = cursor
    return f"{WS_ENDPOINT}?{urlencode(params)}"


def subscribe_frame(coins: list[str], cursor: str | None = None) -> dict[str, Any]:
    """Build a case-preserving allFills subscription frame.

    Args:
        coins (list[str]): Case-sensitive Hydromancer coin identifiers.
        cursor (str | None): Opaque cursor from which to request replay.

    Returns:
        dict[str, Any]: JSON-serializable WebSocket subscription frame.

    """
    subscription: dict[str, Any] = {"type": "allFills", "coins": coins}
    if cursor is not None:
        subscription["cursor"] = cursor
    return {"type": "subscribe", "subscription": subscription}


class HydromancerWebSocket:
    """Provide protocol-limited access to the Hydromancer WebSocket.

    The adapter owns the API token and deliberately suppresses library logging
    because WebSocket request URLs contain that token.
    """

    def __init__(self, token: str) -> None:
        """Initialize the adapter with a Hydromancer API token.

        Args:
            token (str): Secret API token used in each connection URL.

        """
        self._token = token

    async def connect(
        self, session_id: str | None = None, cursor: str | None = None
    ) -> ClientConnection:
        """Open a Hydromancer WebSocket connection or resume a session.

        Args:
            session_id (str | None): Existing session identifier to resume.
            cursor (str | None): Opaque cursor for the last committed frame.

        Returns:
            ClientConnection: Open WebSocket client connection.

        Raises:
            OSError: If the network connection cannot be established.
            websockets.exceptions.WebSocketException: If the handshake fails.

        Note:
            Both arguments are embedded in a token-bearing URL that must not be logged.

        """
        # Do not add a log line here: the URL includes the secret token.
        return await websockets.connect(
            connection_url(self._token, session_id, cursor), logger=_WS_LOGGER, max_queue=4096
        )

    @staticmethod
    async def messages(connection: ClientConnection) -> AsyncIterator[dict[str, Any]]:
        """Yield decoded JSON object messages from a connection.

        Args:
            connection (ClientConnection): Open WebSocket connection to consume.

        Yields:
            dict[str, Any]: Decoded JSON objects in socket arrival order.

        Raises:
            websockets.exceptions.WebSocketException: If connection iteration fails.

        Note:
            Binary frames, malformed JSON, and non-object JSON values are skipped.

        """
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
        """Serialize and send one compact JSON protocol frame.

        Args:
            connection (ClientConnection): Open WebSocket connection to use.
            message (dict[str, Any]): JSON-serializable protocol frame.

        Raises:
            TypeError: If ``message`` is not JSON serializable.
            websockets.exceptions.WebSocketException: If the frame cannot be sent.

        """
        await connection.send(json.dumps(message, separators=(",", ":")))
