"""Historical funding retrieval and optional bounded live supplement."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from websockets.exceptions import WebSocketException

from hydromancer_eda.client.rest import HydromancerAPIError, HydromancerRestClient
from hydromancer_eda.client.ws import HydromancerWebSocket
from hydromancer_eda.models import FundingObservation

FUNDING_HISTORY_START = datetime(2025, 7, 28, tzinfo=UTC)


def to_milliseconds(value: datetime) -> int:
    """Convert an aware datetime to Unix milliseconds."""
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = value.astimezone(UTC) - epoch
    return delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000


def fetch_funding_history(
    client: HydromancerRestClient,
    coin: str,
    start: datetime,
    end: datetime,
    *,
    limit: int = 500,
) -> list[FundingObservation]:
    """Fetch exact observations over ``[start, end)`` with overlap/stall protection."""
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("valid timezone-aware half-open interval required")
    if limit <= 0 or limit > 500:
        raise ValueError("funding history limit must be between 1 and 500")
    start_utc, end_utc = start.astimezone(UTC), end.astimezone(UTC)
    if start_utc < FUNDING_HISTORY_START:
        raise ValueError("fundingHistory data is available from 2025-07-28T00:00:00Z")
    start_ms, end_ms = to_milliseconds(start_utc), to_milliseconds(end_utc)
    if start_utc.microsecond % 1000:
        start_ms += 1
    if end_utc.microsecond % 1000:
        end_ms += 1
    cursor_ms = start_ms
    seen: dict[tuple[str, int], Decimal] = {}
    result: list[FundingObservation] = []
    while cursor_ms < end_ms:
        data = client.post(
            {
                "type": "fundingHistory",
                "coin": coin,
                "startTime": cursor_ms,
                # fundingHistory endTime is inclusive; preserve our half-open interval.
                "endTime": end_ms - 1,
                "limit": limit,
            }
        )
        if not isinstance(data, list):
            raise HydromancerAPIError("Hydromancer funding response was not a list")
        if any(not isinstance(row, dict) for row in data):
            raise HydromancerAPIError("Hydromancer funding page contained a malformed row")
        rows = data
        if not rows:
            break
        max_time = cursor_ms - 1
        for row in rows:
            try:
                observation = FundingObservation.from_rest(row)
            except (ArithmeticError, TypeError, ValueError):
                raise HydromancerAPIError("Hydromancer funding page contained an invalid row") from None
            row_ms_raw = row.get("time")
            if not isinstance(row_ms_raw, int) or isinstance(row_ms_raw, bool):
                raise HydromancerAPIError("Hydromancer funding row had an invalid timestamp")
            row_ms = row_ms_raw
            if observation.coin != coin:
                raise HydromancerAPIError("Hydromancer funding response coin mismatch")
            if row_ms >= end_ms:
                raise HydromancerAPIError(
                    "Hydromancer funding row exceeded the requested end boundary"
                )
            max_time = max(max_time, row_ms)
            key = (observation.coin, row_ms)
            if start_utc <= observation.ts < end_utc:
                previous_rate = seen.get(key)
                if previous_rate is not None and previous_rate != observation.rate:
                    raise HydromancerAPIError(
                        "Hydromancer funding pages contained a conflicting duplicate"
                    )
                if previous_rate is None:
                    seen[key] = observation.rate
                    result.append(observation)
        if len(rows) < limit:
            break
        next_ms = max_time + 1
        if next_ms <= cursor_ms:
            raise HydromancerAPIError("Hydromancer funding pagination made no progress")
        cursor_ms = next_ms
    return sorted(result, key=lambda item: item.ts)


async def fetch_current_funding_rates(
    token: str,
    coins: list[str],
    *,
    timeout: float = 8.0,
) -> list[FundingObservation]:
    """Wait at most ``timeout`` for one fundingRates event, ponging every ping."""
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    websocket = HydromancerWebSocket(token)
    connection: Any = None
    wanted = set(coins)

    async def receive() -> list[FundingObservation]:
        nonlocal connection
        connection = await websocket.connect()
        subscribed = False
        async with connection:
            async for message in websocket.messages(connection):
                message_type = message.get("type")
                if message_type == "ping":
                    await websocket.send(connection, {"type": "pong"})
                    continue
                if message_type == "connected":
                    await websocket.send(
                        connection,
                        {"method": "subscribe", "subscription": {"type": "fundingRates"}},
                    )
                    subscribed = True
                    continue
                if message_type == "error":
                    raise HydromancerAPIError("Hydromancer funding WebSocket reported an error")
                if message_type == "subscriptionUpdate":
                    failed = message.get("failed")
                    if isinstance(failed, list) and failed:
                        raise HydromancerAPIError(
                            "Hydromancer funding WebSocket subscription was rejected"
                        )
                    continue
                if message_type != "fundingRates" or not subscribed:
                    continue
                data = message.get("data")
                if not isinstance(data, dict):
                    continue
                timestamp = data.get("timestamp")
                if not isinstance(timestamp, int) or isinstance(timestamp, bool):
                    continue
                rates = data.get("rates")
                if not isinstance(rates, list):
                    continue
                try:
                    ts = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
                        milliseconds=timestamp
                    )
                    observations: list[FundingObservation] = []
                    observed_coins: set[str] = set()
                    invalid_wanted_row = False
                    for row in rates:
                        if not isinstance(row, dict) or row.get("coin") not in wanted:
                            continue
                        coin = row["coin"]
                        rate = row.get("funding_rate")
                        if coin in observed_coins or not isinstance(rate, str):
                            invalid_wanted_row = True
                            break
                        observations.append(
                            FundingObservation(
                                coin=coin,
                                ts=ts,
                                rate=Decimal(rate),
                                source="fundingRates",
                            )
                        )
                        observed_coins.add(coin)
                except (ArithmeticError, TypeError, ValueError):
                    continue
                if invalid_wanted_row or not observations:
                    continue
                return observations
        raise HydromancerAPIError(
            "Hydromancer funding WebSocket ended before a funding observation arrived"
        )

    try:
        return await asyncio.wait_for(receive(), timeout=timeout)
    except TimeoutError:
        if connection is not None:
            try:
                await asyncio.wait_for(connection.close(), timeout=1.0)
            except (TimeoutError, OSError, WebSocketException):
                pass
        raise HydromancerAPIError(
            f"Hydromancer funding WebSocket timed out after {timeout:g}s"
        ) from None
    except HydromancerAPIError:
        raise
    except (ArithmeticError, OSError, TypeError, ValueError, WebSocketException):
        # Never expose exception text because handshake and transport failures may
        # contain the token-bearing URL.
        raise HydromancerAPIError("Hydromancer funding WebSocket failed") from None
