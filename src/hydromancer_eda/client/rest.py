"""Bounded, retrying Hydromancer REST access without secret-bearing errors."""
from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Iterator
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

INFO_ENDPOINT = "https://api.hydromancer.xyz/info"
RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class HydromancerAPIError(RuntimeError):
    """Sanitized Hydromancer request failure (never includes token or URL)."""


class HydromancerRestClient:
    """Small synchronous JSON client with bounded exponential retries."""

    def __init__(
        self,
        token: str,
        *,
        timeout: float = 15.0,
        max_retries: int = 4,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout <= 0 or max_retries < 0:
            raise ValueError("timeout must be positive and max_retries non-negative")
        self._token = token
        self._timeout = timeout
        self._max_retries = max_retries
        self._sleep = sleep

    def post(self, payload: dict[str, Any]) -> Any:
        """POST JSON, retrying throttling/transient failures with sanitized errors."""
        body = json.dumps(payload, separators=(",", ":")).encode()
        request = Request(
            INFO_ENDPOINT,
            data=body,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "hydromancer-eda/0.1",
            },
            method="POST",
        )
        for attempt in range(self._max_retries + 1):
            try:
                with urlopen(request, timeout=self._timeout) as response:
                    return json.loads(response.read())
            except HTTPError as exc:
                if exc.code not in RETRYABLE_STATUS or attempt == self._max_retries:
                    raise HydromancerAPIError(f"Hydromancer REST failed (HTTP {exc.code})") from None
                retry_after = exc.headers.get("Retry-After")
                delay = self._retry_delay(attempt, retry_after)
            except (TimeoutError, URLError, OSError):
                if attempt == self._max_retries:
                    raise HydromancerAPIError("Hydromancer REST failed after bounded retries") from None
                delay = self._retry_delay(attempt, None)
            except (json.JSONDecodeError, UnicodeError):
                raise HydromancerAPIError("Hydromancer REST returned invalid JSON") from None
            self._sleep(delay)
        raise AssertionError("unreachable")

    @staticmethod
    def _retry_delay(attempt: int, retry_after: str | None) -> float:
        if retry_after is not None:
            try:
                delay = float(retry_after)
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        raise ValueError("Retry-After HTTP-date lacked a timezone")
                    delay = retry_at.timestamp() - time.time()
                except (OverflowError, OSError, TypeError, ValueError):
                    delay = -1.0
            if delay >= 0:
                return min(delay, 30.0)
        ceiling = min(0.5 * (2**attempt), 8.0)
        return random.uniform(ceiling / 2, ceiling)

    def user_fills_pages(
        self, wallet: str, start_ms: int, end_ms_inclusive: int, *, limit: int = 2000
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield documented userFillsByTime pages using its composite continuation.

        Hydromancer documents the continuation cursor as ``time_txIndex`` from the
        last record of a full page. Both source fields must be present with their
        documented integer types; after construction, the cursor string is passed
        back unchanged and never parsed.
        """
        if limit <= 0 or start_ms > end_ms_inclusive:
            raise ValueError("invalid user fills pagination bounds")
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            payload: dict[str, Any] = {
                "type": "userFillsByTime",
                "user": wallet,
                "endTime": end_ms_inclusive,
                "limit": limit,
            }
            if cursor is None:
                payload["startTime"] = start_ms
            else:
                payload["cursor"] = cursor
            data = self.post(payload)
            if not isinstance(data, list):
                raise HydromancerAPIError("Hydromancer user fills response was not a list")
            if any(not isinstance(row, dict) for row in data):
                raise HydromancerAPIError("Hydromancer user fills page contained a malformed row")
            page = data
            if page:
                yield page
            if len(page) < limit:
                return
            last_time = page[-1].get("time")
            last_tx_index = page[-1].get("txIndex")
            if (
                not isinstance(last_time, int)
                or isinstance(last_time, bool)
                or not isinstance(last_tx_index, int)
                or isinstance(last_tx_index, bool)
            ):
                raise HydromancerAPIError(
                    "Hydromancer user fills full page omitted documented cursor fields"
                )
            next_cursor = f"{last_time}_{last_tx_index}"
            if next_cursor in seen:
                raise HydromancerAPIError("Hydromancer user fills pagination made no progress")
            seen.add(next_cursor)
            cursor = next_cursor
