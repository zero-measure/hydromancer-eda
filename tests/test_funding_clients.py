from __future__ import annotations

from datetime import UTC, datetime
from email.message import Message
from email.utils import format_datetime
from typing import ClassVar
from urllib.error import HTTPError

import pytest

import hydromancer_eda.client.rest as rest_module
import hydromancer_eda.funding as funding_module
from hydromancer_eda.client.rest import HydromancerAPIError, HydromancerRestClient


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def read(self):
        return b"[]"


def test_rest_retries_rate_limit_and_honors_bounded_retry_after(monkeypatch) -> None:
    calls = 0
    sleeps = []

    def fake_urlopen(request, timeout):
        nonlocal calls
        calls += 1
        assert timeout == 2
        if calls == 1:
            headers = Message()
            headers["Retry-After"] = "999"
            raise HTTPError("https://secret.invalid/?token=SECRET", 429, "limited", headers, None)
        return Response()

    monkeypatch.setattr(rest_module, "urlopen", fake_urlopen)
    client = HydromancerRestClient("SECRET", timeout=2, max_retries=1, sleep=sleeps.append)
    assert client.post({"type": "fundingHistory"}) == []
    assert sleeps == [30.0]


def test_rest_honors_retry_after_http_date(monkeypatch) -> None:
    calls = 0
    sleeps = []
    now = datetime(2025, 1, 1, tzinfo=UTC)

    def fake_urlopen(request, timeout):
        nonlocal calls
        calls += 1
        if calls == 1:
            headers = Message()
            headers["Retry-After"] = format_datetime(now.replace(second=17), usegmt=True)
            raise HTTPError("https://secret.invalid/?token=SECRET", 503, "busy", headers, None)
        return Response()

    monkeypatch.setattr(rest_module, "urlopen", fake_urlopen)
    monkeypatch.setattr(rest_module.time, "time", lambda: now.timestamp())
    client = HydromancerRestClient("SECRET", max_retries=1, sleep=sleeps.append)
    assert client.post({}) == []
    assert sleeps == [17.0]


def test_rest_errors_are_sanitized(monkeypatch) -> None:
    def fail(request, timeout):
        raise HTTPError("https://host/?token=SECRET", 401, "bad SECRET", Message(), None)

    monkeypatch.setattr(rest_module, "urlopen", fail)
    with pytest.raises(HydromancerAPIError) as caught:
        HydromancerRestClient("SECRET", max_retries=0).post({})
    assert "SECRET" not in str(caught.value)
    assert "https" not in str(caught.value)


class FakeConnection:
    def __init__(self):
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def close(self):
        self.closed = True


class FakeWebSocket:
    sent: ClassVar[list[dict[str, object]]] = []

    def __init__(self, token):
        assert token == "SECRET"
        self.connection = FakeConnection()

    async def connect(self):
        return self.connection

    async def send(self, connection, message):
        self.sent.append(message)

    async def messages(self, connection):
        yield {"type": "connected", "sessionId": "session"}
        yield {"type": "ping"}
        yield {"type": "fundingRates", "data": {"timestamp": 1704067200007, "rates": [
            {"coin": "BTC", "funding_rate": "0.000000000123"},
            {"coin": "btc", "funding_rate": "9"},
        ]}}


@pytest.mark.asyncio
async def test_funding_ws_pongs_filters_case_and_keeps_exact_timestamp(monkeypatch) -> None:
    FakeWebSocket.sent = []
    monkeypatch.setattr(funding_module, "HydromancerWebSocket", FakeWebSocket)
    result = await funding_module.fetch_current_funding_rates("SECRET", ["BTC"], timeout=1)
    assert FakeWebSocket.sent == [
        {"method": "subscribe", "subscription": {"type": "fundingRates"}},
        {"type": "pong"},
    ]
    assert [item.coin for item in result] == ["BTC"]
    assert result[0].ts.microsecond == 7000
    assert str(result[0].rate) == "1.23E-10"


class NoisyWebSocket(FakeWebSocket):
    async def messages(self, connection):
        yield {"type": "connected", "sessionId": "session"}
        yield {"type": "subscriptionUpdate"}
        yield {"type": "fundingRates", "data": "bad"}
        yield {"type": "fundingRates", "data": {"timestamp": True, "rates": []}}
        yield {"type": "fundingRates", "data": {
            "timestamp": 1704067200000,
            "rates": [{"coin": "ETH", "funding_rate": "0.1"}],
        }}
        yield {"type": "ping"}
        yield {"type": "fundingRates", "data": {
            "timestamp": 1704067200000,
            "rates": [{"coin": "BTC", "funding_rate": 0.1}],
        }}
        yield {"type": "fundingRates", "data": {
            "timestamp": 1704067200007,
            "rates": [{"coin": "BTC", "funding_rate": "0.0002"}],
        }}


@pytest.mark.asyncio
async def test_funding_ws_waits_through_unrelated_and_invalid_messages(monkeypatch) -> None:
    NoisyWebSocket.sent = []
    monkeypatch.setattr(funding_module, "HydromancerWebSocket", NoisyWebSocket)
    result = await funding_module.fetch_current_funding_rates("SECRET", ["BTC"], timeout=1)
    assert [item.coin for item in result] == ["BTC"]
    assert result[0].ts.microsecond == 7000
    assert result[0].rate == funding_module.Decimal("0.0002")
    assert {"type": "pong"} in NoisyWebSocket.sent


class HangingWebSocket(FakeWebSocket):
    async def messages(self, connection):
        import asyncio
        await asyncio.sleep(60)
        if False:
            yield {}


@pytest.mark.asyncio
async def test_funding_ws_timeout_is_bounded(monkeypatch) -> None:
    websocket = HangingWebSocket("SECRET")
    monkeypatch.setattr(funding_module, "HydromancerWebSocket", lambda token: websocket)
    with pytest.raises(HydromancerAPIError, match="timed out"):
        await funding_module.fetch_current_funding_rates("SECRET", ["BTC"], timeout=0.01)
    assert websocket.connection.closed


@pytest.mark.parametrize(
    "last_row",
    [
        {"time": 10},
        {"txIndex": 2},
        {"time": True, "txIndex": 2},
        {"time": 10, "txIndex": False},
        {"time": "10", "txIndex": 2},
    ],
)
def test_user_fills_requires_documented_cursor_fields_on_full_page(
    monkeypatch, last_row
) -> None:
    client = HydromancerRestClient("SECRET")
    monkeypatch.setattr(client, "post", lambda payload: [last_row])
    iterator = client.user_fills_pages("0xabc", 0, 20, limit=1)
    assert next(iterator) == [last_row]
    with pytest.raises(HydromancerAPIError, match="documented cursor fields"):
        next(iterator)


def test_user_fills_derives_documented_cursor_from_last_record(monkeypatch) -> None:
    client = HydromancerRestClient("SECRET")
    payloads = []
    pages = iter([[{"time": 10, "txIndex": 2}], []])

    def post(payload):
        payloads.append(payload)
        return next(pages)

    monkeypatch.setattr(client, "post", post)
    assert list(client.user_fills_pages("0xabc", 0, 20, limit=1))
    assert payloads[0]["startTime"] == 0
    assert payloads[1]["cursor"] == "10_2"
    assert "startTime" not in payloads[1]
