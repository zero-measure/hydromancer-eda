import asyncio
import logging
import time
from importlib.resources import files

import pytest

from hydromancer_eda.client.ws import LIVE_FORMAT, connection_url, subscribe_frame
from hydromancer_eda.config import Settings
from hydromancer_eda.ingestion.allfills import (
    MAX_BUFFERED_LIVE,
    AllFillsIngestionService,
    ReplayBufferOverflow,
    ReplayOrderError,
)
from hydromancer_eda.models import OrderflowEvent


class Store:
    def __init__(self):
        self.frames = []
        self.pool = object()

    async def persist_allfills_frame(
        self, events, session_id, cursor, channel="allFills", rejected_entries=None
    ):
        self.frames.append((events, session_id, cursor, channel, rejected_entries or []))
        return len(events)


def settings() -> Settings:
    return Settings(HYDROMANCER_API_KEY="secret-token", PG_DSN="postgresql://example/db", HYDROMANCER_COINS="BTC,Eth")


def fill(time=10, tx_index=1, address="0xabc", tid=1, oid=2, coin="BTC"):
    return [address, {"coin": coin, "px": "1", "sz": "2", "side": "B", "time": time,
                      "txIndex": tx_index, "tid": tid, "oid": oid}]


def quiet(service):
    service._next_label_at = service._next_label_at.replace(year=9999)


def test_url_is_secret_in_logs_and_only_uses_documented_query_parameters(caplog):
    with caplog.at_level(logging.DEBUG):
        url = connection_url("secret-token", "session", "opaque:cursor")
    assert "secret-token" in url
    assert set(url.split("?", 1)[1].split("&")) == {
        "token=secret-token", "sessionId=session", "cursor=opaque%3Acursor", f"liveFormat={LIVE_FORMAT}"
    }
    assert "secret-token" not in caplog.text


def test_subscribe_frame_keeps_coin_case_and_can_resume_from_cursor():
    assert subscribe_frame(["BTC", "Eth"], "opaque:cursor") == {
        "type": "subscribe", "subscription": {
            "type": "allFills", "coins": ["BTC", "Eth"], "cursor": "opaque:cursor"
        }
    }


def test_connected_subscribes_with_persisted_cursor():
    async def run():
        store, sent = Store(), []
        async def send(frame):
            sent.append(frame)
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        service._cursor = "saved:cursor"
        await service.handle_message({"type": "connected", "sessionId": "new", "liveFormat": LIVE_FORMAT}, send)
        assert sent == [subscribe_frame(["BTC", "Eth"], "saved:cursor")]
    asyncio.run(run())


def test_ping_replay_buffering_dedupe_and_cursor_checkpoint():
    async def run():
        store, sent = Store(), []
        async def send(frame):
            sent.append(frame)
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        await service.handle_message({"type": "connected", "sessionId": "new", "liveFormat": LIVE_FORMAT}, send)
        await service.handle_message({"type": "ping"}, send)
        await service.handle_message({"type": "reconnected", "sessionId": "resumed", "liveFormat": LIVE_FORMAT}, send)
        await service.handle_message({"type": "allFills", "channel": "allFills", "seq": 1, "cursor": "live", "fills": [fill(11)], "chunk": 1, "totalChunks": 2, "batchId": 7}, send)
        await service.handle_message({"type": "replay", "channel": "allFills", "cursor": "replay", "chunk": 1, "totalChunks": 1, "data": [fill(10)]}, send)
        assert sent == [subscribe_frame(["BTC", "Eth"]), {"type": "pong"}]
        assert [frame[2] for frame in store.frames] == [None, None, "replay", "live"]
        assert [event.tx_index for frame in store.frames for event in frame[0]] == [1, 1]
    asyncio.run(run())


def test_same_transaction_retains_each_market_wide_participant():
    async def run():
        store, sent = Store(), []
        async def send(frame):
            sent.append(frame)
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        await service.handle_message({"type": "connected", "sessionId": "s", "liveFormat": LIVE_FORMAT}, send)
        fills = [fill(address="0xbuyer", tid=99, oid=1), fill(address="0xseller", tid=99, oid=2)]
        await service.handle_message({"type": "allFills", "channel": "allFills", "seq": 1, "cursor": "one", "fills": fills}, send)
        events = store.frames[-1][0]
        assert [event.wallet for event in events] == ["0xbuyer", "0xseller"]
    asyncio.run(run())


def test_reconnected_without_replay_flushes_buffered_live_frames():
    async def run():
        store, sent = Store(), []
        async def send(frame):
            sent.append(frame)
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        await service.handle_message({"type": "connected", "sessionId": "s", "liveFormat": LIVE_FORMAT}, send)
        await service.handle_message({"type": "reconnected", "sessionId": "s", "liveFormat": LIVE_FORMAT}, send)
        await service.handle_message({"type": "allFills", "channel": "allFills", "seq": 1, "cursor": "one", "fills": [fill(11)]}, send)
        service._buffer_deadline = time.monotonic() - 1
        await service.handle_message({"type": "allFills", "channel": "allFills", "seq": 2, "cursor": "two", "fills": [fill(12)]}, send)
        assert [frame[2] for frame in store.frames] == [None, None, "one", "two"]
        assert [event.ts for frame in store.frames for event in frame[0]]
        assert service._replay_complete and not service._buffered_live
    asyncio.run(run())


def test_missing_live_sequence_reconnects_without_advancing_cursor(caplog):
    async def run():
        store, sent = Store(), []

        async def send(frame):
            sent.append(frame)

        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        await service.handle_message(
            {"type": "connected", "sessionId": "s", "liveFormat": LIVE_FORMAT}, send
        )
        await service.handle_message(
            {"type": "allFills", "channel": "allFills", "seq": 1, "cursor": "one", "fills": [fill()]}, send
        )
        with pytest.raises(ReplayOrderError):
            await service.handle_message(
                {"type": "allFills", "channel": "allFills", "seq": 3, "cursor": "two", "fills": [fill(11)]},
                send,
            )
        assert store.frames[-1][2] == "one"

    with caplog.at_level(logging.WARNING):
        asyncio.run(run())
    assert "sequence gap; reconnecting" in caplog.text


def test_only_liquidated_participant_is_flagged() -> None:
    details = {
        "liquidatedUser": "0xvictim",
        "markPx": "90",
        "method": "market",
    }
    victim_payload = dict(fill(address="0xvictim")[1], address="0xvictim", liquidation=details)
    counterparty_payload = dict(
        fill(address="0xmaker")[1], address="0xmaker", liquidation=details
    )

    victim = OrderflowEvent.from_hydromancer(victim_payload)
    counterparty = OrderflowEvent.from_hydromancer(counterparty_payload)

    assert victim.liquidation is True
    assert victim.liquidation_mark_price == 90
    assert counterparty.liquidation is False
    assert counterparty.liquidation_mark_price is None


def test_case_sensitive_coin_identity_is_preserved():
    payload = dict(fill(coin="xyz:SP500")[1], address="0xabc")
    event = OrderflowEvent.from_hydromancer(payload)
    assert event.symbol == "xyz:SP500"


def test_bad_entry_and_undocumented_live_field_shape_are_ignored():
    async def run():
        store, sent = Store(), []
        async def send(frame):
            sent.append(frame)
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        await service.handle_message({"type": "connected", "sessionId": "s", "liveFormat": LIVE_FORMAT}, send)
        await service.handle_message({"type": "allFills", "channel": "allFills", "seq": 1, "cursor": "x", "filled": [fill()]}, send)
        assert len(store.frames) == 1
    asyncio.run(run())


def test_malformed_fill_is_quarantined_with_cursor_progress():
    async def run():
        store = Store()
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        bad_entry = ["0xabc", {"coin": "BTC"}]
        await service._process_entries([bad_entry], "quarantined")
        events, _, cursor, _, rejects = store.frames[-1]
        assert events == []
        assert cursor == "quarantined"
        assert rejects == [{"reason": "invalid fill: 'time'", "entry": bad_entry}]
        assert service._cursor == "quarantined"

    asyncio.run(run())


def test_new_session_resets_stale_replay_state_and_buffers_live_when_resuming():
    async def run():
        store, sent = Store(), []

        async def send(frame):
            sent.append(frame)

        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        service._cursor = "saved"
        service._replay_chunks = {1}
        service._replay_total = 7
        service._buffered_live = [{"stale": True}]
        await service.handle_message(
            {"type": "connected", "sessionId": "new", "liveFormat": LIVE_FORMAT}, send
        )
        assert not service._replay_complete
        assert service._replay_chunks == set()
        assert service._replay_total is None
        assert service._buffered_live == []

    asyncio.run(run())


def test_envelope_wallet_is_canonical_for_storage_and_dedupe():
    async def run():
        store = Store()
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        entry = fill(address="0xAbC")
        entry[1]["user"] = "0xwrong"
        await service._process_entries([entry], "one")
        event = store.frames[-1][0][0]
        assert event.wallet == "0xabc"
        assert event.raw["address"] == "0xabc"

    asyncio.run(run())


def test_replay_chunk_gap_reconnects_without_advancing_cursor():
    async def run():
        store = Store()
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        service._replay_complete = False
        message = {
            "type": "replay",
            "channel": "allFills",
            "cursor": "unsafe",
            "chunk": 2,
            "totalChunks": 2,
            "data": [fill()],
        }
        with pytest.raises(ReplayOrderError):
            await service._replay(message)
        assert store.frames == []
        assert service._cursor is None

    asyncio.run(run())


def test_live_buffer_overflow_forces_fresh_cursor_based_session():
    async def run():
        store = Store()
        service = AllFillsIngestionService(settings(), store)
        quiet(service)
        service._session_id = "overflowing-session"
        service._replay_complete = False
        service._buffered_live = [{}] * MAX_BUFFERED_LIVE
        message = {
            "type": "allFills",
            "channel": "allFills",
            "seq": 1,
            "cursor": "live",
            "fills": [fill()],
        }
        with pytest.raises(ReplayBufferOverflow):
            await service._live(message)
        assert service._session_id is None
        assert service._buffered_live == []
        assert service._cursor is None

    asyncio.run(run())


def test_packaged_schema_is_available():
    schema = files("hydromancer_eda.sql").joinpath("schema.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS orderflow_events" in schema
