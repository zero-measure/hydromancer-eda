import asyncio
import struct
from collections import deque
from datetime import UTC, datetime
from decimal import Decimal

import msgpack
import zstandard

from hydromancer_eda.config import Settings
from hydromancer_eda.ingestion import marketdata
from hydromancer_eda.ingestion.marketdata import (
    BookState,
    L2BookHandler,
    L4BookHandler,
    LeverageHandler,
    RestingOrder,
)


class Store:
    def __init__(self) -> None:
        self.l2 = []
        self.l2_changes = []
        self.l2_states = []
        self.l4 = []
        self.leverage = []
        self.snapshot_args = None

    async def persist_l2_ticks(
        self, ticks, level_changes, states, _session, _cursor
    ):
        self.l2.extend(ticks)
        self.l2_changes.extend(level_changes)
        self.l2_states.extend(states)

    async def persist_l4_events(self, events, _session, _cursor):
        self.l4.extend(events)

    async def persist_leverage_updates(self, updates, _session, _cursor):
        self.leverage.extend(updates)

    async def load_l2_book_state(self, _symbol):
        return None

    async def next_l2_event_number(self, _symbol):
        return 1

    async def recent_l2_mids(self, _symbol, _limit):
        return []

    async def replace_l2_book_state(self, *args):
        self.snapshot_args = args


def settings() -> Settings:
    return Settings(
        HYDROMANCER_API_KEY="test",
        PG_DSN="postgresql://example/test",
        HYDROMANCER_COINS="COIN",
        L2_VOLATILITY_MIN_SAMPLES=2,
    )


def test_documented_positional_l2_snapshot_bootstraps_book(monkeypatch) -> None:
    snapshot = [
        "COIN",
        1_735_689_600_000,
        [[["ignored"]], []],
        6,
        "epoch",
        4,
    ]
    snapshot[2] = [
        [["99", "5", 1]],
        [["101", "3", 1]],
    ]
    monkeypatch.setattr(
        marketdata, "_post_snapshot", lambda *_args: msgpack.packb(snapshot)
    )
    store = Store()
    handler = L2BookHandler(settings(), store)  # type: ignore[arg-type]

    asyncio.run(handler.ensure_ready())

    assert handler.states["COIN"].source_seq == 4
    assert handler.states["COIN"].height == 6
    assert handler.states["COIN"].event_no == 0
    assert store.snapshot_args is not None


def test_l2_counts_one_event_per_coin_diff_and_builds_compact_tick() -> None:
    store = Store()
    handler = L2BookHandler(settings(), store)  # type: ignore[arg-type]
    handler.states["COIN"] = BookState(
        epoch="epoch",
        source_seq=4,
        height=6,
        event_no=10,
        bids={Decimal(99): (Decimal(5), 1)},
        asks={Decimal(101): (Decimal(3), 1)},
        mids=deque([99.0, 100.0], maxlen=1001),
    )
    payload = {
        "data": {
            "height": 7,
            "time": 1_735_689_600_000,
            "diffs": [
                {
                    "coin": "COIN",
                    "epoch": "epoch",
                    "seq": 5,
                    "prev_seq": 4,
                    "levels": [[{"px": "100", "sz": "4", "n": 1}], []],
                }
            ],
        }
    }

    asyncio.run(handler.process(payload, "opaque", "session"))

    assert len(store.l2) == 1
    assert store.l2[0].event_no == 11
    assert store.l2[0].best_bid == Decimal(100)
    assert store.l2[0].best_ask == Decimal(101)
    assert store.l2[0].trailing_event_volatility is not None
    assert store.l2_changes == [("COIN", "bid", Decimal(100), Decimal(4), 1)]
    assert store.l2_states[0][:5] == ("COIN", "epoch", 5, 7, 11)

    # A replay/live overlap at the same per-coin sequence is a no-op.
    asyncio.run(handler.process(payload, "opaque-2", "session"))
    assert len(store.l2) == 1
    assert handler.states["COIN"].event_no == 11


def test_documented_l4_binary_snapshot_seeds_untracked_orders(monkeypatch) -> None:
    market = ["COIN", [["0xABC", "100", "2", 42]], []]
    blob = zstandard.ZstdCompressor().compress(msgpack.packb(market))
    raw = struct.pack("<IQQI", 1, 8, 1_735_689_600_000, len(blob)) + blob
    monkeypatch.setattr(marketdata, "_post_snapshot", lambda *_args: raw)
    handler = L4BookHandler(settings(), Store())  # type: ignore[arg-type]

    asyncio.run(handler.ensure_ready())

    assert handler.snapshot_height == 8
    assert handler.orders["42"].wallet == "0xabc"
    assert handler.orders["42"].tracked is False


def test_l4_remove_uses_seeded_wallet_and_order_lifetime() -> None:
    store = Store()
    handler = L4BookHandler(settings(), store)  # type: ignore[arg-type]
    opened = datetime(2025, 1, 1, tzinfo=UTC)
    handler.ready = True
    handler.orders["42"] = RestingOrder(
        ts=opened,
        symbol="COIN",
        wallet="0xabc",
        side="buy",
        price=Decimal(100),
        size=Decimal(1),
        tracked=False,
    )
    payload = {
        "data": {
            "height": 8,
            "timestamp": int((opened.timestamp() + 10) * 1000),
            "diffs": [{"type": "remove", "coin": "COIN", "oid": 42}],
        }
    }

    asyncio.run(handler.process(payload, "opaque", "session"))

    assert len(store.l4) == 1
    assert store.l4[0].wallet == "0xabc"
    assert store.l4[0].resting_seconds == 10
    assert store.l4[0].aggregate is False
    assert "42" not in handler.orders


def test_leverage_handler_keeps_explicit_and_target_leverage() -> None:
    store = Store()
    handler = LeverageHandler(settings(), store)  # type: ignore[arg-type]
    payload = {
        "updates": [
            {
                "update_type": "leverage",
                "time": 1_735_689_600_000,
                "user": "0xABC",
                "coin": "COIN",
                "leverage": 20,
                "is_cross": True,
                "tx_index": 1,
            },
            {
                "update_type": "isolated_margin",
                "time": 1_735_689_601_000,
                "user": "0xABC",
                "coin": "COIN",
                "is_buy": False,
                "ntli": "0.25",
                "tx_index": 2,
            },
            {
                "update_type": "top_up_isolated_margin",
                "time": 1_735_689_602_000,
                "user": "0xABC",
                "coin": "COIN",
                "target_leverage": "8",
                "tx_index": 3,
            },
        ]
    }

    asyncio.run(handler.process(payload, "opaque", "session"))

    assert [update.leverage for update in store.leverage] == [20, None, 8]
    assert store.leverage[0].wallet == "0xabc"
    assert store.leverage[0].is_cross is True
    assert store.leverage[1].ntli == Decimal("0.25")
    assert store.leverage[1].is_buy is False
    assert store.leverage[2].is_cross is False
