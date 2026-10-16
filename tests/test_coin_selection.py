"""Coin selection applies to live/replay records, not only subscriptions."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from hydromancer_eda.config import Settings
from hydromancer_eda.ingestion.allfills import AllFillsIngestionService
from hydromancer_eda.ingestion.marketdata import (
    BookState,
    L2BookHandler,
    L4BookHandler,
    LeverageHandler,
)


class Store:
    def __init__(self) -> None:
        self.fills: list[Any] = []
        self.rejects: list[Any] = []
        self.ticks: list[Any] = []
        self.orders: list[Any] = []
        self.leverage: list[Any] = []
        self.cursor: str | None = None

    async def persist_allfills_frame(
        self, events, _session, cursor, rejected_entries=None
    ):
        self.fills.extend(events)
        self.rejects.extend(rejected_entries or [])
        self.cursor = cursor

    async def persist_l2_ticks(self, ticks, _changes, _states, _session, cursor):
        self.ticks.extend(ticks)
        self.cursor = cursor

    async def persist_l4_events(self, events, _session, cursor):
        self.orders.extend(events)
        self.cursor = cursor

    async def persist_leverage_updates(self, updates, _session, cursor):
        self.leverage.extend(updates)
        self.cursor = cursor

    async def persist_channel_cursor(self, _channel, _session, cursor):
        self.cursor = cursor


def settings() -> Settings:
    return Settings(
        _env_file=None,
        HYDROMANCER_API_KEY="test",
        PG_DSN="postgresql://example/test",
        HYDROMANCER_COINS="SOL,BTC,ETH",
    )


def test_allfills_filters_before_normalization_and_keeps_opaque_cursor() -> None:
    async def run():
        store = Store()
        service = AllFillsIngestionService(settings(), store)  # type: ignore[arg-type]
        service._next_label_at = datetime(9999, 1, 1, tzinfo=UTC)
        entries = [
            ["0xabc", {"coin": "CASHCAT"}],
            ["0xabc", {"coin": "btc"}],
        ]
        for index, coin in enumerate(settings().coins):
            entries.append(["0xabc", {
                "coin": coin, "time": 1000, "txIndex": index,
                "px": "10", "sz": "1", "side": "B",
            }])
        await service._process_entries(entries, "opaque:selection")
        assert [event.symbol for event in store.fills] == ["SOL", "BTC", "ETH"]
        assert store.rejects == []
        # A frame containing only excluded coins still advances its checkpoint.
        await service._process_entries(entries[:2], "opaque:excluded-only")
        assert len(store.fills) == 3
        assert store.cursor == "opaque:excluded-only"

    asyncio.run(run())


def test_l2_skips_unconfigured_diffs_and_resync_without_bootstrap() -> None:
    async def run():
        store = Store()
        handler = L2BookHandler(settings(), store)  # type: ignore[arg-type]
        handler.states["BTC"] = BookState(
            epoch="epoch", source_seq=1, height=1, event_no=1,
            bids={Decimal(99): (Decimal(2), 1)},
            asks={Decimal(101): (Decimal(2), 1)},
        )
        await handler.process({
            "height": 2, "time": 1000, "diffs": [
                {"coin": "CASHCAT"},
                {"coin": "BTC", "epoch": "epoch", "seq": 2, "prev_seq": 1,
                 "levels": [[], []]},
            ],
        }, "opaque:diff", "session")
        assert [tick.symbol for tick in store.ticks] == ["BTC"]
        assert list(handler.states) == ["BTC"]
        await handler.process(
            {"type": "resync", "coin": "CASHCAT"}, "opaque:resync", "session"
        )
        assert store.cursor == "opaque:resync"
        assert list(handler.states) == ["BTC"]

    asyncio.run(run())


def test_l4_filters_unconfigured_orders_and_preserves_source_event_index() -> None:
    async def run():
        store = Store()
        handler = L4BookHandler(settings(), store)  # type: ignore[arg-type]
        handler.ready = True
        await handler.process({
            "height": 2, "timestamp": 1000, "diffs": [
                {"coin": "CASHCAT"},
                {"type": "new", "coin": "ETH", "oid": 1, "side": "B",
                 "user": "0xabc", "px": "100", "sz": "2"},
                {"type": "remove", "oid": 999},
            ],
        }, "opaque:orders", "session")
        assert [event.symbol for event in store.orders] == ["ETH"]
        assert store.orders[0].event_index == 1
        assert list(handler.orders) == ["1"]
        # Coin-less updates may use attribution from a retained resting order.
        await handler.process({
            "height": 3, "timestamp": 2000,
            "diffs": [{"type": "remove", "oid": 1}],
        }, "opaque:remove", "session")
        assert store.orders[-1].symbol == "ETH"
        assert handler.orders == {}
        assert store.cursor == "opaque:remove"

    asyncio.run(run())


def test_unfiltered_leverage_stream_persists_only_selected_coins() -> None:
    async def run():
        store = Store()
        handler = LeverageHandler(settings(), store)  # type: ignore[arg-type]
        # The API does not document a server-side coin filter for this stream.
        assert handler.subscription == {"type": "allLeverageUpdates"}
        updates = [{"coin": "CASHCAT"}]
        for index, coin in enumerate(settings().coins):
            updates.append({
                "coin": coin, "update_type": "leverage", "time": 1000,
                "user": "0xabc", "leverage": 5, "tx_index": index,
            })
        await handler.process({"updates": updates}, "opaque:mixed", "session")
        assert [update.symbol for update in store.leverage] == ["SOL", "BTC", "ETH"]
        # Replay passes individual updates instead of a live envelope.
        await handler.process(updates[0], "opaque:excluded", "session")
        assert len(store.leverage) == 3
        assert store.cursor == "opaque:excluded"

    asyncio.run(run())


def test_explicit_majors_migration_covers_all_symbol_tables() -> None:
    root = Path(__file__).parents[1]
    sql = (root / "sql/restrict_to_majors.sql").read_text()
    schema = (root / "src/hydromancer_eda/sql/schema.sql").read_text()
    for definition in schema.split("CREATE TABLE IF NOT EXISTS ")[1:]:
        table, columns = definition.split(" (", 1)
        if "symbol TEXT" in columns.split(");", 1)[0]:
            assert f"'{table}'" in sql
    assert "CHECK (symbol IN (''SOL'', ''BTC'', ''ETH''))" in sql
    assert "DELETE FROM public.wallet_flow_labels" in sql
    assert "SET session_id = NULL" in sql
    assert "cursor = NULL" not in sql
    assert sql.rstrip().endswith("COMMIT;")
