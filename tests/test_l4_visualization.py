import struct
from decimal import Decimal

import msgpack
import pytest
import zstandard

from hydromancer_eda.visualization.l4 import (
    L4BookAnimator,
    L4BookState,
    _build_figure,
    parse_l4_snapshot,
)


def snapshot_bytes() -> bytes:
    market = [
        "COIN",
        [
            ["0xAAA", "100", "3", 1],
            ["0xBBB", "100", "1", 2],
            ["0xAAA", "99", "2", 3],
        ],
        [["0xCCC", "101", "4", 4]],
    ]
    blob = zstandard.ZstdCompressor(write_content_size=False).compress(
        msgpack.packb(market)
    )
    return struct.pack("<IQQI", 1, 10, 1_735_689_600_000, len(blob)) + blob


def test_parse_l4_snapshot_retains_wallet_orders() -> None:
    snapshot = parse_l4_snapshot(snapshot_bytes())

    assert snapshot["height"] == 10
    assert snapshot["markets"][0]["coin"] == "COIN"
    assert snapshot["markets"][0]["bids"][0] == {
        "order_id": "1",
        "wallet": "0xaaa",
        "price": Decimal(100),
        "size": Decimal(3),
        "side": "bid",
    }


def test_l4_state_applies_new_update_and_remove_in_block_order() -> None:
    state = L4BookState(parse_l4_snapshot(snapshot_bytes()), "COIN")

    changed = state.apply(
        {
            "height": 11,
            "timestamp": 1_735_689_601_000,
            "diffs": [
                {"type": "update", "coin": "COIN", "oid": 1, "sz": "2"},
                {"type": "remove", "coin": "COIN", "oid": 2},
                {
                    "type": "new",
                    "coin": "COIN",
                    "oid": 5,
                    "user": "0xDDD",
                    "side": "A",
                    "px": "102",
                    "sz": "5",
                },
            ],
        }
    )

    assert changed is True
    assert state.orders["1"]["size"] == Decimal(2)
    assert "2" not in state.orders
    assert state.orders["5"]["wallet"] == "0xddd"
    assert state.apply({"height": 10, "timestamp": 0, "diffs": []}) is False


def test_figure_wallet_segments_sum_to_100_percent_per_level() -> None:
    pytest.importorskip("plotly")
    figure = _build_figure(
        parse_l4_snapshot(snapshot_bytes()),
        coin="COIN",
        level_count=2,
        minimum_named_share=0,
    )

    totals: dict[str, float] = {}
    for trace in figure.data:
        for label, value in zip(trace.y, trace.x, strict=True):
            totals[str(label)] = totals.get(str(label), 0.0) + abs(float(value))

    assert totals["BID 100"] == pytest.approx(100)
    assert totals["BID 99"] == pytest.approx(100)
    assert totals["ASK 101"] == pytest.approx(100)


def test_animator_mutates_the_same_figure_in_place() -> None:
    pytest.importorskip("plotly")
    initial = parse_l4_snapshot(snapshot_bytes())
    figure = _build_figure(initial, "COIN", 2, 0)
    figure_id = id(figure)
    animator = L4BookAnimator(initial, "COIN", 2, 0, figure)
    state = L4BookState(initial, "COIN")
    state.apply(
        {
            "height": 11,
            "timestamp": 1_735_689_601_000,
            "diffs": [
                {
                    "type": "new",
                    "coin": "COIN",
                    "oid": 5,
                    "user": "0xDDD",
                    "side": "B",
                    "px": "100",
                    "sz": "2",
                }
            ],
        }
    )

    assert animator.update(state.snapshot(), max_fps=0) is True
    assert id(animator.figure) == figure_id
    assert "0xddd" in {trace.meta for trace in figure.data}
