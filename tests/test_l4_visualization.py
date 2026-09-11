import struct
from decimal import Decimal

import msgpack
import pytest
import zstandard

from hydromancer_eda.visualization.l4 import (
    L4BookAnimator,
    L4BookState,
    _book_details,
    _build_figure,
    animate_l4_book,
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
    blob = zstandard.ZstdCompressor(write_content_size=False).compress(msgpack.packb(market))
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


def ladder_snapshot():
    snapshot = parse_l4_snapshot(snapshot_bytes())
    snapshot["markets"][0]["asks"].append(
        {
            "order_id": "5",
            "wallet": "0xddd",
            "price": Decimal(102),
            "size": Decimal(5),
            "side": "ask",
        }
    )
    return snapshot


def annotations_by_name(figure):
    return {annotation.name: annotation for annotation in figure.layout.annotations}


def test_ladder_orders_asks_spread_bids_and_accumulates_from_touch() -> None:
    pytest.importorskip("plotly")
    figure = _build_figure(ladder_snapshot(), "COIN", 2, 0)
    annotations = annotations_by_name(figure)
    assert list(figure.layout.yaxis.categoryarray) == [
        "ASK 102",
        "ASK 101",
        "__spread__",
        "BID 100",
        "BID 99",
    ]
    assert list(figure.layout.yaxis.range) == [4.5, -0.5]
    assert {
        name: annotations[name].text
        for name in (
            "size:ASK 102",
            "total:ASK 102",
            "total:ASK 101",
            "total:BID 100",
            "total:BID 99",
            "spread-value",
            "spread-percent",
        )
    } == {
        "size:ASK 102": "5",
        "total:ASK 102": "9",
        "total:ASK 101": "4",
        "total:BID 100": "4",
        "total:BID 99": "6",
        "spread-value": "1",
        "spread-percent": "0.995%",
    }
    assert annotations["header-price"].text == "Price"
    assert annotations["header-size"].text == "Size (COIN)"
    assert annotations["header-total"].text == "Total (COIN)"
    assert annotations["price:ASK 102"].font.color == "#f56586"
    assert annotations["price:BID 100"].font.color == "#20b798"
    assert annotations["size:ASK 102"].x == annotations["header-size"].x
    assert annotations["total:ASK 102"].xanchor == "right"
    assert figure.layout.width == 480
    assert figure.layout.paper_bgcolor == figure.layout.plot_bgcolor == "#0d191e"
    assert figure.layout.showlegend is False
    assert figure.layout.xaxis.visible is False
    assert figure.layout.xaxis.fixedrange is True
    assert figure.layout.transition.duration == 0
    assert figure.layout.height == 356


def test_depth_shading_uses_one_scale_and_only_visible_levels() -> None:
    pytest.importorskip("plotly")
    figure = _build_figure(ladder_snapshot(), "COIN", 2, 0)
    depth = {shape.name: shape for shape in figure.layout.shapes if shape.name.startswith("depth:")}
    assert depth["depth:ASK 102"].x1 == 1
    assert depth["depth:ASK 101"].x1 == pytest.approx(4 / 9)
    assert depth["depth:BID 100"].x1 == pytest.approx(4 / 9)
    assert depth["depth:BID 99"].x1 == pytest.approx(6 / 9)
    assert all(shape.layer == "below" and shape.line.width == 0 for shape in depth.values())
    assert depth["depth:ASK 102"].fillcolor == "rgba(245,101,134,0.12)"
    assert depth["depth:BID 99"].fillcolor == "rgba(32,183,152,0.12)"
    one_level = _build_figure(ladder_snapshot(), "COIN", 1, 0)
    assert annotations_by_name(one_level)["total:ASK 101"].text == "4"
    assert "ASK 102" not in one_level.layout.yaxis.categoryarray


def test_wallet_labels_hover_and_other_group_are_retained() -> None:
    pytest.importorskip("plotly")
    figure = _build_figure(ladder_snapshot(), "COIN", 2, 30)
    traces = {trace.meta: trace for trace in figure.data}
    assert "0xbbb" not in traces
    assert "Other" in traces
    row_index = list(traces["0xaaa"].y).index("BID 100")
    assert traces["0xaaa"].x[row_index] == 75
    assert traces["Other"].x[row_index] == 25
    assert traces["0xaaa"].customdata[row_index] == (
        "wallet=0xaaa<br>side=bid<br>price=100<br>level size=4<br>share=75.000%"
        "<br>cumulative size=4"
    )
    assert "75.0%" in traces["0xaaa"].text[row_index]
    ownership = annotations_by_name(figure)["ownership:BID 100"]
    assert "75.0%" in ownership.text and "Other 25.0%" in ownership.text
    assert "wallet=Other<br>share=25.000%" in ownership.hovertext


@pytest.mark.parametrize("empty_sides", [("asks",), ("bids",), ("asks", "bids")])
def test_empty_sides_have_explicit_placeholders_and_no_fabricated_spread(empty_sides) -> None:
    pytest.importorskip("plotly")
    snapshot = ladder_snapshot()
    for side in empty_sides:
        snapshot["markets"][0][side] = []
    figure = _build_figure(snapshot, "COIN", 2, 0)
    annotations = annotations_by_name(figure)
    for side in empty_sides:
        assert annotations[f"empty-{side}"].text == f"No resting {side}"
    assert annotations["spread-value"].text == annotations["spread-percent"].text == "—"
    assert "__spread__" in figure.layout.yaxis.categoryarray


def test_live_shrink_and_reappearance_replace_layout_without_replacing_traces() -> None:
    pytest.importorskip("plotly")
    initial = ladder_snapshot()
    figure = _build_figure(initial, "COIN", 2, 0)
    animator = L4BookAnimator(initial, "COIN", 2, 0, figure)
    original_traces = {trace.meta: trace for trace in figure.data}
    empty = ladder_snapshot()
    empty["markets"][0].update(bids=[], asks=[])
    animator.update(empty, max_fps=0)
    assert all(trace.visible is False for trace in figure.data)
    assert not any(shape.name.startswith("depth:") for shape in figure.layout.shapes)
    assert "price:ASK 102" not in annotations_by_name(figure)
    animator.update(initial, max_fps=0)
    assert all(trace.visible is True for trace in figure.data)
    assert all(trace is original_traces[trace.meta] for trace in figure.data)
    assert "empty-asks" not in annotations_by_name(figure)
    assert figure.to_plotly_json() == _build_figure(initial, "COIN", 2, 0).to_plotly_json()


def test_fractional_prices_zero_size_and_unchanged_precision() -> None:
    pytest.importorskip("plotly")
    snapshot = ladder_snapshot()
    market = snapshot["markets"][0]
    market["asks"] = [dict(market["asks"][0], price=Decimal("0.00000012"), size=Decimal(0))]
    market["bids"] = [dict(market["bids"][0], price=Decimal("0.00000010"), size=Decimal(0))]
    figure = _build_figure(snapshot, "COIN", 2, 0)
    annotations = annotations_by_name(figure)
    assert annotations["spread-value"].text == "0.00000002"
    assert annotations["spread-percent"].text == "18.182%"
    assert all(shape.x1 == 0 for shape in figure.layout.shapes if shape.name.startswith("depth:"))


def test_many_wallets_wrap_without_losing_previously_visible_percentages() -> None:
    pytest.importorskip("plotly")
    snapshot = ladder_snapshot()
    market = snapshot["markets"][0]
    market["bids"] = [
        dict(market["bids"][0], wallet=f"0x{i:040x}", order_id=str(i), size=Decimal(1))
        for i in range(20)
    ]
    figure = _build_figure(snapshot, "COIN", 1, 0)
    ownership = annotations_by_name(figure)["ownership:BID 100"]
    assert ownership.text.count("5.0%") == 20
    assert "<br>" in ownership.text
    assert all(len(line) <= 54 for line in ownership.text.split("<br>"))
    assert figure.layout.height > 268


def test_widget_has_responsive_container_and_keyboard_accessible_live_details(monkeypatch) -> None:
    pytest.importorskip("plotly")
    pytest.importorskip("ipywidgets")
    pytest.importorskip("anywidget")
    displayed = []
    monkeypatch.setattr("IPython.display.display", displayed.append)
    animator = animate_l4_book(ladder_snapshot(), "COIN", 2, 0)
    container = displayed[0]
    assert container.children[1] is animator.figure
    assert container.layout.width == "100%"
    assert container.layout.max_width == "480px"
    assert container.layout.overflow == "auto"
    assert animator.figure.layout.width == 480
    assert animator.figure._config["displayModeBar"] is False
    sizer = container.children[0]
    sizer.measured_width = 320
    assert animator.figure.layout.width == 320
    disclosure = container.children[2]
    assert animator.details_widget.layout.display == "none"
    disclosure.value = True
    assert animator.details_widget.layout.display == ""
    assert container.children[3] is animator.details_widget
    assert "scope='col'" in animator.details_widget.value
    assert "tabindex='0'" in animator.details_widget.value
    assert "0xaaa: 75.000%" in animator.details_widget.value
    updated = ladder_snapshot()
    updated["height"] = 11
    animator.update(updated, max_fps=0)
    assert "Block 11" in animator.details_widget.value
    assert disclosure.value is True
    assert animator.figure.layout.width == 320
    assert len(displayed) == 1
    animator.figure.close()
    sizer.close()
    disclosure.close()
    animator.details_widget.close()
    container.close()


def test_details_and_annotations_escape_market_and_wallet_text() -> None:
    pytest.importorskip("plotly")
    snapshot = ladder_snapshot()
    market = snapshot["markets"][0]
    market["coin"] = "<b>COIN</b>"
    market["bids"][0]["wallet"] = "<script>alert(1)</script>"
    html = _book_details(snapshot, None, 2, 0)
    assert "<script>" not in html and "<b>COIN</b>" not in html
    assert "&lt;script&gt;" in html and "&lt;b&gt;COIN&lt;/b&gt;" in html
    figure = _build_figure(snapshot, None, 2, 0)
    assert annotations_by_name(figure)["market"].text == "&lt;b&gt;COIN&lt;/b&gt;"


def test_full_wallet_addresses_wrap_in_hover_without_losing_details() -> None:
    pytest.importorskip("plotly")
    snapshot = ladder_snapshot()
    wallet = "0x" + "a" * 40
    snapshot["markets"][0]["bids"][0]["wallet"] = wallet
    figure = _build_figure(snapshot, "COIN", 2, 0)
    trace = next(trace for trace in figure.data if trace.meta == wallet)
    hover = trace.customdata[list(trace.y).index("BID 100")]
    assert f"wallet={wallet}" in hover.replace("<br>", "")
    assert "share=75.000%" in hover
    assert max(map(len, hover.split("<br>"))) <= 35
    annotation_hover = annotations_by_name(figure)["price:BID 100"].hovertext
    assert f"wallet={wallet}" in annotation_hover.replace("<br>", "")
    assert max(map(len, annotation_hover.split("<br>"))) <= 35


def test_layout_is_deterministic_regardless_of_snapshot_order() -> None:
    pytest.importorskip("plotly")
    initial = ladder_snapshot()
    shuffled = ladder_snapshot()
    for side in ("bids", "asks"):
        shuffled["markets"][0][side].reverse()
    assert (
        _build_figure(initial, "COIN", 2, 0).to_plotly_json()
        == _build_figure(shuffled, "COIN", 2, 0).to_plotly_json()
    )
