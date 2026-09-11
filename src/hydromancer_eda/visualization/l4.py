"""Animate wallet ownership of L4 price levels in Jupyter notebooks."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from html import escape
from textwrap import wrap
from typing import Any

from hydromancer_eda.client.ws import HydromancerWebSocket
from hydromancer_eda.ingestion.orderbook.l4 import (
    L4Order,
    L4Snapshot,
    decode_l4_snapshot,
)

# Backward-compatible visualization import; decoding now lives at ingestion boundary.
parse_l4_snapshot = decode_l4_snapshot


def l4_snapshot_overview(snapshot: L4Snapshot) -> dict[str, Any]:
    """Return display-safe counts while retaining details in the original snapshot."""
    return {
        "market_count": snapshot["market_count"],
        "height": snapshot["height"],
        "timestamp_ms": snapshot["timestamp_ms"],
        "markets": [
            {
                "coin": market["coin"],
                "bid_orders": len(market["bids"]),
                "ask_orders": len(market["asks"]),
            }
            for market in snapshot["markets"]
        ],
    }


# Shared by the static export and the live notebook widget.
_BOOK_BG = "#0d191e"
_BOOK_TEXT = "#d2dcdd"
_BOOK_MUTED = "#94a3a6"
_BOOK_BORDER = "#29393e"
_BOOK_ASK = "#f56586"
_BOOK_BID = "#20b798"
_BOOK_WIDTH = 480
_SPREAD = "__spread__"


def _wallet_color(wallet: str) -> str:
    if wallet == "Other":
        return "hsl(195, 10%, 25%)"
    hue = int.from_bytes(hashlib.sha256(wallet.encode()).digest()[:2], "big") % 360
    return f"hsl({hue}, 18%, 24%)"


def _market(snapshot: L4Snapshot, coin: str | None) -> dict[str, Any]:
    markets = snapshot.get("markets", [])
    if not markets:
        raise ValueError("Snapshot contains no markets")
    selected = coin or str(markets[0]["coin"])
    for market in markets:
        if market["coin"] == selected:
            return market
    raise ValueError(f"Snapshot does not contain market {selected!r}")


def _visible_levels(
    market: dict[str, Any], level_count: int
) -> list[tuple[str, Decimal, list[L4Order]]]:
    grouped: dict[tuple[str, Decimal], list[L4Order]] = defaultdict(list)
    for side_key, side in (("bids", "bid"), ("asks", "ask")):
        for order in market[side_key]:
            grouped[(side, Decimal(str(order["price"])))].append(order)
    bids = sorted(
        ((side, price, orders) for (side, price), orders in grouped.items() if side == "bid"),
        key=lambda row: row[1],
        reverse=True,
    )[:level_count]
    asks = sorted(
        ((side, price, orders) for (side, price), orders in grouped.items() if side == "ask"),
        key=lambda row: row[1],
    )[:level_count]
    return [*bids, *asks]


def _frame_data(
    snapshot: L4Snapshot,
    coin: str | None,
    level_count: int,
    minimum_named_share: float,
) -> tuple[str, list[dict[str, Any]], set[str]]:
    market = _market(snapshot, coin)
    selected_coin = str(market["coin"])
    levels = _visible_levels(market, level_count)
    rows: list[dict[str, Any]] = []
    wallets: set[str] = set()
    for side, price, orders in levels:
        wallet_sizes: dict[str, Decimal] = defaultdict(Decimal)
        for order in orders:
            wallet_sizes[str(order["wallet"])] += Decimal(str(order["size"]))
        total = sum(wallet_sizes.values(), Decimal())
        shares: dict[str, float] = {}
        other = 0.0
        for wallet, size in wallet_sizes.items():
            share = float(size / total * 100) if total else 0.0
            if share < minimum_named_share:
                other += share
            else:
                shares[wallet] = share
                wallets.add(wallet)
        if other:
            shares["Other"] = other
            wallets.add("Other")
        rows.append(
            {
                "side": side,
                "price": price,
                "label": f"{side.upper()} {price}",
                "total": total,
                "shares": shares,
            }
        )
    # Accumulate outwards from the touch independently on each side. Only the
    # requested visible levels contribute; never accumulate in screen order.
    for side in ("ask", "bid"):
        cumulative = Decimal()
        for row in sorted(
            (row for row in rows if row["side"] == side),
            key=lambda row: row["price"],
            reverse=side == "bid",
        ):
            cumulative += row["total"]
            row["cumulative"] = cumulative
    rows.sort(key=lambda row: (row["side"] != "ask", -row["price"]))
    return selected_coin, rows, wallets


def _hover_wallet(wallet: str) -> str:
    # Plotly hover labels do not wrap automatically; retain the full address
    # without letting it run beyond a narrow notebook output.
    return "<br>".join(escape(wallet[i : i + 28]) for i in range(0, len(wallet), 28))


def _wallet_trace_values(
    wallet: str, rows: list[dict[str, Any]]
) -> tuple[list[float], list[str], list[str], list[str]]:
    x: list[float] = []
    hover: list[str] = []
    text: list[str] = []
    labels: list[str] = []
    for row in rows:
        share = row["shares"].get(wallet, 0.0)
        x.append(share)
        labels.append(row["label"])
        short_wallet = wallet if wallet == "Other" else f"{wallet[:8]}…{wallet[-4:]}"
        hover.append(
            f"wallet={_hover_wallet(wallet)}<br>side={row['side']}<br>price={row['price']}"
            f"<br>level size={row['total']}<br>share={share:.3f}%"
            f"<br>cumulative size={row['cumulative']}"
        )
        text.append(f"{escape(short_wallet)} {share:.1f}%" if share >= 5 else "")
    return x, labels, hover, text


def _ownership_lines(row: dict[str, Any]) -> list[str]:
    """Keep every previously visible (>=5%) wallet label, including on narrow outputs."""
    labels = []
    for wallet, share in sorted(
        row["shares"].items(), key=lambda item: (item[0] == "Other", item[0])
    ):
        if share >= 5:
            short = wallet if wallet == "Other" else f"{wallet[:8]}…{wallet[-4:]}"
            labels.append(f"{short} {share:.1f}%")
    return wrap(" · ".join(labels), width=54, break_long_words=False, break_on_hyphens=False)


def _book_layout(
    snapshot: L4Snapshot, coin: str, rows: list[dict[str, Any]], level_count: int
) -> dict[str, Any]:
    """A deterministic price ladder, with wallet attribution below each numeric row."""
    asks = [row for row in rows if row["side"] == "ask"]
    bids = [row for row in rows if row["side"] == "bid"]
    labels = [
        *([row["label"] for row in asks] or ["__no_asks__"]),
        _SPREAD,
        *([row["label"] for row in bids] or ["__no_bids__"]),
    ]
    ownership = {row["label"]: _ownership_lines(row) for row in rows}
    row_height = 32 + 12 * max((len(lines) for lines in ownership.values()), default=1)
    annotations: list[dict[str, Any]] = []
    shapes: list[dict[str, Any]] = []

    def text(
        name: str,
        value: str,
        x: float,
        y: str | float,
        *,
        anchor: str = "left",
        color: str = _BOOK_TEXT,
        size: int = 12,
        yref: str = "y",
        yshift: float = 0,
        **extra: Any,
    ) -> None:
        annotations.append(
            {
                "name": name,
                "text": value,
                "x": x,
                "y": y,
                "xref": "paper",
                "yref": yref,
                "xanchor": anchor,
                "yanchor": "middle",
                "yshift": yshift,
                "showarrow": False,
                "font": {"size": size, "color": color},
                **extra,
            }
        )

    def rect(name: str, label: str, width: float, color: str, top: float, bottom: float) -> None:
        shapes.append(
            {
                "name": name,
                "type": "rect",
                "xref": "paper",
                "yref": "y",
                "x0": 0,
                "x1": width,
                "y0": labels.index(label) + top,
                "y1": labels.index(label) + bottom,
                "fillcolor": color,
                "line": {"width": 0},
                "layer": "below",
            }
        )

    price_decimals = max(
        (max(0, -int(row["price"].as_tuple().exponent)) for row in rows), default=0
    )
    max_depth = max((row["cumulative"] for row in rows), default=Decimal())
    for row in rows:
        label = row["label"]
        color = _BOOK_ASK if row["side"] == "ask" else _BOOK_BID
        depth_color = "rgba(245,101,134,0.12)" if row["side"] == "ask" else "rgba(32,183,152,0.12)"
        rect(
            f"depth:{label}",
            label,
            float(row["cumulative"] / max_depth) if max_depth else 0,
            depth_color,
            -0.48,
            0.48,
        )
        numeric_shift = row_height / 2 - 13
        details = (
            f"{row['side'].upper()} · price={row['price']}<br>level size={row['total']}"
            f"<br>cumulative size={row['cumulative']}<br>"
            + "<br>".join(
                f"wallet={_hover_wallet(wallet)}<br>share={share:.3f}%"
                for wallet, share in sorted(row["shares"].items())
            )
        )
        text(
            f"price:{label}",
            f"{row['price']:,.{price_decimals}f}",
            0.035,
            label,
            color=color,
            yshift=numeric_shift,
            hovertext=details,
        )
        text(
            f"size:{label}",
            f"{row['total']:,f}",
            0.61,
            label,
            anchor="right",
            yshift=numeric_shift,
            hovertext=details,
        )
        text(
            f"total:{label}",
            f"{row['cumulative']:,f}",
            0.965,
            label,
            anchor="right",
            yshift=numeric_shift,
            hovertext=details,
        )
        text(
            f"ownership:{label}",
            "<br>".join(escape(line) for line in ownership[label]),
            0.035,
            label,
            size=10,
            color=_BOOK_MUTED,
            yshift=numeric_shift - 13,
            yanchor="top",
            align="left",
            hovertext=details,
        )

    rect("spread-background", _SPREAD, 1, "#213137", -0.48, 0.48)
    spread_text, percent_text = "—", "—"
    if asks and bids:
        spread = asks[-1]["price"] - bids[0]["price"]
        mid = (asks[-1]["price"] + bids[0]["price"]) / 2
        spread_text = f"{spread:,.{price_decimals}f}"
        percent_text = f"{spread / mid * 100:.3f}%" if mid else "—"
    text("spread-label", "Spread", 0.035, _SPREAD)
    text("spread-value", spread_text, 0.61, _SPREAD, anchor="right")
    text("spread-percent", percent_text, 0.965, _SPREAD, anchor="right")
    for side, side_rows in (("asks", asks), ("bids", bids)):
        if not side_rows:
            text(
                f"empty-{side}",
                f"No resting {side}",
                0.5,
                f"__no_{side}__",
                anchor="center",
                color=_BOOK_MUTED,
            )

    text("heading", "Order Book", 0.035, 1, yref="paper", yshift=83, size=15)
    text(
        "market",
        escape(coin),
        0.965,
        1,
        yref="paper",
        yshift=83,
        anchor="right",
        color="#75dfcc",
        size=12,
    )
    text(
        "source",
        "L4 wallet attribution",
        0.035,
        1,
        yref="paper",
        yshift=49,
        color=_BOOK_MUTED,
        size=10,
    )
    text(
        "block",
        f"Block {escape(str(snapshot.get('height', '?')))}",
        0.965,
        1,
        yref="paper",
        yshift=49,
        anchor="right",
        color=_BOOK_MUTED,
        size=10,
    )
    for name, value, x, anchor in (
        ("price", "Price", 0.035, "left"),
        ("size", f"Size ({escape(coin)})", 0.61, "right"),
        ("total", f"Total ({escape(coin)})", 0.965, "right"),
    ):
        text(
            f"header-{name}",
            value,
            x,
            1,
            yref="paper",
            yshift=19,
            anchor=anchor,
            color=_BOOK_MUTED,
            size=11,
        )
    text(
        "footer",
        f"Top {level_count}/side · Wallet % below · Hover for details",
        0.035,
        0,
        yref="paper",
        yshift=-21,
        color=_BOOK_MUTED,
        size=9,
    )
    for name, y, shift, color, width, end in (
        ("header-border", 1, 66, _BOOK_BORDER, 1, 1),
        ("active-accent", 1, 66, "#75dfcc", 2, 0.30),
        ("columns-border", 1, 0, _BOOK_BORDER, 1, 1),
        ("footer-border", 0, 0, _BOOK_BORDER, 1, 1),
    ):
        shapes.append(
            {
                "name": name,
                "type": "line",
                "xref": "paper",
                "yref": "paper",
                "x0": 0,
                "x1": end,
                "y0": shift,
                "y1": shift,
                "ysizemode": "pixel",
                "yanchor": y,
                "line": {"color": color, "width": width},
            }
        )
    return {
        "annotations": annotations,
        "shapes": shapes,
        "title": {"text": ""},
        "showlegend": False,
        "barmode": "stack",
        "paper_bgcolor": _BOOK_BG,
        "plot_bgcolor": _BOOK_BG,
        "font": {
            "family": "Inter, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif",
            "size": 12,
            "color": _BOOK_TEXT,
        },
        "xaxis": {"range": [0, 100], "visible": False, "fixedrange": True},
        "yaxis": {
            "type": "category",
            "categoryorder": "array",
            "categoryarray": labels,
            "range": [len(labels) - 0.5, -0.5],
            "visible": False,
            "fixedrange": True,
        },
        "height": row_height * len(labels) + 136,
        "autosize": True,
        "margin": {"l": 1, "r": 1, "t": 104, "b": 32, "pad": 0},
        "hoverlabel": {
            "bgcolor": "#213137",
            "bordercolor": _BOOK_BORDER,
            "font": {"size": 12, "color": _BOOK_TEXT},
            "align": "left",
        },
        "hovermode": "closest",
        "dragmode": False,
        "transition": {"duration": 0},
        "template": "none",
    }


def _update_figure_in_place(
    figure: Any,
    snapshot: L4Snapshot,
    coin: str | None,
    level_count: int,
    minimum_named_share: float,
) -> None:
    """Mutate one FigureWidget; never emit or replace its notebook output."""
    selected_coin, rows, wallets = _frame_data(snapshot, coin, level_count, minimum_named_share)
    existing = {str(trace.meta): trace for trace in figure.data}
    with figure.batch_update():
        for wallet, trace in existing.items():
            if wallet not in wallets:
                trace.visible = False
                continue
            x, y, hover, text = _wallet_trace_values(wallet, rows)
            trace.update(
                x=x,
                y=y,
                customdata=hover,
                text=text,
                visible=True,
            )
        for wallet in sorted(
            wallets - existing.keys(), key=lambda value: (value == "Other", value)
        ):
            x, y, hover, text = _wallet_trace_values(wallet, rows)
            figure.add_bar(
                meta=wallet,
                name=wallet if wallet == "Other" else f"{wallet[:8]}…{wallet[-4:]}",
                y=y,
                x=x,
                orientation="h",
                marker_color=_wallet_color(wallet),
                customdata=hover,
                hovertemplate="%{customdata}<extra></extra>",
                text=text,
                # A slim ownership strip remains hoverable, with every >=5%
                # label rendered legibly above it rather than clipped in tiny bars.
                textposition="none",
                width=0.04,
                offset=0.44,
                marker_line_width=0,
                showlegend=False,
                visible=True,
            )
        # Replace array-valued layout fields rather than merging stale rows when
        # a side shrinks or disappears during live updates.
        layout = _book_layout(snapshot, selected_coin, rows, level_count)
        figure.layout.annotations = ()
        figure.layout.shapes = ()
        figure.update_layout(**layout)


def _build_figure(
    snapshot: L4Snapshot,
    coin: str | None,
    level_count: int,
    minimum_named_share: float,
) -> Any:
    """Build a static figure, primarily for export and deterministic tests."""
    import plotly.graph_objects as go

    figure = go.Figure()
    _update_figure_in_place(figure, snapshot, coin, level_count, minimum_named_share)
    figure.update_layout(width=_BOOK_WIDTH)
    return figure


def _book_details(
    snapshot: L4Snapshot, coin: str | None, level_count: int, minimum_named_share: float
) -> str:
    """Semantic, selectable equivalent of hover data for keyboard/screen-reader use."""
    selected_coin, rows, _ = _frame_data(snapshot, coin, level_count, minimum_named_share)
    body = []
    for row in rows:
        shares = "<br>".join(
            f"{escape(wallet)}: {share:.3f}%" for wallet, share in sorted(row["shares"].items())
        )
        body.append(
            f"<tr><th scope='row'>{row['side'].upper()} {row['price']}</th>"
            f"<td>{row['total']}</td><td>{row['cumulative']}</td><td>{shares}</td></tr>"
        )
    return (
        f"<div style='overflow:auto;color:{_BOOK_TEXT};background:{_BOOK_BG};padding:12px' "
        "tabindex='0' role='region' aria-label='Order book exact values'>"
        "<table style='width:100%;font-size:12px;text-align:left;border-spacing:8px'>"
        f"<caption>{escape(selected_coin)} · L4 wallet attribution · "
        f"Block {escape(str(snapshot.get('height', '?')))}<br>"
        "Size and Total in base units; cumulative totals cover visible levels.</caption>"
        "<thead><tr><th scope='col'>Side / Price</th><th scope='col'>Size</th>"
        "<th scope='col'>Total</th><th scope='col'>Wallet / Share</th></tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
        + ("<p>No resting orders</p>" if not rows else "")
        + "</div>"
    )


@dataclass
class L4BookAnimator:
    """Update one persistent notebook FigureWidget as L4 state changes."""

    snapshot: L4Snapshot
    coin: str | None
    level_count: int
    minimum_named_share: float
    figure: Any
    last_render_time: float = 0.0
    details_widget: Any = None

    def update(self, snapshot: L4Snapshot, max_fps: float = 5.0) -> bool:
        """Mutate the existing widget, coalescing renders but not book updates."""
        self.snapshot = snapshot
        now = time.monotonic()
        if max_fps > 0 and now - self.last_render_time < 1 / max_fps:
            return False
        _update_figure_in_place(
            self.figure,
            snapshot,
            self.coin,
            self.level_count,
            self.minimum_named_share,
        )
        if self.details_widget is not None:
            self.details_widget.value = _book_details(
                snapshot, self.coin, self.level_count, self.minimum_named_share
            )
        self.last_render_time = now
        return True


def animate_l4_book(
    snapshot: L4Snapshot,
    coin: str | None = None,
    level_count: int = 12,
    minimum_named_share: float = 0.0,
) -> L4BookAnimator:
    """Display a compact price ladder with L4 wallet ownership and live updates.

    Size and cumulative Total are in base-coin units; Price is in quote units. Totals
    cover the visible levels on each side. Wallet labels >=5% remain inline;
    all shares (including smaller segments) and exact sizes are available on hover.
    """
    import plotly.graph_objects as go
    from IPython.display import display
    from ipywidgets import HTML, Layout, ToggleButton, VBox

    from hydromancer_eda.visualization._sizing import BookSizer

    if level_count < 1:
        raise ValueError("level_count must be positive")
    if not 0 <= minimum_named_share <= 100:
        raise ValueError("minimum_named_share must be between 0 and 100")
    figure = go.FigureWidget()
    # FigureWidget's synced config trait is the notebook equivalent of the
    # config argument to Figure.show; chart controls would obscure the header.
    figure._config = {**figure._config, "displayModeBar": False}
    _update_figure_in_place(figure, snapshot, coin, level_count, minimum_named_share)
    figure.update_layout(width=_BOOK_WIDTH)
    sizer = BookSizer(
        measured_width=_BOOK_WIDTH,
        maximum=_BOOK_WIDTH,
        layout=Layout(width="100%", height="0px", min_height="0px"),
    )
    sizer.observe(lambda change: figure.update_layout(width=change["new"]), names="measured_width")
    # Constrain the existing widget, not a separate UI. It shrinks with the
    # notebook output on mobile and retains its identity through live updates.
    details = HTML(
        _book_details(snapshot, coin, level_count, minimum_named_share),
        layout=Layout(display="none"),
    )
    disclosure = ToggleButton(
        description="Exact values & wallet shares",
        tooltip="Show or hide exact order book values and wallet ownership",
        layout=Layout(width="100%", min_height="32px"),
    )
    disclosure.observe(
        lambda change: setattr(details.layout, "display", "" if change["new"] else "none"),
        names="value",
    )
    display(
        VBox(
            [sizer, figure, disclosure, details],
            layout=Layout(
                width="100%", max_width=f"{_BOOK_WIDTH}px", min_width="0", overflow="auto"
            ),
        )
    )
    return L4BookAnimator(
        snapshot=snapshot,
        coin=coin,
        level_count=level_count,
        minimum_named_share=minimum_named_share,
        figure=figure,
        last_render_time=time.monotonic(),
        details_widget=details,
    )


class L4BookState:
    """Apply l4BookUpdates diffs to a full snapshot used by the animator."""

    def __init__(self, snapshot: L4Snapshot, coin: str | None = None) -> None:
        market = _market(snapshot, coin)
        self.coin = str(market["coin"])
        self.height = int(snapshot["height"])
        self.timestamp_ms = int(snapshot["timestamp_ms"])
        self.orders = {
            str(order["order_id"]): dict(order)
            for side in (market["bids"], market["asks"])
            for order in side
        }

    def apply(self, data: dict[str, Any]) -> bool:
        """Apply one per-block diff batch; return false for snapshot-covered batches."""
        height = int(data["height"])
        if height <= self.height:
            return False
        for diff in data.get("diffs", []):
            if str(diff.get("coin", self.coin)) != self.coin:
                continue
            order_id = str(diff["oid"])
            event_type = diff["type"]
            if event_type == "new":
                self.orders[order_id] = {
                    "order_id": order_id,
                    "wallet": str(diff["user"]).lower(),
                    "price": Decimal(str(diff["px"])),
                    "size": Decimal(str(diff["sz"])),
                    "side": "bid" if diff["side"] == "B" else "ask",
                }
            elif event_type == "update" and order_id in self.orders:
                self.orders[order_id]["size"] = Decimal(str(diff["sz"]))
            elif event_type == "remove":
                self.orders.pop(order_id, None)
        self.height = height
        self.timestamp_ms = int(data["timestamp"])
        return True

    def snapshot(self) -> L4Snapshot:
        """Build the current full-book shape consumed by the animator."""
        bids = [order for order in self.orders.values() if order["side"] == "bid"]
        asks = [order for order in self.orders.values() if order["side"] == "ask"]
        return {
            "market_count": 1,
            "height": self.height,
            "timestamp_ms": self.timestamp_ms,
            "markets": [{"coin": self.coin, "bids": bids, "asks": asks}],
        }


async def stream_l4_book_updates(
    animator: L4BookAnimator,
    api_key: str,
    snapshot_fetcher: Callable[[], L4Snapshot] | None = None,
    max_fps: float = 5.0,
) -> None:
    """Keep an animation current using l4BookUpdates rather than REST polling.

    A fresh snapshot is fetched after subscribing when ``snapshot_fetcher`` is
    provided. WebSocket frames queue while it is fetched, and batches covered by
    the snapshot height are discarded before live diffs are applied.
    """
    websocket = HydromancerWebSocket(api_key)
    connection = await websocket.connect()
    async with connection:
        messages = websocket.messages(connection)
        async for message in messages:
            if message.get("type") == "ping":
                await websocket.send(connection, {"type": "pong"})
            elif message.get("type") == "connected":
                await websocket.send(
                    connection,
                    {
                        "method": "subscribe",
                        "subscription": {
                            "type": "l4BookUpdates",
                            "coins": [animator.coin or _market(animator.snapshot, None)["coin"]],
                            "merge": True,
                        },
                    },
                )
                break
        initial = (
            await asyncio.to_thread(snapshot_fetcher)
            if snapshot_fetcher is not None
            else animator.snapshot
        )
        state = L4BookState(initial, animator.coin)
        animator.update(initial, max_fps=0)
        async for message in messages:
            message_type = message.get("type")
            if message_type == "ping":
                await websocket.send(connection, {"type": "pong"})
                continue
            if message_type != "l4BookUpdates" and message.get("channel") != "l4BookUpdates":
                continue
            data = message.get("data")
            if isinstance(data, dict) and state.apply(data):
                animator.update(state.snapshot(), max_fps=max_fps)
