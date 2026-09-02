"""Animate wallet ownership of L4 price levels in Jupyter notebooks."""

from __future__ import annotations

import asyncio
import hashlib
import struct
import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import msgpack
import zstandard

from hydromancer_eda.client.ws import HydromancerWebSocket

L4Order = dict[str, Any]
L4Snapshot = dict[str, Any]


def parse_l4_snapshot(body: bytes) -> L4Snapshot:
    """Decode the documented multi-zstd L4 snapshot without dropping wallet data."""
    if len(body) < 20:
        raise ValueError("L4 response is shorter than its 20-byte header")
    count, height, timestamp_ms = struct.unpack_from("<IQQ", body, 0)
    offset = 20
    decompressor = zstandard.ZstdDecompressor()
    markets: list[dict[str, Any]] = []
    for _ in range(count):
        if offset + 4 > len(body):
            raise ValueError("Truncated L4 blob-length field")
        blob_length = struct.unpack_from("<I", body, offset)[0]
        offset += 4
        blob = body[offset : offset + blob_length]
        if len(blob) != blob_length:
            raise ValueError("Truncated L4 compressed blob")
        offset += blob_length
        with decompressor.stream_reader(blob) as reader:
            coin, raw_bids, raw_asks = msgpack.unpackb(reader.read(), raw=False)

        def orders(raw_orders: list[list[Any]], side: str) -> list[L4Order]:
            return [
                {
                    "order_id": str(order_id),
                    "wallet": str(wallet).lower(),
                    "price": Decimal(str(price)),
                    "size": Decimal(str(size)),
                    "side": side,
                }
                for wallet, price, size, order_id in raw_orders
            ]

        markets.append(
            {
                "coin": str(coin),
                "bids": orders(raw_bids, "bid"),
                "asks": orders(raw_asks, "ask"),
            }
        )
    if offset != len(body):
        raise ValueError("Unexpected trailing bytes in L4 snapshot")
    return {
        "market_count": count,
        "height": int(height),
        "timestamp_ms": int(timestamp_ms),
        "markets": markets,
    }


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


def _wallet_color(wallet: str) -> str:
    if wallet == "Other":
        return "hsl(0, 0%, 55%)"
    hue = int.from_bytes(hashlib.sha256(wallet.encode()).digest()[:2], "big") % 360
    return f"hsl({hue}, 62%, 48%)"


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
    return selected_coin, rows, wallets


def _wallet_trace_values(
    wallet: str, rows: list[dict[str, Any]]
) -> tuple[list[float], list[str], list[str], list[str]]:
    x: list[float] = []
    hover: list[str] = []
    text: list[str] = []
    labels: list[str] = []
    for row in rows:
        share = row["shares"].get(wallet, 0.0)
        x.append(-share if row["side"] == "bid" else share)
        labels.append(row["label"])
        short_wallet = wallet if wallet == "Other" else f"{wallet[:8]}…{wallet[-4:]}"
        hover.append(
            f"wallet={wallet}<br>side={row['side']}<br>price={row['price']}"
            f"<br>level size={row['total']}<br>share={share:.3f}%"
        )
        text.append(f"{short_wallet} {share:.1f}%" if share >= 5 else "")
    return x, labels, hover, text


def _update_figure_in_place(
    figure: Any,
    snapshot: L4Snapshot,
    coin: str | None,
    level_count: int,
    minimum_named_share: float,
) -> None:
    """Mutate one FigureWidget; never emit or replace its notebook output."""
    selected_coin, rows, wallets = _frame_data(
        snapshot, coin, level_count, minimum_named_share
    )
    existing = {str(trace.meta): trace for trace in figure.data}
    labels = [row["label"] for row in rows]

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
        for wallet in sorted(wallets - existing.keys(), key=lambda value: (value == "Other", value)):
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
                textposition="inside",
            )
        figure.update_layout(
            title=(
                f"{selected_coin} L4 wallet ownership — block "
                f"{snapshot.get('height', '?')}"
            ),
            barmode="relative",
            xaxis={
                "title": "Wallet share of price level (bid ← % → ask)",
                "range": [-100, 100],
                "ticksuffix": "%",
                "zeroline": True,
                "zerolinewidth": 2,
            },
            yaxis={
                "categoryorder": "array",
                "categoryarray": list(reversed(labels)),
            },
            legend={"title": "Wallet", "tracegroupgap": 0},
            height=max(500, 34 * len(rows) + 170),
            margin={"l": 120, "r": 30, "t": 70, "b": 60},
            transition={"duration": 120, "easing": "cubic-in-out"},
            template="plotly_white",
        )


def _build_figure(
    snapshot: L4Snapshot,
    coin: str | None,
    level_count: int,
    minimum_named_share: float,
) -> Any:
    """Build a static figure, primarily for export and deterministic tests."""
    import plotly.graph_objects as go

    figure = go.Figure()
    _update_figure_in_place(
        figure, snapshot, coin, level_count, minimum_named_share
    )
    return figure


@dataclass
class L4BookAnimator:
    """Update one persistent notebook FigureWidget as L4 state changes."""

    snapshot: L4Snapshot
    coin: str | None
    level_count: int
    minimum_named_share: float
    figure: Any
    last_render_time: float = 0.0

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
        self.last_render_time = now
        return True


def animate_l4_book(
    snapshot: L4Snapshot,
    coin: str | None = None,
    level_count: int = 12,
    minimum_named_share: float = 0.0,
) -> L4BookAnimator:
    """Display one FigureWidget and return its in-place incremental updater."""
    import plotly.graph_objects as go
    from IPython.display import display

    if level_count < 1:
        raise ValueError("level_count must be positive")
    if not 0 <= minimum_named_share <= 100:
        raise ValueError("minimum_named_share must be between 0 and 100")
    figure = go.FigureWidget()
    _update_figure_in_place(
        figure, snapshot, coin, level_count, minimum_named_share
    )
    display(figure)
    return L4BookAnimator(
        snapshot=snapshot,
        coin=coin,
        level_count=level_count,
        minimum_named_share=minimum_named_share,
        figure=figure,
        last_render_time=time.monotonic(),
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
