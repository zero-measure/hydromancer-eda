"""Decode wallet-attributed L4 snapshots used to seed order lifecycle state."""

from __future__ import annotations

import struct
from decimal import Decimal
from typing import Any

import msgpack
import zstandard

L4Order = dict[str, Any]
L4Snapshot = dict[str, Any]


def decode_l4_snapshot(payload: bytes) -> L4Snapshot:
    """Decode the documented multi-zstd L4 snapshot without dropping wallet data."""
    if len(payload) < 20:
        raise ValueError("L4 response is shorter than its 20-byte header")
    count, height, timestamp_ms = struct.unpack_from("<IQQ", payload, 0)
    offset = 20
    decompressor = zstandard.ZstdDecompressor()
    markets: list[dict[str, Any]] = []
    for _ in range(count):
        if offset + 4 > len(payload):
            raise ValueError("Truncated L4 blob-length field")
        blob_length = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        blob = payload[offset : offset + blob_length]
        if len(blob) != blob_length:
            raise ValueError("Truncated L4 compressed blob")
        offset += blob_length
        with decompressor.stream_reader(blob) as reader:
            decoded = msgpack.unpackb(reader.read(), raw=False)
        if not isinstance(decoded, list) or len(decoded) != 3:
            raise ValueError("Unexpected L4 market snapshot shape")
        coin, raw_bids, raw_asks = decoded

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
    if offset != len(payload):
        raise ValueError("Unexpected trailing bytes in L4 snapshot")
    return {
        "market_count": count,
        "height": int(height),
        "timestamp_ms": int(timestamp_ms),
        "markets": markets,
    }


# The visualization API historically exposed this name. Keep one canonical decoder.
parse_l4_snapshot = decode_l4_snapshot
