"""Decode L2 snapshots used to bootstrap the incremental order book."""

from __future__ import annotations

import struct
from typing import Any, cast

import msgpack
import zstandard


def decode_l2_snapshots(payload: bytes) -> list[list[Any]]:
    """Decode single-msgpack or length-prefixed multi-zstd L2 snapshots."""
    if len(payload) >= 12 and payload[8:12] == b"\x28\xb5\x2f\xfd":
        count = struct.unpack_from("<I", payload)[0]
        offset = 4
        snapshots: list[Any] = []
        decompressor = zstandard.ZstdDecompressor()
        for _ in range(count):
            if offset + 4 > len(payload):
                raise ValueError("Truncated L2 snapshot blob-length field")
            blob_length = struct.unpack_from("<I", payload, offset)[0]
            offset += 4
            blob = payload[offset : offset + blob_length]
            if len(blob) != blob_length:
                raise ValueError("Truncated L2 snapshot compressed blob")
            offset += blob_length
            with decompressor.stream_reader(blob) as reader:
                snapshots.append(msgpack.unpackb(reader.read(), raw=False))
        if offset != len(payload):
            raise ValueError("Unexpected trailing bytes in L2 snapshot response")
    else:
        unpacker = msgpack.Unpacker(raw=False)
        unpacker.feed(payload)
        objects = list(unpacker)
        if not objects or unpacker.tell() != len(payload):
            raise ValueError("Empty or truncated L2 snapshot response")
        if len(objects) > 1:
            snapshots = objects
        else:
            decoded = objects[0]
            snapshots = [decoded] if decoded and isinstance(decoded[0], str) else decoded
    if not all(
        isinstance(snapshot, list)
        and len(snapshot) == 6
        and isinstance(snapshot[0], str)
        for snapshot in snapshots
    ):
        raise ValueError("Unexpected L2 snapshot response shape")
    return cast(list[list[Any]], snapshots)
