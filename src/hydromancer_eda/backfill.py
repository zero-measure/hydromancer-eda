"""Safe, explicitly wallet-scoped reconstruction of historical fill direction."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from hydromancer_eda.client.rest import HydromancerRestClient
from hydromancer_eda.funding import to_milliseconds

LIMITATIONS = (
    "userFillsByTime is wallet-scoped; Hydromancer documents full availability from "
    "2025-07-28 for most, but not all, high-volume users (TWAP from 2025-08-02). "
    "Only exact wallet/time/txIndex/trade/order matches are updated; all others remain NULL. "
    "Pagination uses the documented time_txIndex continuation derived from the final row; "
    "the resulting cursor string is passed back unchanged and never parsed."
)


def _integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"invalid {field}")
    if value < 0 or value > 2**63 - 1:
        raise ValueError(f"invalid {field}")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"invalid {field}")
    return value


def _decimal(value: object, field: str, *, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise TypeError(f"invalid {field}")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"invalid {field}") from None
    if not parsed.is_finite() or (positive and parsed <= 0):
        raise ValueError(f"invalid {field}")
    return parsed


def _validated_fill(fill: dict[str, Any]) -> dict[str, object]:
    """Normalize every value used by SQL; callers separately enforce the time range."""
    side = _text(fill.get("side"), "side")
    if side not in {"A", "B"}:
        raise ValueError("invalid side")
    return {
        "dir": _text(fill.get("dir"), "dir"),
        "startPosition": _decimal(fill.get("startPosition"), "startPosition"),
        "time": _integer(fill.get("time"), "time"),
        "txIndex": _integer(fill.get("txIndex"), "txIndex"),
        "tid": _integer(fill.get("tid"), "tid"),
        "oid": _integer(fill.get("oid"), "oid"),
        "coin": _text(fill.get("coin"), "coin"),
        "side": side,
        "px": _decimal(fill.get("px"), "px", positive=True),
        "sz": _decimal(fill.get("sz"), "sz", positive=True),
    }


def _identity(fill: dict[str, object]) -> tuple[object, ...]:
    return tuple(fill[field] for field in ("time", "txIndex", "tid", "oid"))


def _payload(fill: dict[str, object]) -> tuple[object, ...]:
    return tuple(
        fill[field]
        for field in ("dir", "startPosition", "coin", "side", "px", "sz")
    )


async def backfill_wallet_directions(
    pool: Any,
    client: HydromancerRestClient,
    wallet: str,
    start: datetime,
    end: datetime,
) -> tuple[int, int]:
    """Backfill one known wallet from documented userFillsByTime and record provenance."""
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("valid timezone-aware half-open interval required")
    start_ms, end_ms = to_milliseconds(start), to_milliseconds(end)
    if start.microsecond % 1000:
        start_ms += 1
    if end.microsecond % 1000:
        end_ms += 1
    if end_ms <= start_ms:
        raise ValueError("interval contains no whole millisecond timestamps")
    normalized_wallet = wallet.strip().lower()
    if (
        len(normalized_wallet) != 42
        or not normalized_wallet.startswith("0x")
        or any(character not in "0123456789abcdef" for character in normalized_wallet[2:])
    ):
        raise ValueError("wallet must be a 0x-prefixed 20-byte hexadecimal address")
    local_candidates = _integer(
        await pool.fetchval(
            """SELECT COUNT(*) FROM orderflow_events
               WHERE wallet=$1 AND ts >= $2 AND ts < $3
                 AND (direction IS NULL OR start_position IS NULL)""",
            normalized_wallet,
            start,
            end,
        ),
        "local candidate count",
    )
    run_id = _integer(await pool.fetchval(
        """INSERT INTO direction_backfill_runs
           (wallet,start_ts,end_ts,source,source_limitations,fetched_count,matched_count,
            local_candidate_count)
           VALUES ($1,$2,$3,'userFillsByTime',$4,0,0,$5) RETURNING id""",
        normalized_wallet,
        start,
        end,
        LIMITATIONS,
        local_candidates,
    ), "backfill run id")
    fetched = matched = unusable = rejected = 0
    usable: list[dict[str, object]] = []
    seen: dict[tuple[object, ...], tuple[object, ...]] = {}
    try:
        # Retrieve and validate the complete source before opening the write transaction.
        # This includes SQL-bound types and duplicate consistency, not just required keys.
        for page in client.user_fills_pages(normalized_wallet, start_ms, end_ms - 1):
            for raw_fill in page:
                fetched += 1
                try:
                    fill = _validated_fill(raw_fill)
                except (TypeError, ValueError):
                    unusable += 1
                    continue
                if not start_ms <= _integer(fill["time"], "time") < end_ms:
                    rejected += 1
                    continue
                identity = _identity(fill)
                payload = _payload(fill)
                previous = seen.get(identity)
                if previous is not None:
                    if previous != payload:
                        raise ValueError("conflicting duplicate userFillsByTime identity")
                    continue
                seen[identity] = payload
                usable.append(fill)

        async with pool.acquire() as connection, connection.transaction():
            for fill in usable:
                result = await connection.execute(
                    """UPDATE orderflow_events SET
                       direction=COALESCE(direction,$1),
                       start_position=COALESCE(start_position,$2),
                       direction_provenance=CASE WHEN direction IS NULL
                           THEN 'userFillsByTime' ELSE direction_provenance END,
                       start_position_provenance=CASE WHEN start_position IS NULL
                           THEN 'userFillsByTime' ELSE start_position_provenance END,
                       direction_reconstructed_at=CASE WHEN direction IS NULL
                           THEN NOW() ELSE direction_reconstructed_at END,
                       start_position_reconstructed_at=CASE WHEN start_position IS NULL
                           THEN NOW() ELSE start_position_reconstructed_at END
                       WHERE wallet=$3
                         AND ts=TIMESTAMPTZ 'epoch' + $4::bigint * INTERVAL '1 millisecond'
                         AND tx_index=$5 AND trade_id=$6 AND order_id=$7
                         AND symbol=$8
                         AND side=CASE $9 WHEN 'B' THEN 'buy' WHEN 'A' THEN 'sell' ELSE '' END
                         AND price=$10::numeric AND size=$11::numeric
                         AND (direction IS NULL OR start_position IS NULL)""",
                    fill["dir"],
                    fill["startPosition"],
                    normalized_wallet,
                    fill["time"],
                    fill["txIndex"],
                    str(fill["tid"]),
                    str(fill["oid"]),
                    fill["coin"],
                    fill["side"],
                    fill["px"],
                    fill["sz"],
                )
                try:
                    updated = int(result.rsplit(" ", 1)[-1])
                except (AttributeError, ValueError):
                    raise RuntimeError("unrecognized database update count") from None
                if updated not in {0, 1}:
                    raise RuntimeError("backfill identity matched multiple local rows")
                matched += updated
            if matched > fetched or matched > local_candidates:
                raise RuntimeError("backfill match count exceeded audited bounds")
            await connection.execute(
                """UPDATE direction_backfill_runs SET fetched_count=$1,matched_count=$2,
                   unusable_count=$3,rejected_count=$4,
                   coverage=CASE WHEN local_candidate_count=0 THEN NULL
                       ELSE $2::numeric/local_candidate_count END,
                   source_complete=TRUE,status='completed',failure_type=NULL,
                   completed_at=NOW() WHERE id=$5""",
                fetched,
                matched,
                unusable,
                rejected,
                run_id,
            )
    except Exception as exc:
        await pool.execute(
            """UPDATE direction_backfill_runs SET fetched_count=$1,matched_count=0,
               unusable_count=$2,rejected_count=$3,status='failed',failure_type=$4,
               source_complete=FALSE,coverage=NULL,completed_at=NOW() WHERE id=$5""",
            fetched,
            unusable,
            rejected,
            type(exc).__name__,
            run_id,
        )
        raise
    return fetched, matched
