"""Hourly taker position-flow classification with explicit coverage."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from pydantic import BaseModel

ZERO = Decimal(0)
_DIRECTION_PROVENANCE = frozenset({"allFills", "userFillsByTime"})


def _finite_decimal(value: Any) -> Decimal:
    """Parse an external numeric value, rejecting NaN and infinities."""
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


class HourlyPositionFlow(BaseModel):
    """One UTC-hour flow bucket; net values are null when coverage is incomplete."""

    coin: str
    hour: datetime
    long_open_notional: Decimal | None
    long_close_notional: Decimal | None
    short_open_notional: Decimal | None
    short_close_notional: Decimal | None
    total_notional: Decimal | None
    classified_notional: Decimal | None
    unclassified_notional: Decimal | None
    coverage: Decimal | None
    interval_coverage: Decimal
    known_source_gap: bool = False
    net_long_notional: Decimal | None
    net_short_notional: Decimal | None


def _direction_matches(direction: str, start: Decimal, size: Decimal, side: str) -> bool:
    """Validate documented position direction against side and the implied flip."""
    end = start + (size if side == "buy" else -size)
    expected: str
    if side == "buy":
        expected = (
            "short > long"
            if start < 0 < end
            else ("close short" if start < 0 else "open long")
        )
    elif side == "sell":
        expected = (
            "long > short"
            if start > 0 > end
            else ("close long" if start > 0 else "open short")
        )
    else:
        return False
    normalized = " ".join(direction.strip().lower().split())
    # Liquidation labels are documented fill directions and have the same position motion.
    if normalized == expected:
        return True
    # Documented liquidation labels include margin qualifiers (for example,
    # "Liquidated Cross Long") but describe the same closing motion.
    if expected == "close long":
        return normalized.startswith("liquidated ") and normalized.endswith(" long")
    if expected == "close short":
        return normalized.startswith("liquidated ") and normalized.endswith(" short")
    return False


def _parts(start: Decimal, size: Decimal, side: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Return quantity parts (long open/close, short open/close), splitting flips."""
    size = abs(size)
    if side == "buy":
        short_close = min(size, max(-start, ZERO))
        return size - short_close, ZERO, ZERO, short_close
    if side == "sell":
        long_close = min(size, max(start, ZERO))
        return ZERO, long_close, size - long_close, ZERO
    raise ValueError("unknown side")


def classify_hourly_rows(
    rows: list[dict[str, Any]],
    coins: list[str],
    start: datetime,
    end: datetime,
    *,
    allow_partial: bool = False,
    known_gap_hours: set[datetime] | None = None,
) -> list[HourlyPositionFlow]:
    """Aggregate taker participant rows into complete UTC hours over ``[start,end)``."""
    if start.tzinfo is None or end.tzinfo is None or start >= end:
        raise ValueError("valid timezone-aware half-open interval required")
    start_utc, end_utc = start.astimezone(UTC), end.astimezone(UTC)
    known_gap_hours = known_gap_hours or set()
    first_hour = start_utc.replace(minute=0, second=0, microsecond=0)
    buckets: dict[tuple[str, datetime], dict[str, Decimal]] = {}
    hour = first_hour
    while hour < end_utc:
        for coin in coins:
            buckets[(coin, hour)] = {
                "lo": ZERO, "lc": ZERO, "so": ZERO, "sc": ZERO,
                "total": ZERO, "classified": ZERO,
            }
        hour += timedelta(hours=1)

    for row in rows:
        if row.get("aggressor") != "taker":
            continue
        ts, row_coin = row.get("ts"), row.get("symbol")
        if not isinstance(ts, datetime) or not isinstance(row_coin, str):
            continue
        ts = ts.astimezone(UTC)
        if not (start_utc <= ts < end_utc):
            continue
        key = (row_coin, ts.replace(minute=0, second=0, microsecond=0))
        if key not in buckets:
            continue
        try:
            price, size = _finite_decimal(row["price"]), abs(_finite_decimal(row["size"]))
            raw_notional = row.get("notional", price * size)
            notional = abs(_finite_decimal(raw_notional))
        except (ArithmeticError, KeyError, ValueError):
            # Persisted rows are numeric, but callers may classify external mappings.
            # An invalid notional cannot safely contribute to either the numerator or total.
            continue
        bucket = buckets[key]
        try:
            new_total = bucket["total"] + notional
        except ArithmeticError:
            continue
        if not new_total.is_finite():
            continue
        bucket["total"] = new_total
        direction = row.get("direction")
        start_position, side = row.get("start_position"), row.get("side")
        direction_source = row.get("direction_provenance")
        position_source = row.get("start_position_provenance")
        has_strict_provenance = (
            direction_source in _DIRECTION_PROVENANCE
            and position_source in _DIRECTION_PROVENANCE
        )
        if (
            not isinstance(direction, str)
            or start_position is None
            or side not in {"buy", "sell"}
            or size == ZERO
            or not has_strict_provenance
        ):
            continue
        try:
            position = _finite_decimal(start_position)
            if not _direction_matches(direction, position, size, str(side)):
                continue
            lo, lc, so, sc = _parts(position, size, str(side))
            # Allocate exact row notional proportionally, robust to notional != px*size.
            allocations = tuple(notional * part / size for part in (lo, lc, so, sc))
            if not all(value.is_finite() for value in allocations):
                continue
            new_values = tuple(
                bucket[name] + allocation
                for name, allocation in zip(("lo", "lc", "so", "sc"), allocations, strict=True)
            )
            new_classified = bucket["classified"] + notional
        except ArithmeticError:
            continue
        if not all(value.is_finite() for value in (*new_values, new_classified)):
            continue
        for name, value in zip(("lo", "lc", "so", "sc"), new_values, strict=True):
            bucket[name] = value
        bucket["classified"] = new_classified

    output: list[HourlyPositionFlow] = []
    for (coin, bucket_hour), values in buckets.items():
        total, classified = values["total"], values["classified"]
        unclassified = total - classified
        coverage = classified / total if total else None
        bucket_end = bucket_hour + timedelta(hours=1)
        observed_start = max(bucket_hour, start_utc)
        observed_end = min(bucket_end, end_utc)
        interval_coverage = Decimal(str((observed_end - observed_start).total_seconds())) / Decimal(3600)
        known_source_gap = bucket_hour in known_gap_hours
        complete = (
            total > ZERO
            and unclassified == ZERO
            and interval_coverage == Decimal(1)
            and not known_source_gap
        )
        expose = complete or (allow_partial and classified > ZERO)
        has_activity = total > ZERO
        output.append(
            HourlyPositionFlow(
                coin=coin,
                hour=bucket_hour,
                long_open_notional=values["lo"] if has_activity else None,
                long_close_notional=values["lc"] if has_activity else None,
                short_open_notional=values["so"] if has_activity else None,
                short_close_notional=values["sc"] if has_activity else None,
                total_notional=total if has_activity else None,
                classified_notional=classified if has_activity else None,
                unclassified_notional=unclassified if has_activity else None,
                coverage=coverage,
                interval_coverage=interval_coverage,
                known_source_gap=known_source_gap,
                net_long_notional=(values["lo"] - values["lc"]) if expose else None,
                # Keep the analytical metric positive for net short opening; the
                # visualization negates it to place short accumulation below zero.
                net_short_notional=(values["so"] - values["sc"]) if expose else None,
            )
        )
    return sorted(output, key=lambda item: (item.coin, item.hour))


async def load_hourly_position_flow(
    pool: Any,
    coins: list[str],
    start: datetime,
    end: datetime,
    *,
    allow_partial: bool = False,
) -> list[HourlyPositionFlow]:
    """Load only taker participant rows and classify them without party double-counting."""
    records = await pool.fetch(
        """SELECT ts,symbol,side,aggressor,price,size,notional,direction,start_position,
                  direction_provenance,start_position_provenance
           FROM orderflow_events
           WHERE symbol = ANY($1::text[]) AND ts >= $2 AND ts < $3
             AND aggressor = 'taker'
           ORDER BY ts,tx_index,id""",
        coins,
        start,
        end,
    )
    gaps = await pool.fetch(
        """SELECT start_ts,end_ts FROM ingestion_gaps
           WHERE channel='allFills' AND end_ts >= $1 AND start_ts < $2
           ORDER BY start_ts""",
        start,
        end,
    )
    gap_hours: set[datetime] = set()
    for record in gaps:
        gap_start = record["start_ts"].astimezone(UTC).replace(
            minute=0, second=0, microsecond=0
        )
        gap_end = record["end_ts"].astimezone(UTC).replace(
            minute=0, second=0, microsecond=0
        )
        hour = gap_start
        while hour <= gap_end:
            gap_hours.add(hour)
            hour += timedelta(hours=1)
    return classify_hourly_rows(
        [dict(record) for record in records],
        coins,
        start,
        end,
        allow_partial=allow_partial,
        known_gap_hours=gap_hours,
    )
