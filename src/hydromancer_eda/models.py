"""Canonical models for flow, book, leverage, and wallet-label data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, cast

from pydantic import BaseModel, Field, field_validator

Aggressor = Literal["taker", "maker", "unknown"]
Side = Literal["buy", "sell", "unknown"]
DirectionProvenance = Literal["allFills", "userFillsByTime"]


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    """Return the first present, non-null payload value."""
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


class OrderflowEvent(BaseModel):
    """Represent one normalized participant-level allFills execution."""

    ts: datetime
    symbol: str
    wallet: str
    side: Side = "unknown"
    aggressor: Aggressor = "unknown"
    price: Decimal
    size: Decimal
    notional: Decimal
    order_id: str | None = None
    trade_id: str | None = None
    tx_index: int
    realized_pnl: Decimal | None = None
    direction: str | None = None
    start_position: Decimal | None = None
    direction_provenance: DirectionProvenance | None = None
    start_position_provenance: DirectionProvenance | None = None
    liquidation: bool = False
    liquidation_mark_price: Decimal | None = None
    liquidation_method: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict, exclude=True)

    @staticmethod
    def timestamp_from_hydromancer(payload: dict[str, Any]) -> datetime:
        """Parse a Hydromancer timestamp as a timezone-aware datetime."""
        ts_value = _first_present(payload, "time", "ts", "timestamp")
        if isinstance(ts_value, (int, float)) and not isinstance(ts_value, bool):
            return datetime.fromtimestamp(ts_value / 1000, tz=UTC)
        if isinstance(ts_value, str):
            ts = datetime.fromisoformat(ts_value)
            return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
        raise ValueError(f"Missing or invalid timestamp in payload: {payload}")

    @classmethod
    def from_hydromancer(cls, payload: dict[str, Any]) -> OrderflowEvent:
        """Normalize a Hydromancer participant fill."""
        ts = cls.timestamp_from_hydromancer(payload)
        price_value = _first_present(payload, "px", "price")
        size_value = _first_present(payload, "sz", "size")
        if price_value is None or size_value is None:
            raise ValueError("Missing fill price or size")
        price = Decimal(str(price_value))
        size = Decimal(str(size_value))

        side_raw = str(payload.get("side", "unknown")).lower()
        side = cast(
            Side,
            {"b": "buy", "buy": "buy", "a": "sell", "sell": "sell"}.get(
                side_raw, "unknown"
            ),
        )
        aggressor_raw = str(payload.get("aggressor", "")).lower()
        if not aggressor_raw:
            crossed = payload.get("crossed")
            aggressor_raw = (
                "taker" if crossed is True else "maker" if crossed is False else "unknown"
            )
        aggressor = cast(
            Aggressor,
            aggressor_raw if aggressor_raw in {"taker", "maker"} else "unknown",
        )

        wallet_value = _first_present(payload, "address", "user", "wallet")
        wallet = "" if wallet_value is None else str(wallet_value).strip().lower()
        symbol_value = _first_present(payload, "coin", "symbol")
        symbol = "" if symbol_value is None else str(symbol_value).strip()
        if not wallet or not symbol:
            raise ValueError("Missing wallet or symbol")

        liquidation_data = payload.get("liquidation")
        liquidation_details = liquidation_data if isinstance(liquidation_data, dict) else None
        liquidated_user = (
            str(liquidation_details.get("liquidatedUser", "")).strip().lower()
            if liquidation_details
            else ""
        )
        liquidation = bool(liquidated_user and liquidated_user == wallet)
        liquidation_mark = liquidation_details.get("markPx") if liquidation_details else None
        liquidation_method = liquidation_details.get("method") if liquidation_details else None
        notional_value = payload.get("notional")
        notional = (
            Decimal(str(notional_value))
            if notional_value is not None
            else abs(price * size)
        )
        oid = _first_present(payload, "oid", "order_id")
        tid = _first_present(payload, "tid", "trade_id")
        realized_pnl = _first_present(payload, "closedPnl", "realized_pnl")
        direction = payload.get("dir")
        start_position = payload.get("startPosition")

        return cls(
            ts=ts,
            symbol=symbol,
            wallet=wallet,
            side=side,
            aggressor=aggressor,
            price=price,
            size=size,
            notional=notional,
            order_id=None if oid is None else str(oid),
            trade_id=None if tid is None else str(tid),
            tx_index=int(payload["txIndex"]),
            realized_pnl=Decimal(str(realized_pnl)) if realized_pnl is not None else None,
            direction=str(direction) if direction is not None else None,
            start_position=(
                Decimal(str(start_position)) if start_position is not None else None
            ),
            direction_provenance="allFills" if direction is not None else None,
            start_position_provenance="allFills" if start_position is not None else None,
            liquidation=liquidation,
            liquidation_mark_price=(
                Decimal(str(liquidation_mark))
                if liquidation and liquidation_mark is not None
                else None
            ),
            liquidation_method=(
                str(liquidation_method)
                if liquidation and liquidation_method is not None
                else None
            ),
            raw=payload,
        )


class FundingObservation(BaseModel):
    """One exact-timestamp funding-rate observation."""

    coin: str
    ts: datetime
    rate: Decimal
    source: Literal["fundingHistory", "fundingRates"] = "fundingHistory"

    @field_validator("coin")
    @classmethod
    def validate_coin(cls, value: str) -> str:
        """Require a nonempty case-preserved protocol coin identifier."""
        if not value:
            raise ValueError("coin must not be empty")
        return value

    @field_validator("ts")
    @classmethod
    def validate_timestamp(cls, value: datetime) -> datetime:
        """Require an aware timestamp and normalize it to UTC without rounding."""
        if value.tzinfo is None:
            raise ValueError("funding timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("rate")
    @classmethod
    def validate_rate(cls, value: Decimal) -> Decimal:
        """Reject non-finite rates while preserving decimal precision."""
        if not value.is_finite():
            raise ValueError("funding rate must be finite")
        return value

    @classmethod
    def from_rest(cls, payload: dict[str, Any]) -> FundingObservation:
        """Parse a fundingHistory row without losing decimal or timestamp precision."""
        coin, timestamp, rate = payload.get("coin"), payload.get("time"), payload.get("fundingRate")
        if not isinstance(coin, str) or not isinstance(timestamp, int) or isinstance(timestamp, bool):
            raise TypeError("Invalid funding observation identity")
        if rate is None:
            raise ValueError("Missing funding rate")
        return cls(
            coin=coin,
            ts=datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=timestamp),
            rate=Decimal(str(rate)),
        )


class L2BookTick(BaseModel):
    """Represent one post-diff per-coin L2 book state."""

    ts: datetime
    symbol: str
    height: int
    epoch: str
    source_seq: int
    event_no: int
    best_bid: Decimal
    best_ask: Decimal
    mid_price: Decimal
    spread_bps: float
    bid_size: Decimal
    ask_size: Decimal
    imbalance: float
    trailing_event_volatility: float | None = None


class L4OrderEvent(BaseModel):
    """Represent one wallet-attributed L4 order lifecycle update."""

    ts: datetime
    height: int
    event_index: int
    symbol: str
    order_id: str
    wallet: str | None = None
    event_type: Literal["new", "update", "remove"]
    side: Side = "unknown"
    price: Decimal | None = None
    size: Decimal | None = None
    resting_seconds: float | None = None
    aggregate: bool = Field(default=True, exclude=True)


class LeverageUpdate(BaseModel):
    """Represent one leverage or isolated-margin update."""

    ts: datetime
    wallet: str
    symbol: str
    update_type: Literal["leverage", "isolated_margin", "top_up_isolated_margin"]
    leverage: float | None = None
    is_cross: bool | None = None
    is_buy: bool | None = None
    ntli: Decimal | None = None
    tx_index: int


MarkoutMetrics = dict[str, dict[str, float | int | None]]


class WalletLabelSnapshot(BaseModel):
    """Represent one current toxic-flow classifier feature row per wallet."""

    bucket_ts: datetime
    wallet: str
    trade_count: int
    taker_ratio: float | None = None
    liquidation_ratio: float | None = None
    round_number_notional_ratio: float | None = None
    round_number_size_ratio: float | None = None
    message_cadence_per_min: float | None = None
    realized_pnl: float | None = None
    max_leverage: float | None = None
    current_max_leverage: float | None = None
    leverage_update_count: int = 0
    isolated_margin_update_count: int = 0
    order_new_count: int = 0
    order_update_count: int = 0
    order_remove_count: int = 0
    order_remove_ratio: float | None = None
    round_order_size_ratio: float | None = None
    average_order_lifetime_seconds: float | None = None
    markout_metrics: MarkoutMetrics = Field(default_factory=dict)
