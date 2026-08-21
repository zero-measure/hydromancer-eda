from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, cast

from pydantic import BaseModel, Field

EventType = Literal["trade", "fill", "cancel", "liquidation", "position_open", "position_close"]
Aggressor = Literal["taker", "maker", "unknown"]
Side = Literal["buy", "sell", "unknown"]
MarginMode = Literal["cross", "isolated", "unknown"]


def _first_present(payload: dict[str, Any], *keys: str) -> Any:
    """Return the first present non-null value, retaining valid falsy values."""
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


class OrderflowEvent(BaseModel):
    ts: datetime
    symbol: str
    wallet: str
    event_type: EventType = "trade"
    side: Side = "unknown"
    aggressor: Aggressor = "unknown"

    price: Decimal | None = None
    size: Decimal | None = None
    notional: Decimal | None = None
    leverage: Decimal | None = None
    margin_mode: MarginMode = "unknown"

    order_id: str | None = None
    trade_id: str | None = None
    position_id: str | None = None
    tx_index: int

    realized_pnl: Decimal | None = None
    liquidation: bool = False

    raw: dict[str, Any] = Field(default_factory=dict)

    @staticmethod
    def timestamp_from_hydromancer(payload: dict[str, Any]) -> datetime:
        ts_value = _first_present(payload, "time", "ts", "timestamp")
        if isinstance(ts_value, (int, float)) and not isinstance(ts_value, bool):
            return datetime.fromtimestamp(ts_value / 1000, tz=UTC)
        if isinstance(ts_value, str):
            ts = datetime.fromisoformat(ts_value)
            return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
        raise ValueError(f"Missing or invalid timestamp in payload: {payload}")

    @classmethod
    def from_hydromancer(cls, payload: dict[str, Any]) -> OrderflowEvent:
        """Normalize a Hydromancer fill while retaining unknown enum values as data."""
        ts = cls.timestamp_from_hydromancer(payload)
        price = _first_present(payload, "px", "price")
        size = _first_present(payload, "sz", "size")
        notional = payload.get("notional")
        if notional is None and price is not None and size is not None:
            notional = Decimal(str(price)) * Decimal(str(size))

        side_raw = str(payload.get("side", "unknown")).lower()
        side = cast(Side, {"b": "buy", "buy": "buy", "a": "sell", "sell": "sell"}.get(side_raw, "unknown"))
        aggressor_raw = str(payload.get("aggressor", "")).lower()
        if not aggressor_raw:
            crossed = payload.get("crossed")
            aggressor_raw = (
                "taker" if crossed is True else "maker" if crossed is False else "unknown"
            )
        aggressor = cast(Aggressor, aggressor_raw if aggressor_raw in {"taker", "maker"} else "unknown")
        margin_raw = str(payload.get("margin_mode", "unknown")).lower()
        margin_mode = cast(MarginMode, margin_raw if margin_raw in {"cross", "isolated"} else "unknown")

        direction = str(payload.get("dir", "")).lower()
        liquidation = bool(payload.get("liquidation", False) or "liquidat" in direction)
        # allFills defines the envelope address as the participant identity.
        wallet_value = _first_present(payload, "address", "user", "wallet")
        wallet = "" if wallet_value is None else str(wallet_value).strip().lower()
        if not wallet:
            raise ValueError("Missing wallet in payload")
        symbol_value = _first_present(payload, "coin", "symbol")
        symbol = "" if symbol_value is None else str(symbol_value).strip()
        if not symbol:
            raise ValueError("Missing symbol in payload")

        oid = _first_present(payload, "oid", "order_id")
        tid = _first_present(payload, "tid", "trade_id")
        position_id = payload.get("position_id")
        realized_pnl = _first_present(payload, "closedPnl", "realized_pnl")

        return cls(
            ts=ts,
            symbol=symbol,
            wallet=wallet,
            event_type="liquidation" if liquidation else "trade",
            side=side,
            aggressor=aggressor,
            price=Decimal(str(price)) if price is not None else None,
            size=Decimal(str(size)) if size is not None else None,
            notional=Decimal(str(notional)) if notional is not None else None,
            leverage=(
                Decimal(str(payload["leverage"]))
                if payload.get("leverage") is not None
                else None
            ),
            margin_mode=margin_mode,
            order_id=None if oid is None else str(oid),
            trade_id=None if tid is None else str(tid),
            position_id=None if position_id is None else str(position_id),
            tx_index=int(payload["txIndex"]),
            realized_pnl=Decimal(str(realized_pnl)) if realized_pnl is not None else None,
            liquidation=liquidation,
            raw=payload,
        )


class WalletLabelSnapshot(BaseModel):
    bucket_ts: datetime
    wallet: str

    taker_markout: float | None = None
    realized_pnl: float | None = None
    liquidation_count: int | None = None
    taker_ratio: float | None = None
    cancel_to_fill_ratio: float | None = None
    message_cadence_per_min: float | None = None
    holding_time_sec_median: float | None = None
    liquidity_provision_score: float | None = None
    round_number_notional_ratio: float | None = None
    max_leverage: float | None = None
    cross_margin_ratio: float | None = None
