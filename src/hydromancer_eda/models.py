"""Canonical models for normalized order flow and wallet label snapshots."""

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
    """Return the first present, non-null payload value.

    Args:
        payload (dict[str, Any]): Mapping to inspect.
        *keys (str): Candidate keys in descending priority order.

    Returns:
        Any: First non-null value, including valid falsy values, or ``None``.

    """
    for key in keys:
        value = payload.get(key)
        if value is not None:
            return value
    return None


class OrderflowEvent(BaseModel):
    """Represent one normalized participant-level allFills event.

    Unknown side, aggressor, and margin values normalize to ``"unknown"``.
    The ``raw`` field retains the ingestion payload, including its canonical
    envelope wallet address.
    """

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
        """Parse a Hydromancer timestamp as a timezone-aware datetime.

        Args:
            payload (dict[str, Any]): Fill payload containing ``time``, ``ts``,
                or ``timestamp`` as epoch milliseconds or an ISO-8601 string.

        Returns:
            datetime: Parsed timestamp; naive strings are interpreted as UTC.

        Raises:
            ValueError: If no supported timestamp value is present or parseable.

        """
        ts_value = _first_present(payload, "time", "ts", "timestamp")
        if isinstance(ts_value, (int, float)) and not isinstance(ts_value, bool):
            return datetime.fromtimestamp(ts_value / 1000, tz=UTC)
        if isinstance(ts_value, str):
            ts = datetime.fromisoformat(ts_value)
            return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts
        raise ValueError(f"Missing or invalid timestamp in payload: {payload}")

    @classmethod
    def from_hydromancer(cls, payload: dict[str, Any]) -> OrderflowEvent:
        """Normalize a Hydromancer fill into an order-flow event.

        Args:
            payload (dict[str, Any]): Fill payload augmented with the canonical
                allFills envelope address.

        Returns:
            OrderflowEvent: Validated participant-level event.

        Raises:
            KeyError: If the required ``txIndex`` field is absent.
            TypeError: If a required numeric value has an unsupported type.
            ValueError: If required identity fields or numeric values are invalid.
            ArithmeticError: If derived notional arithmetic fails.

        """
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
    """Represent wallet-flow metrics computed for one aggregation timestamp.

    Optional fields cover taker markout, realized performance, liquidation and
    message activity, holding time, liquidity provision, notional preferences,
    leverage, and margin-mode usage. A value is ``None`` when source events are
    insufficient to calculate the metric.
    """

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
