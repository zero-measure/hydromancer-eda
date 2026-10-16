from __future__ import annotations

import csv
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from hydromancer_eda.analysis.position_flow import classify_hourly_rows
from hydromancer_eda.cli import parse_interval_value
from hydromancer_eda.client.rest import HydromancerAPIError
from hydromancer_eda.funding import fetch_funding_history
from hydromancer_eda.models import FundingObservation, OrderflowEvent
from hydromancer_eda.visualization.funding_flow import (
    build_funding_flow_figure,
    write_analysis_csv,
    write_analysis_html,
)


class FakeClient:
    def __init__(self, pages):
        self.pages = iter(pages)
        self.payloads = []

    def post(self, payload):
        self.payloads.append(payload)
        return next(self.pages)


def test_funding_model_preserves_exact_timestamp_and_decimal() -> None:
    item = FundingObservation.from_rest(
        {"coin": "xyz:GOLD", "fundingRate": "0.0000125000001", "time": 1762398000007}
    )
    assert item.coin == "xyz:GOLD"
    assert item.rate == Decimal("0.0000125000001")
    assert item.ts.microsecond == 7000


def test_funding_model_rejects_nonfinite_rates_and_naive_timestamps() -> None:
    with pytest.raises(ValueError, match="finite"):
        FundingObservation(
            coin="BTC",
            ts=datetime(2025, 8, 1, tzinfo=UTC),
            rate=Decimal("NaN"),
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        FundingObservation(
            coin="BTC",
            ts=datetime(2025, 8, 1, tzinfo=UTC).replace(tzinfo=None),
            rate=Decimal("0.1"),
        )


def test_orderflow_persists_documented_direction_fields() -> None:
    item = OrderflowEvent.from_hydromancer(
        {"coin": "BTC", "address": "0xabc", "px": "2", "sz": "3", "side": "B",
         "time": 1_700_000_000_000, "txIndex": 1, "crossed": True,
         "dir": "Close Short", "startPosition": "-2"}
    )
    assert item.direction == "Close Short"
    assert item.start_position == Decimal(-2)
    assert item.direction_provenance == "allFills"


def test_funding_pagination_is_half_open_deduped_and_case_preserving() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    end = datetime(2025, 8, 1, 3, tzinfo=UTC)
    base = int(start.timestamp() * 1000)
    row1 = {"coin": "hyna:BTC", "fundingRate": "0.1", "time": base + 7}
    row2 = {"coin": "hyna:BTC", "fundingRate": "0.2", "time": base + 3_600_009}
    client = FakeClient([[row1, row2], [row2]])
    items = fetch_funding_history(client, "hyna:BTC", start, end, limit=2)
    assert [(item.rate, item.ts.microsecond) for item in items] == [
        (Decimal("0.1"), 7000), (Decimal("0.2"), 9000)
    ]
    assert client.payloads[0]["coin"] == "hyna:BTC"
    assert client.payloads[0]["endTime"] == int(end.timestamp() * 1000) - 1
    assert client.payloads[1]["startTime"] == row2["time"] + 1


def test_funding_history_rejects_dates_before_documented_availability() -> None:
    with pytest.raises(ValueError, match="2025-07-28"):
        fetch_funding_history(
            FakeClient([]),
            "BTC",
            datetime(2025, 7, 27, tzinfo=UTC),
            datetime(2025, 7, 28, tzinfo=UTC),
        )


def test_funding_page_rejects_wrong_coin_even_outside_local_filter() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    client = FakeClient(
        [[{"coin": "ETH", "fundingRate": "1", "time": int(start.timestamp() * 1000) - 1}]]
    )
    with pytest.raises(HydromancerAPIError, match="coin mismatch"):
        fetch_funding_history(
            client, "BTC", start, datetime(2025, 8, 2, tzinfo=UTC)
        )


def test_funding_pagination_rejects_no_progress() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    stale = int(start.timestamp() * 1000) - 1
    client = FakeClient([[{"coin": "BTC", "fundingRate": "1", "time": stale}]])
    with pytest.raises(HydromancerAPIError, match="no progress"):
        fetch_funding_history(client, "BTC", start, datetime(2025, 8, 2, tzinfo=UTC), limit=1)


def test_date_and_timestamp_interval_semantics() -> None:
    assert parse_interval_value("2025-08-01", is_end=False) == datetime(2025, 8, 1, tzinfo=UTC)
    assert parse_interval_value("2025-08-01", is_end=True) == datetime(2025, 8, 2, tzinfo=UTC)
    assert parse_interval_value("2025-08-01T12:00:00Z", is_end=True) == datetime(
        2025, 8, 1, 12, tzinfo=UTC
    )
    assert parse_interval_value("2025-08-01T12:00:00", is_end=False) == datetime(
        2025, 8, 1, 12, tzinfo=UTC
    )


def test_position_flow_splits_flip_and_inhibits_partial() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = [
        # Buy 3 from -2: two short close, one long open; $30 allocated 20/10.
        {"ts": start, "symbol": "BTC", "aggressor": "taker", "side": "buy",
         "price": Decimal(10), "size": Decimal(3), "notional": Decimal(30),
         "direction": "Short > Long", "start_position": Decimal(-2),
         "direction_provenance": "allFills", "start_position_provenance": "allFills"},
        # Taker but legacy direction is missing: hour must be incomplete.
        {"ts": start, "symbol": "BTC", "aggressor": "taker", "side": "sell",
         "price": Decimal(10), "size": Decimal(1), "notional": Decimal(10),
         "direction": None, "start_position": None},
        # Maker participant is deliberately excluded, preventing party double-counting.
        {"ts": start, "symbol": "BTC", "aggressor": "maker", "side": "sell",
         "price": Decimal(10), "size": Decimal(3), "notional": Decimal(30),
         "direction": "Open Short", "start_position": Decimal(0),
         "direction_provenance": "allFills", "start_position_provenance": "allFills"},
    ]
    strict = classify_hourly_rows(rows, ["BTC"], start, datetime(2025, 1, 1, 1, tzinfo=UTC))[0]
    assert strict.long_open_notional == Decimal(10)
    assert strict.short_close_notional == Decimal(20)
    assert strict.total_notional == Decimal(40)
    assert strict.coverage == Decimal("0.75")
    assert strict.interval_coverage == Decimal(1)
    assert strict.net_long_notional is None
    partial = classify_hourly_rows(
        rows, ["BTC"], start, datetime(2025, 1, 1, 1, tzinfo=UTC), allow_partial=True
    )[0]
    assert partial.net_long_notional == Decimal(10)
    assert partial.net_short_notional == Decimal(-20)


def test_known_allfills_gap_inhibits_otherwise_complete_hour() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    row = {
        "ts": start,
        "symbol": "BTC",
        "aggressor": "taker",
        "side": "buy",
        "price": Decimal(10),
        "size": Decimal(1),
        "notional": Decimal(10),
        "direction": "Open Long",
        "start_position": Decimal(0),
        "direction_provenance": "allFills",
        "start_position_provenance": "allFills",
    }
    strict = classify_hourly_rows(
        [row],
        ["BTC"],
        start,
        datetime(2025, 8, 1, 1, tzinfo=UTC),
        known_gap_hours={start},
    )[0]
    assert strict.known_source_gap
    assert strict.net_long_notional is None
    partial = classify_hourly_rows(
        [row],
        ["BTC"],
        start,
        datetime(2025, 8, 1, 1, tzinfo=UTC),
        known_gap_hours={start},
        allow_partial=True,
    )[0]
    assert partial.net_long_notional == Decimal(10)


def test_partial_edge_hour_is_null_without_explicit_override() -> None:
    start = datetime(2025, 1, 1, 0, 30, tzinfo=UTC)
    row = {"ts": start, "symbol": "BTC", "aggressor": "taker", "side": "buy",
           "price": Decimal(10), "size": Decimal(1), "notional": Decimal(10),
           "direction": "Open Long", "start_position": Decimal(0),
           "direction_provenance": "allFills", "start_position_provenance": "allFills"}
    strict = classify_hourly_rows([row], ["BTC"], start, datetime(2025, 1, 1, 1, tzinfo=UTC))[0]
    assert strict.coverage == Decimal(1)
    assert strict.interval_coverage == Decimal("0.5")
    assert strict.net_long_notional is None


def test_chart_and_csv_use_exact_funding_timestamp(tmp_path) -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    flows = classify_hourly_rows([], ["BTC"], start, datetime(2025, 1, 1, 1, tzinfo=UTC))
    funding = [FundingObservation(coin="BTC", ts=start.replace(microsecond=7000), rate=Decimal(".01"))]
    figure = build_funding_flow_figure(["BTC"], flows, funding)
    assert figure.data[2].x[0].microsecond == 7000
    assert flows[0].total_notional is None
    assert flows[0].long_open_notional is None
    assert figure.data[0].y[0] is None
    first_html, second_html = tmp_path / "first.html", tmp_path / "second.html"
    write_analysis_html(first_html, figure)
    write_analysis_html(second_html, figure)
    assert first_html.read_bytes() == second_html.read_bytes()
    assert 'id="hydromancer-funding-flow"' in first_html.read_text()
    path = tmp_path / "analysis.csv"
    write_analysis_csv(path, flows, funding)
    records = list(csv.DictReader(path.open()))
    assert records[-1]["funding_timestamp_utc"].endswith(".007000+00:00")


def test_funding_conflicting_overlap_fails_closed() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    timestamp = int(start.timestamp() * 1000)
    first = {"coin": "BTC", "fundingRate": "1", "time": timestamp}
    conflict = {"coin": "BTC", "fundingRate": "2", "time": timestamp}
    client = FakeClient([[first], [conflict]])
    with pytest.raises(HydromancerAPIError, match="conflicting duplicate"):
        fetch_funding_history(client, "BTC", start, datetime(2025, 8, 2, tzinfo=UTC), limit=1)


def test_funding_malformed_full_page_fails_closed() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    client = FakeClient([[{"coin": "BTC", "fundingRate": "1", "time": 1}, "bad"]])
    with pytest.raises(HydromancerAPIError, match="malformed"):
        fetch_funding_history(client, "BTC", start, datetime(2025, 8, 2, tzinfo=UTC), limit=2)
