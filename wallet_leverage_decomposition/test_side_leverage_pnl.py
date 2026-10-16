from datetime import date

import pytest
from side_leverage_pnl import build_figure, calculate_snapshot


def test_split_allocated_equity_and_pnl(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    snapshot_dir = tmp_path / "date=2026-09-22"
    snapshot_dir.mkdir()
    path = snapshot_dir / "1_1790035523948.parquet"
    connection = duckdb.connect()
    try:
        # wallet-a trades both sides on native; wallet-b holds HIP-3 shorts.
        # wallet-c has invalid equity but still contributes notional and PnL.
        connection.execute(
            """COPY (
                SELECT * FROM (VALUES
                    ('wallet-a', 'BTC', 2.0, 240.0, 100.0, 5.0, 120.0),
                    ('wallet-a', 'ETH', -1.0, 80.0, 100.0, -2.0, 120.0),
                    ('wallet-b', 'xyz:TSLA', -3.0, 270.0, 100.0, 1.0, 90.0),
                    ('wallet-c', 'BTC', 1.0, 100.0, 90.0, 2.0, -10.0),
                    ('wallet-a', 'xyz:MSFT', 1.0, 50.0, 40.0, 0.0, 25.0),
                    ('wallet-d', 'BTC', 1.0, 10.0, NULL, 3.0, 10.0),
                    ('wallet-e', 'BTC', 1.0, 15.0, 15.0, 0.0, NULL)
                ) rows(user, market, size, notional, entry_price, funding_pnl, account_value)
            ) TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
        rows = calculate_snapshot(connection, path)
        long = next(row for row in rows if row["scope"] == "platform" and row["side"] == "long")
        short = next(row for row in rows if row["scope"] == "platform" and row["side"] == "short")
    finally:
        connection.close()

    # 120 native equity splits 90/30 for wallet-a, xyz collateral stays independent.
    assert long["gross_notional_usd"] == pytest.approx(415)
    assert long["eligible_gross_notional_usd"] == pytest.approx(300)
    assert long["allocated_account_value"] == pytest.approx(125)
    assert long["aggregate_effective_leverage"] == pytest.approx(2.4)
    assert long["notional_weighted_effective_leverage"] == pytest.approx(2.5)
    assert long["excluded_nonpositive_account_count"] == 2
    assert long["position_count"] == 5
    assert long["unrealized_pnl_usd"] == pytest.approx(40 + 10 + 10)
    assert long["cumulative_funding_pnl_usd"] == pytest.approx(7)
    assert long["net_open_pnl_usd"] == pytest.approx(67)
    assert long["pnl_position_count"] == 4
    assert long["excluded_pnl_position_count"] == 1
    assert short["gross_notional_usd"] == pytest.approx(350)
    assert short["allocated_account_value"] == pytest.approx(120)
    assert short["aggregate_effective_leverage"] == pytest.approx(350 / 120)
    assert short["notional_weighted_effective_leverage"] == pytest.approx(
        (80 * 80 / 30 + 270 * 270 / 90) / 350
    )
    assert short["unrealized_pnl_usd"] == pytest.approx(20 + 30)
    assert short["cumulative_funding_pnl_usd"] == pytest.approx(-1)
    assert short["net_open_pnl_usd"] == pytest.approx(49)
    assert long["allocated_account_value"] + short["allocated_account_value"] == pytest.approx(245)
    assert long["snapshot_date"] == date(2026, 9, 22).isoformat()
    assert all(r["scope"] in ("platform", "dex") for r in rows)

    figure = build_figure(rows)
    assert len(figure.data) == 6
    assert figure.data[0].name == "Long"
    assert figure.data[3].name == "Short"
    assert figure.data[0].connectgaps is False
    assert figure.data[1].y[0] == pytest.approx(2.5)
    assert figure.data[4].y[0] == pytest.approx(short["notional_weighted_effective_leverage"])
    assert [trace.name for trace in figure.data if trace.showlegend] == ["Long", "Short"]
    for axis in (figure.layout.xaxis, figure.layout.xaxis2, figure.layout.xaxis3):
        assert axis.showticklabels is True
        assert axis.title.text == "Snapshot date (UTC)"
        assert axis.tickformat == "%b %Y"
    assert figure.layout.yaxis2.title.text == "Notional-weighted effective leverage (×)"
    assert figure.layout.xaxis3.rangeslider.visible is True


def test_inconsistent_equity_rejected(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    snapshot_dir = tmp_path / "date=2026-09-22"
    snapshot_dir.mkdir()
    path = snapshot_dir / "1_1790035523948.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """COPY (SELECT * FROM (VALUES
                ('a', 'BTC', 1.0, 100.0, 100.0, 0.0, 100.0),
                ('a', 'ETH', -1.0, 100.0, 100.0, 0.0, 200.0)
            ) rows(user, market, size, notional, entry_price, funding_pnl, account_value))
            TO ? (FORMAT PARQUET)""",
            [str(path)],
        )
        with pytest.raises(ValueError, match="inconsistent"):
            calculate_snapshot(connection, path)
    finally:
        connection.close()
