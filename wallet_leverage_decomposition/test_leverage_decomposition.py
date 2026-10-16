from datetime import UTC, date, datetime

import pytest
from effective_leverage import (
    build_decile_figure,
    build_decile_notional_share_figure,
    build_decile_wave_figure,
    calculate_snapshot,
)
from leverage_decomposition import (
    BUCKET_LABELS,
    SnapshotObject,
    aggregate_api_snapshot,
    leverage_bucket,
    select_daily_snapshots,
    validate_aggregates,
)


@pytest.mark.parametrize(
    ("leverage", "expected"),
    [(1, ("1x", 0)), (9, ("9x", 8)), (10, ("10x–25x", 9)),
     (25, ("10x–25x", 9)), (26, (">25x", 10)), (100, (">25x", 10))],
)
def test_leverage_bucket_boundaries(leverage, expected):
    assert leverage_bucket(leverage) == expected


@pytest.mark.parametrize("leverage", [0, -1, 1.5, True, None, float("nan")])
def test_leverage_bucket_rejects_invalid_values(leverage):
    with pytest.raises((TypeError, ValueError)):
        leverage_bucket(leverage)


def test_selects_latest_object_per_day():
    objects = [
        SnapshotObject(date(2025, 8, 1), 100, "older", 1, "a"),
        SnapshotObject(date(2025, 8, 1), 200, "newer", 1, "b"),
        SnapshotObject(date(2025, 8, 2), 300, "next", 1, "c"),
    ]
    selected = select_daily_snapshots(objects, date(2025, 8, 1), date(2025, 8, 1))
    assert [item.key for item in selected] == ["newer"]


def test_api_aggregation_is_gross_and_reconciles_to_100_percent():
    # Frame shape: [identifier, market, positions, addresses].
    frame_btc = [
        "id", "BTC",
        [
            [1.0, 100.0, 0.0, 100.0, 0, 1, 50.0, 1000.0],
            [-2.0, -200.0, 0.0, 100.0, 0, 10, 150.0, 1000.0],
        ],
        ["wallet-a", "wallet-b"],
    ]
    frame_eth = [
        "id", "ETH",
        [[3.0, 300.0, 0.0, 100.0, 1, 30, 80.0, 1000.0]],
        ["wallet-a"],
    ]
    timestamp = datetime(2026, 1, 1, 0, 10, tzinfo=UTC)
    rows = aggregate_api_snapshot([frame_btc, frame_eth], timestamp)
    validate_aggregates(rows)

    assert len(rows) == len(BUCKET_LABELS)
    assert sum(row.notional_usd for row in rows) == 600.0
    assert sum(row.share_pct for row in rows) == pytest.approx(100.0)
    assert rows[0].notional_usd == 100.0
    assert rows[9].notional_usd == 200.0
    assert rows[10].notional_usd == 300.0
    assert all(row.total_wallet_count == 2 for row in rows)
    assert rows[0].wallet_count == 1
    assert rows[10].wallet_count == 1


def test_effective_leverage_summary_and_deciles(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    snapshot_dir = tmp_path / "date=2025-08-02"
    snapshot_dir.mkdir()
    path = snapshot_dir / "1_1754092948185.parquet"
    connection = duckdb.connect()
    try:
        # Twenty user/DEX observations produce two observations in every decile.
        connection.execute(
            """
            COPY (
                SELECT
                    'wallet-' || value::VARCHAR AS user,
                    CASE WHEN value <= 10 THEN 'BTC' ELSE 'xyz:TSLA' END AS market,
                    1.0 AS size,
                    value * 100.0 AS notional,
                    100.0 AS account_value
                FROM range(1, 21) values(value)
                UNION ALL
                SELECT 'wallet-1', 'ETH', 1.0, 50.0, 100.0
            ) TO ? (FORMAT PARQUET)
            """,
            [str(path)],
        )
        summaries, deciles = calculate_snapshot(connection, path)
    finally:
        connection.close()

    platform = next(row for row in summaries if row["scope"] == "platform")
    # Gross is 100 * sum(1..20), plus wallet-1's second position.
    assert platform["gross_notional_usd"] == pytest.approx(21_050.0)
    assert platform["account_value"] == pytest.approx(2_000.0)
    assert platform["aggregate_effective_leverage"] == pytest.approx(10.525)
    assert platform["p50_effective_leverage"] == pytest.approx(10.5)
    platform_deciles = [row for row in deciles if row["scope"] == "platform"]
    assert [row["decile"] for row in platform_deciles] == list(range(1, 11))
    assert all(row["wallet_dex_count"] == 2 for row in platform_deciles)
    assert sum(row["share_of_scope_gross_notional_pct"] for row in platform_deciles) == pytest.approx(100)

    figure = build_decile_figure(deciles)
    assert len(figure.data) == 10
    assert [trace.name for trace in figure.data] == [f"Decile {value}" for value in range(1, 11)]
    assert figure.layout.height == 2400
    assert figure.layout.xaxis10.rangeslider.visible is True
    assert all(
        getattr(figure.layout, f"yaxis{value if value > 1 else ''}").matches is None
        for value in range(1, 11)
    )
    assert all(
        getattr(figure.layout, f"yaxis{value if value > 1 else ''}").rangemode == "normal"
        for value in range(1, 11)
    )

    shares = build_decile_notional_share_figure(deciles)
    assert len(shares.data) == 10
    assert shares.layout.barmode == "stack"
    assert shares.layout.yaxis.range == (0, 100)
    assert sum(trace.y[0] for trace in shares.data) == pytest.approx(100)

    wave = build_decile_wave_figure(deciles)
    assert len(wave.frames) == 1
    assert len(wave.data[0].x) == 10
    assert list(wave.data[0].x) == list(range(1, 11))
    assert wave.layout.sliders[0].currentvalue.prefix == "Snapshot: "
