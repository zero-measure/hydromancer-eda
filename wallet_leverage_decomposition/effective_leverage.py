"""Calculate daily account-equity-based effective leverage distributions.

The cached Hydromancer Reservoir perp snapshots already contain ``account_value``
on every position row.  This program deduplicates that value at the user/DEX
level, sums gross position notional, and writes platform and per-DEX summaries.

A platform row follows the requested definition exactly over positioned
(user, DEX) observations::

    sum(gross_notional) / sum(account_value)

Wallet-level percentiles and deciles use ``gross_notional / account_value``.
Only positive, finite account values can enter the distribution.  Invalid
account-value observations are counted separately in the output.
"""
from __future__ import annotations

import argparse
import csv
import math
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from leverage_decomposition import (
    EARLIEST_SNAPSHOT_DATE,
    _timestamp_from_path,
    cached_snapshot_paths,
    parse_date,
)

PERCENTILES = (0.10, 0.20, 0.25, 0.30, 0.40, 0.50, 0.60, 0.70, 0.75, 0.80, 0.90, 0.95, 0.99)

SUMMARY_FIELDS = (
    "snapshot_date",
    "timestamp",
    "scope",
    "dex",
    "wallet_dex_count",
    "distinct_wallet_count",
    "excluded_nonpositive_account_count",
    "gross_notional_usd",
    "account_value",
    "aggregate_effective_leverage",
    "notional_weighted_effective_leverage",
    *(f"p{round(percentile * 100):02d}_effective_leverage" for percentile in PERCENTILES),
)

DECILE_FIELDS = (
    "snapshot_date",
    "timestamp",
    "scope",
    "dex",
    "decile",
    "wallet_dex_count",
    "distinct_wallet_count",
    "min_effective_leverage",
    "median_effective_leverage",
    "max_effective_leverage",
    "gross_notional_usd",
    "account_value",
    "aggregate_effective_leverage",
    "notional_weighted_effective_leverage",
    "share_of_scope_gross_notional_pct",
    "share_of_scope_account_value_pct",
)

# Market names use ``dex:market`` for HIP-3 and no prefix for native Hyperliquid.
BASE_SQL = """
WITH position_rows AS (
    SELECT
        user,
        CASE
            WHEN contains(market, ':') THEN split_part(market, ':', 1)
            ELSE 'hyperliquid'
        END AS dex,
        abs(notional) AS gross_notional,
        account_value
    FROM read_parquet(?)
    WHERE size != 0
      AND notional IS NOT NULL AND isfinite(notional)
),
wallet_dex_raw AS (
    SELECT
        user,
        dex,
        sum(gross_notional) AS gross_notional,
        min(account_value) AS min_account_value,
        max(account_value) AS account_value,
        count(DISTINCT account_value) AS account_value_versions
    FROM position_rows
    GROUP BY user, dex
),
wallet_dex AS (
    SELECT
        user,
        dex,
        gross_notional,
        account_value,
        account_value_versions,
        account_value > 0 AND isfinite(account_value) AS valid_account_value,
        CASE
            WHEN account_value > 0 AND isfinite(account_value)
            THEN gross_notional / account_value
        END AS effective_leverage
    FROM wallet_dex_raw
),
expanded AS (
    SELECT 'platform' AS scope, 'ALL' AS scope_dex, * FROM wallet_dex
    UNION ALL
    SELECT 'dex' AS scope, dex AS scope_dex, * FROM wallet_dex
)
"""

SUMMARY_SQL = BASE_SQL + """
SELECT
    scope,
    scope_dex,
    count(*) FILTER (WHERE valid_account_value) AS wallet_dex_count,
    count(DISTINCT user) FILTER (WHERE valid_account_value) AS distinct_wallet_count,
    count(*) FILTER (WHERE NOT valid_account_value) AS excluded_count,
    sum(gross_notional) FILTER (WHERE valid_account_value) AS gross_notional,
    sum(account_value) FILTER (WHERE valid_account_value) AS account_value,
    sum(gross_notional) FILTER (WHERE valid_account_value)
        / nullif(sum(account_value) FILTER (WHERE valid_account_value), 0)
        AS aggregate_effective_leverage,
    sum(effective_leverage * gross_notional) FILTER (WHERE valid_account_value)
        / nullif(sum(gross_notional) FILTER (WHERE valid_account_value), 0)
        AS notional_weighted_effective_leverage,
    quantile_cont(effective_leverage, ?) FILTER (WHERE valid_account_value) AS percentiles,
    max(account_value_versions) AS max_account_value_versions
FROM expanded
GROUP BY scope, scope_dex
ORDER BY CASE WHEN scope = 'platform' THEN 0 ELSE 1 END, scope_dex
"""

DECILE_SQL = BASE_SQL + """
, valid AS (
    SELECT *
    FROM expanded
    WHERE valid_account_value
),
ranked AS (
    SELECT
        *,
        ntile(10) OVER (
            PARTITION BY scope, scope_dex
            ORDER BY effective_leverage, user, dex
        ) AS decile
    FROM valid
),
scope_totals AS (
    SELECT
        scope,
        scope_dex,
        sum(gross_notional) AS scope_gross_notional,
        sum(account_value) AS scope_account_value
    FROM valid
    GROUP BY scope, scope_dex
)
SELECT
    ranked.scope,
    ranked.scope_dex,
    decile,
    count(*) AS wallet_dex_count,
    count(DISTINCT user) AS distinct_wallet_count,
    min(effective_leverage) AS min_effective_leverage,
    median(effective_leverage) AS median_effective_leverage,
    max(effective_leverage) AS max_effective_leverage,
    sum(gross_notional) AS gross_notional,
    sum(account_value) AS account_value,
    sum(gross_notional) / nullif(sum(account_value), 0) AS aggregate_effective_leverage,
    sum(effective_leverage * gross_notional) / nullif(sum(gross_notional), 0)
        AS notional_weighted_effective_leverage,
    100 * sum(gross_notional) / nullif(scope_gross_notional, 0) AS gross_share_pct,
    100 * sum(account_value) / nullif(scope_account_value, 0) AS account_value_share_pct
FROM ranked
JOIN scope_totals USING (scope, scope_dex)
GROUP BY ranked.scope, ranked.scope_dex, decile,
         scope_gross_notional, scope_account_value
ORDER BY CASE WHEN ranked.scope = 'platform' THEN 0 ELSE 1 END,
         ranked.scope_dex, decile
"""


def _finite_float(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("effective leverage output contained a non-finite value")
    return result


def calculate_snapshot(connection: Any, path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Calculate summaries and equal-count deciles for one position snapshot."""
    snapshot_date = date.fromisoformat(path.parent.name.removeprefix("date="))
    timestamp = _timestamp_from_path(path)
    timestamp_text = timestamp.isoformat()

    summary_rows: list[dict[str, Any]] = []
    records = connection.execute(SUMMARY_SQL, [str(path), list(PERCENTILES)]).fetchall()
    for record in records:
        (
            scope,
            dex,
            wallet_dex_count,
            distinct_wallet_count,
            excluded_count,
            gross_notional,
            account_value,
            aggregate_leverage,
            weighted_leverage,
            percentile_values,
            max_versions,
        ) = record
        if int(max_versions) > 1:
            raise ValueError(
                f"account_value is inconsistent within a user/DEX in snapshot {path}"
            )
        row: dict[str, Any] = {
            "snapshot_date": snapshot_date.isoformat(),
            "timestamp": timestamp_text,
            "scope": str(scope),
            "dex": str(dex),
            "wallet_dex_count": int(wallet_dex_count),
            "distinct_wallet_count": int(distinct_wallet_count),
            "excluded_nonpositive_account_count": int(excluded_count),
            "gross_notional_usd": _finite_float(gross_notional),
            "account_value": _finite_float(account_value),
            "aggregate_effective_leverage": _finite_float(aggregate_leverage),
            "notional_weighted_effective_leverage": _finite_float(weighted_leverage),
        }
        for percentile, value in zip(PERCENTILES, percentile_values, strict=True):
            row[f"p{round(percentile * 100):02d}_effective_leverage"] = _finite_float(value)
        summary_rows.append(row)

    decile_rows: list[dict[str, Any]] = []
    records = connection.execute(DECILE_SQL, [str(path)]).fetchall()
    for record in records:
        (
            scope,
            dex,
            decile,
            wallet_dex_count,
            distinct_wallet_count,
            minimum,
            median,
            maximum,
            gross_notional,
            account_value,
            aggregate_leverage,
            weighted_leverage,
            gross_share,
            account_share,
        ) = record
        decile_rows.append(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "timestamp": timestamp_text,
                "scope": str(scope),
                "dex": str(dex),
                "decile": int(decile),
                "wallet_dex_count": int(wallet_dex_count),
                "distinct_wallet_count": int(distinct_wallet_count),
                "min_effective_leverage": _finite_float(minimum),
                "median_effective_leverage": _finite_float(median),
                "max_effective_leverage": _finite_float(maximum),
                "gross_notional_usd": _finite_float(gross_notional),
                "account_value": _finite_float(account_value),
                "aggregate_effective_leverage": _finite_float(aggregate_leverage),
                "notional_weighted_effective_leverage": _finite_float(weighted_leverage),
                "share_of_scope_gross_notional_pct": _finite_float(gross_share),
                "share_of_scope_account_value_pct": _finite_float(account_share),
            }
        )
    return summary_rows, decile_rows


def write_rows(path: Path, fields: Sequence[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def build_decile_figure(rows: Sequence[dict[str, Any]]):  # type: ignore[no-untyped-def]
    """Build independently scaled time-series panels for each platform decile."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        raise RuntimeError("plotly is required; install requirements.txt") from None

    platform_rows = [row for row in rows if row["scope"] == "platform"]
    if not platform_rows:
        raise ValueError("no platform decile rows")
    colors = (
        "#440154", "#482878", "#3E4989", "#31688E", "#26828E",
        "#1F9E89", "#35B779", "#6DCD59", "#B4DE2C", "#FDE725",
    )
    subplot_titles = [
        f"Decile {decile} (percentiles {(decile - 1) * 10}–{decile * 10})"
        for decile in range(1, 11)
    ]
    figure = make_subplots(
        rows=10,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.018,
        subplot_titles=subplot_titles,
    )
    for decile, color in zip(range(1, 11), colors, strict=True):
        selected = sorted(
            (row for row in platform_rows if int(row["decile"]) == decile),
            key=lambda row: str(row["timestamp"]),
        )
        x: list[str] = []
        y: list[float | None] = []
        custom: list[list[Any]] = []
        previous_date: date | None = None
        baseline = float(selected[0]["notional_weighted_effective_leverage"])
        for row in selected:
            current_date = date.fromisoformat(str(row["snapshot_date"]))
            # An explicit null prevents Plotly from drawing through missing snapshot periods.
            if previous_date is not None and current_date - previous_date > timedelta(days=1):
                x.append((previous_date + timedelta(days=1)).isoformat())
                y.append(None)
                custom.append([None] * 8)
            weighted_leverage = float(row["notional_weighted_effective_leverage"])
            x.append(str(row["timestamp"]))
            y.append(weighted_leverage)
            custom.append(
                [
                    int(row["wallet_dex_count"]),
                    float(row["min_effective_leverage"]),
                    float(row["median_effective_leverage"]),
                    float(row["max_effective_leverage"]),
                    float(row["share_of_scope_gross_notional_pct"]),
                    float(row["gross_notional_usd"]),
                    float(row["aggregate_effective_leverage"]),
                    100 * (weighted_leverage / baseline - 1),
                ]
            )
            previous_date = current_date
        figure.add_trace(
            go.Scatter(
                x=x,
                y=y,
                customdata=custom,
                name=f"Decile {decile}",
                mode="lines",
                line={"color": color, "width": 2.2 if decile in (1, 10) else 1.7},
                connectgaps=False,
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "%{x|%Y-%m-%d %H:%M UTC}<br>"
                    "Notional-weighted effective leverage: %{y:.2f}x<br>"
                    "Change from first snapshot: %{customdata[7]:+.2f}%<br>"
                    "Decile range: %{customdata[1]:.2f}x–%{customdata[3]:.2f}x<br>"
                    "Decile median: %{customdata[2]:.2f}x<br>"
                    "Ratio of totals: %{customdata[6]:.2f}x<br>"
                    "Share of platform notional: %{customdata[4]:.2f}%<br>"
                    "Gross notional: $%{customdata[5]:,.0f}<br>"
                    "Wallet/DEX observations: %{customdata[0]:,}"
                    "<extra></extra>"
                ),
                showlegend=False,
            ),
            row=decile,
            col=1,
        )

    first_date = min(date.fromisoformat(str(row["snapshot_date"])) for row in platform_rows)
    latest_date = max(date.fromisoformat(str(row["snapshot_date"])) for row in platform_rows)
    axis_end = max(latest_date, datetime.now(UTC).date())
    figure.update_layout(
        title={
            "text": (
                "Platform Effective Leverage by Wallet/DEX Decile"
                f"<br><sup>Gross-notional-weighted within each equal-count decile; "
                f"each panel has an independent Y-axis; snapshots "
                f"{first_date} to {latest_date}</sup>"
            ),
            "x": 0.01,
        },
        template="plotly_white",
        height=2400,
        hovermode="x",
        margin={"l": 90, "r": 35, "t": 145, "b": 115},
    )
    figure.update_xaxes(
        range=[first_date.isoformat(), axis_end.isoformat()],
        showgrid=False,
    )
    figure.update_xaxes(
        title="Snapshot date (UTC)",
        rangeslider={"visible": True, "thickness": 0.04},
        rangeselector={
            "buttons": [
                {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                {"count": 3, "label": "3m", "step": "month", "stepmode": "backward"},
                {"count": 6, "label": "6m", "step": "month", "stepmode": "backward"},
                {"count": 1, "label": "1y", "step": "year", "stepmode": "backward"},
                {"step": "all", "label": "All"},
            ]
        },
        row=10,
        col=1,
    )
    figure.update_yaxes(
        title="Effective leverage",
        ticksuffix="x",
        rangemode="normal",
        gridcolor="rgba(0, 0, 0, 0.12)",
        automargin=True,
    )
    if latest_date < axis_end:
        figure.add_annotation(
            x=latest_date.isoformat(),
            y=1,
            yref="paper",
            text=f"Latest available snapshot: {latest_date}",
            showarrow=True,
            arrowhead=2,
            ax=-80,
            ay=35,
        )
    return figure


def build_decile_notional_share_figure(
    rows: Sequence[dict[str, Any]],
):  # type: ignore[no-untyped-def]
    """Build stacked daily bars of platform gross-notional share by decile."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        raise RuntimeError("plotly is required; install requirements.txt") from None

    platform_rows = [row for row in rows if row["scope"] == "platform"]
    if not platform_rows:
        raise ValueError("no platform decile rows")
    colors = (
        "#440154", "#482878", "#3E4989", "#31688E", "#26828E",
        "#1F9E89", "#35B779", "#6DCD59", "#B4DE2C", "#FDE725",
    )
    figure = go.Figure()
    for decile, color in zip(range(1, 11), colors, strict=True):
        selected = sorted(
            (row for row in platform_rows if int(row["decile"]) == decile),
            key=lambda row: str(row["timestamp"]),
        )
        figure.add_trace(
            go.Bar(
                x=[str(row["timestamp"]) for row in selected],
                y=[float(row["share_of_scope_gross_notional_pct"]) for row in selected],
                name=f"Decile {decile}",
                marker_color=color,
                width=0.85 * 24 * 60 * 60 * 1000,
                customdata=[
                    [
                        float(row["notional_weighted_effective_leverage"]),
                        float(row["min_effective_leverage"]),
                        float(row["median_effective_leverage"]),
                        float(row["max_effective_leverage"]),
                        float(row["gross_notional_usd"]),
                        int(row["wallet_dex_count"]),
                        float(row["aggregate_effective_leverage"]),
                    ]
                    for row in selected
                ],
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>"
                    "%{x|%Y-%m-%d %H:%M UTC}<br>"
                    "Share of platform notional: %{y:.2f}%<br>"
                    "Notional-weighted effective leverage: %{customdata[0]:.2f}x<br>"
                    "Decile range: %{customdata[1]:.2f}x–%{customdata[3]:.2f}x<br>"
                    "Decile median: %{customdata[2]:.2f}x<br>"
                    "Ratio of totals: %{customdata[6]:.2f}x<br>"
                    "Gross notional: $%{customdata[4]:,.0f}<br>"
                    "Wallet/DEX observations: %{customdata[5]:,}"
                    "<extra></extra>"
                ),
            )
        )

    first_date = min(date.fromisoformat(str(row["snapshot_date"])) for row in platform_rows)
    latest_date = max(date.fromisoformat(str(row["snapshot_date"])) for row in platform_rows)
    axis_end = max(latest_date, datetime.now(UTC).date())
    figure.update_layout(
        title={
            "text": (
                "Platform Gross Notional Share by Effective-Leverage Decile"
                f"<br><sup>Equal-count wallet/DEX deciles; snapshots "
                f"{first_date} to {latest_date}</sup>"
            ),
            "x": 0.01,
        },
        template="plotly_white",
        barmode="stack",
        height=760,
        hovermode="x unified",
        bargap=0.08,
        legend={
            "title": "Effective-leverage decile",
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
        },
        margin={"l": 85, "r": 35, "t": 125, "b": 100},
    )
    figure.update_xaxes(
        title="Snapshot date (UTC)",
        range=[first_date.isoformat(), axis_end.isoformat()],
        rangeslider={"visible": True, "thickness": 0.08},
        rangeselector={
            "buttons": [
                {"count": 1, "label": "1m", "step": "month", "stepmode": "backward"},
                {"count": 3, "label": "3m", "step": "month", "stepmode": "backward"},
                {"count": 6, "label": "6m", "step": "month", "stepmode": "backward"},
                {"count": 1, "label": "1y", "step": "year", "stepmode": "backward"},
                {"step": "all", "label": "All"},
            ]
        },
        showgrid=False,
    )
    figure.update_yaxes(
        title="Share of platform gross notional",
        range=[0, 100],
        ticksuffix="%",
        gridcolor="rgba(0, 0, 0, 0.12)",
    )
    if latest_date < axis_end:
        figure.add_annotation(
            x=latest_date.isoformat(),
            y=1,
            yref="paper",
            text=f"Latest available snapshot: {latest_date}",
            showarrow=True,
            arrowhead=2,
            ax=-80,
            ay=35,
        )
    return figure


def build_decile_wave_figure(rows: Sequence[dict[str, Any]]):  # type: ignore[no-untyped-def]
    """Build a GIF-like animated wave of notional-weighted leverage by decile."""
    try:
        import plotly.graph_objects as go
    except ImportError:
        raise RuntimeError("plotly is required; install requirements.txt") from None

    platform_rows = [row for row in rows if row["scope"] == "platform"]
    if not platform_rows:
        raise ValueError("no platform decile rows")
    by_date: dict[str, list[dict[str, Any]]] = {}
    for row in platform_rows:
        by_date.setdefault(str(row["snapshot_date"]), []).append(row)
    for snapshot_date, daily in by_date.items():
        daily.sort(key=lambda row: int(row["decile"]))
        if [int(row["decile"]) for row in daily] != list(range(1, 11)):
            raise ValueError(f"snapshot {snapshot_date} does not contain all ten deciles")

    dates = sorted(by_date)
    maximum = max(
        float(row["notional_weighted_effective_leverage"])
        for daily in by_date.values()
        for row in daily
    )
    y_max = math.ceil(maximum * 1.12 / 5) * 5

    def wave_trace(snapshot_date: str) -> Any:
        daily = by_date[snapshot_date]
        return go.Scatter(
            x=[int(row["decile"]) for row in daily],
            y=[float(row["notional_weighted_effective_leverage"]) for row in daily],
            customdata=[
                [
                    float(row["min_effective_leverage"]),
                    float(row["median_effective_leverage"]),
                    float(row["max_effective_leverage"]),
                    float(row["share_of_scope_gross_notional_pct"]),
                    float(row["gross_notional_usd"]),
                    int(row["wallet_dex_count"]),
                    float(row["aggregate_effective_leverage"]),
                ]
                for row in daily
            ],
            mode="lines+markers",
            line={"color": "#1E81B0", "width": 4, "shape": "spline", "smoothing": 1.15},
            marker={
                "color": list(range(1, 11)),
                "colorscale": "Viridis",
                "cmin": 1,
                "cmax": 10,
                "size": [
                    9 + 1.5 * math.sqrt(float(row["share_of_scope_gross_notional_pct"]))
                    for row in daily
                ],
                "line": {"color": "white", "width": 1},
            },
            fill="tozeroy",
            fillcolor="rgba(30, 129, 176, 0.22)",
            name="Leverage distribution",
            hovertemplate=(
                "<b>Decile %{x}</b><br>"
                "Notional-weighted effective leverage: %{y:.2f}x<br>"
                "Decile range: %{customdata[0]:.2f}x–%{customdata[2]:.2f}x<br>"
                "Decile median: %{customdata[1]:.2f}x<br>"
                "Ratio of totals: %{customdata[6]:.2f}x<br>"
                "Share of platform notional: %{customdata[3]:.2f}%<br>"
                "Gross notional: $%{customdata[4]:,.0f}<br>"
                "Wallet/DEX observations: %{customdata[5]:,}"
                "<extra></extra>"
            ),
        )

    def date_trace(snapshot_date: str) -> Any:
        return go.Scatter(
            x=[1.15],
            y=[y_max * 0.91],
            text=[snapshot_date],
            mode="text",
            textfont={"size": 28, "color": "rgba(11, 60, 93, 0.72)"},
            hoverinfo="skip",
            showlegend=False,
        )

    frames = [
        go.Frame(
            name=snapshot_date,
            data=[wave_trace(snapshot_date), date_trace(snapshot_date)],
            traces=[0, 1],
        )
        for snapshot_date in dates
    ]
    figure = go.Figure(
        data=[wave_trace(dates[0]), date_trace(dates[0])],
        frames=frames,
    )
    slider_steps = [
        {
            "args": [
                [snapshot_date],
                {
                    "frame": {"duration": 0, "redraw": True},
                    "mode": "immediate",
                    "transition": {"duration": 0},
                },
            ],
            "label": snapshot_date,
            "method": "animate",
        }
        for snapshot_date in dates
    ]
    figure.update_layout(
        title={
            "text": (
                "The Effective-Leverage Wave"
                f"<br><sup>Notional-weighted leverage across equal-count wallet/DEX deciles; "
                f"{dates[0]} to {dates[-1]}</sup>"
            ),
            "x": 0.01,
        },
        template="plotly_white",
        height=720,
        showlegend=False,
        hovermode="closest",
        margin={"l": 85, "r": 40, "t": 115, "b": 145},
        xaxis={
            "title": "Effective-leverage decile (lowest → highest)",
            "tickmode": "linear",
            "tick0": 1,
            "dtick": 1,
            "range": [0.75, 10.25],
            "fixedrange": True,
            "showgrid": False,
        },
        yaxis={
            "title": "Notional-weighted effective leverage (x)",
            "ticksuffix": "x",
            "range": [0, y_max],
            "fixedrange": True,
            "gridcolor": "rgba(0, 0, 0, 0.12)",
        },
        updatemenus=[
            {
                "type": "buttons",
                "direction": "left",
                "x": 0,
                "y": -0.17,
                "xanchor": "left",
                "yanchor": "top",
                "buttons": [
                    {
                        "label": "▶ Play",
                        "method": "animate",
                        "args": [
                            None,
                            {
                                "frame": {"duration": 80, "redraw": True},
                                "fromcurrent": True,
                                "transition": {"duration": 45, "easing": "cubic-in-out"},
                            },
                        ],
                    },
                    {
                        "label": "❚❚ Pause",
                        "method": "animate",
                        "args": [
                            [None],
                            {
                                "frame": {"duration": 0, "redraw": False},
                                "mode": "immediate",
                                "transition": {"duration": 0},
                            },
                        ],
                    },
                ],
            }
        ],
        sliders=[
            {
                "active": 0,
                "currentvalue": {"prefix": "Snapshot: ", "font": {"size": 15}},
                "pad": {"t": 45, "b": 5},
                "steps": slider_steps,
                "transition": {"duration": 0},
            }
        ],
        annotations=[
            {
                "text": "Marker size reflects each decile’s share of platform gross notional.",
                "xref": "paper",
                "yref": "paper",
                "x": 1,
                "y": -0.12,
                "xanchor": "right",
                "showarrow": False,
                "font": {"size": 12, "color": "#555"},
            }
        ],
    )
    return figure


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_date, default=EARLIEST_SNAPSHOT_DATE)
    parser.add_argument("--end", type=parse_date, default=datetime.now(UTC).date())
    parser.add_argument(
        "--cache-dir", type=Path, default=Path(__file__).parent / "data" / "reservoir"
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path(__file__).parent / "output"
    )
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    options = build_parser().parse_args(arguments)
    if options.start > options.end:
        raise SystemExit("--start must not be after --end")
    paths = cached_snapshot_paths(options.cache_dir, options.start, options.end)
    if not paths:
        raise SystemExit("no cached position snapshots found")

    try:
        import duckdb
    except ImportError:
        raise SystemExit("duckdb is required; install requirements.txt") from None

    summaries: list[dict[str, Any]] = []
    deciles: list[dict[str, Any]] = []
    connection = duckdb.connect()
    try:
        connection.execute("SET threads TO 4")
        for number, path in enumerate(paths, 1):
            print(f"Calculating effective leverage {number}/{len(paths)} ({path.parent.name})")
            daily_summaries, daily_deciles = calculate_snapshot(connection, path)
            summaries.extend(daily_summaries)
            deciles.extend(daily_deciles)
    finally:
        connection.close()

    summary_path = options.output_dir / "daily_effective_leverage.csv"
    decile_path = options.output_dir / "daily_effective_leverage_deciles.csv"
    chart_path = options.output_dir / "platform_decile_notional_weighted_effective_leverage.html"
    wave_path = options.output_dir / "platform_decile_leverage_wave.html"
    write_rows(summary_path, SUMMARY_FIELDS, summaries)
    write_rows(decile_path, DECILE_FIELDS, deciles)
    from leverage_decomposition import write_html

    write_html(chart_path, build_decile_figure(deciles))
    write_html(wave_path, build_decile_wave_figure(deciles))
    print(f"Wrote {summary_path}")
    print(f"Wrote {decile_path}")
    print(f"Wrote {chart_path}")
    print(f"Wrote {wave_path}")
    print(f"Coverage: {paths[0].parent.name[5:]} through {paths[-1].parent.name[5:]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
