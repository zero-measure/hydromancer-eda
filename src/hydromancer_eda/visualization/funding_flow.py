"""Deterministic Plotly chart and CSV export for funding versus position flow."""
from __future__ import annotations

import csv
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from hydromancer_eda.analysis.position_flow import HourlyPositionFlow
from hydromancer_eda.models import FundingObservation


def build_funding_flow_figure(
    coins: Sequence[str],
    flows: Sequence[HourlyPositionFlow],
    funding: Sequence[FundingObservation],
):  # type: ignore[no-untyped-def]
    """Build one subplot per coin with bars and exact-time secondary-axis funding."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    figure = make_subplots(
        rows=len(coins), cols=1, shared_xaxes=True,
        specs=[[{"secondary_y": True}] for _ in coins],
        subplot_titles=list(coins), vertical_spacing=min(0.08, 0.3 / max(len(coins), 1)),
    )
    for row_number, coin in enumerate(coins, 1):
        coin_flows = [item for item in flows if item.coin == coin]
        x = [item.hour for item in coin_flows]
        custom = [
            ["missing" if item.total_notional is None else str(item.total_notional),
             "missing" if item.classified_notional is None else str(item.classified_notional),
             "missing" if item.unclassified_notional is None else str(item.unclassified_notional),
             "missing" if item.coverage is None else f"{item.coverage:.2%}",
             f"{item.interval_coverage:.2%}",
             "yes" if item.known_source_gap else "no"]
            for item in coin_flows
        ]
        hover = (
            "%{x|%Y-%m-%d %H:%M:%S UTC}<br>%{fullData.name}: %{y}<br>"
            "total=%{customdata[0]}<br>classified=%{customdata[1]}<br>"
            "unclassified=%{customdata[2]}<br>classification coverage=%{customdata[3]}<br>"
            "interval coverage=%{customdata[4]}<br>known source gap=%{customdata[5]}"
            "<extra></extra>"
        )
        figure.add_trace(
            go.Bar(name=f"{coin} net long", x=x,
                   y=[None if item.net_long_notional is None else float(item.net_long_notional) for item in coin_flows],
                   customdata=custom, hovertemplate=hover, marker_color="#238636",
                   showlegend=row_number == 1),
            row=row_number, col=1, secondary_y=False,
        )
        short_custom = [
            [*details, "missing" if item.net_short_notional is None else str(item.net_short_notional)]
            for details, item in zip(custom, coin_flows, strict=True)
        ]
        short_hover = (
            "%{x|%Y-%m-%d %H:%M:%S UTC}<br>%{fullData.name}: %{customdata[6]}<br>"
            "display value=%{y}<br>total=%{customdata[0]}<br>"
            "classified=%{customdata[1]}<br>unclassified=%{customdata[2]}<br>"
            "classification coverage=%{customdata[3]}<br>"
            "interval coverage=%{customdata[4]}<br>known source gap=%{customdata[5]}"
            "<extra></extra>"
        )
        figure.add_trace(
            go.Bar(name=f"{coin} net short", x=x,
                   y=[None if item.net_short_notional is None else -float(item.net_short_notional) for item in coin_flows],
                   customdata=short_custom, hovertemplate=short_hover, marker_color="#da3633",
                   showlegend=row_number == 1),
            row=row_number, col=1, secondary_y=False,
        )
        coin_funding = sorted((item for item in funding if item.coin == coin), key=lambda item: item.ts)
        figure.add_trace(
            go.Scatter(
                name=f"{coin} funding", mode="lines+markers",
                x=[item.ts for item in coin_funding], y=[float(item.rate) for item in coin_funding],
                customdata=[[item.ts.isoformat(), item.source] for item in coin_funding],
                hovertemplate=("exact timestamp=%{customdata[0]}<br>funding=%{y}<br>"
                               "source=%{customdata[1]}<extra></extra>"),
                connectgaps=False, marker={"size": 6}, line={"color": "#8957e5"},
                showlegend=row_number == 1,
            ),
            row=row_number, col=1, secondary_y=True,
        )
        figure.update_yaxes(title_text="USD notional", row=row_number, col=1, secondary_y=False)
        figure.update_yaxes(title_text="funding rate", row=row_number, col=1, secondary_y=True)
    figure.update_layout(
        title="Hourly taker position flow vs exact-timestamp funding",
        barmode="overlay", template="plotly_white", hovermode="x unified",
        height=360 * len(coins), legend={"orientation": "h"},
    )
    return figure


def write_analysis_html(path: str | Path, figure: Any) -> None:
    """Write self-contained Plotly HTML with a stable element id."""
    import plotly.io as pio

    pio.write_html(
        figure,
        file=path,
        include_plotlyjs=True,
        full_html=True,
        auto_open=False,
        div_id="hydromancer-funding-flow",
    )


def write_analysis_csv(
    path: str | Path,
    flows: Sequence[HourlyPositionFlow],
    funding: Sequence[FundingObservation],
) -> None:
    """Write typed flow/funding rows; funding_timestamp always remains exact."""
    fields = [
        "coin", "hour_utc", "funding_timestamp_utc", "funding_rate",
        "funding_source", "net_long_notional", "net_short_notional", "long_open_notional",
        "long_close_notional", "short_open_notional", "short_close_notional",
        "total_notional", "classified_notional", "unclassified_notional", "coverage",
        "interval_coverage", "known_source_gap",
    ]
    funding_by_hour: dict[tuple[str, datetime], FundingObservation] = {}
    for observation in sorted(funding, key=lambda value: (value.coin, value.ts)):
        hour = observation.ts.replace(minute=0, second=0, microsecond=0)
        funding_by_hour[(observation.coin, hour)] = observation
    with Path(path).open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for item in flows:
            funding_observation = funding_by_hour.get((item.coin, item.hour))
            writer.writerow({
                "coin": item.coin,
                "hour_utc": item.hour.isoformat(),
                "funding_timestamp_utc": (
                    funding_observation.ts.isoformat()
                    if funding_observation is not None
                    else None
                ),
                "funding_rate": (
                    funding_observation.rate if funding_observation is not None else None
                ),
                "funding_source": (
                    funding_observation.source if funding_observation is not None else None
                ),
                "net_long_notional": item.net_long_notional,
                "net_short_notional": item.net_short_notional,
                "long_open_notional": item.long_open_notional,
                "long_close_notional": item.long_close_notional,
                "short_open_notional": item.short_open_notional,
                "short_close_notional": item.short_close_notional,
                "total_notional": item.total_notional,
                "classified_notional": item.classified_notional,
                "unclassified_notional": item.unclassified_notional,
                "coverage": item.coverage,
                "interval_coverage": item.interval_coverage,
                "known_source_gap": item.known_source_gap,
            })
