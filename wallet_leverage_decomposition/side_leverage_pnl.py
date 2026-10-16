"""Daily long/short notional, allocated effective leverage and open-position PnL.

Uses cached Reservoir perp position snapshots. PnL is a *snapshot stock* for
currently open positions, NOT daily realized trading PnL: unrealized mark-to-entry
PnL plus the snapshot's cumulative funding_pnl. Fees, closed positions, and
realized PnL are not available in these files.
"""
from __future__ import annotations

import argparse
import csv
import math
from collections.abc import Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from leverage_decomposition import (
    EARLIEST_SNAPSHOT_DATE,
    _timestamp_from_path,
    cached_snapshot_paths,
    parse_date,
    write_html,
)

FIELDS = (
    "snapshot_date", "timestamp", "scope", "dex", "side",
    "position_count", "wallet_dex_count", "distinct_wallet_count",
    "gross_notional_usd", "eligible_gross_notional_usd", "allocated_account_value",
    "aggregate_effective_leverage", "notional_weighted_effective_leverage",
    "excluded_nonpositive_account_count",
    "unrealized_pnl_usd", "cumulative_funding_pnl_usd", "net_open_pnl_usd",
    "pnl_position_count", "excluded_pnl_position_count",
)

# Position notional is unsigned in Reservoir; the sign of size determines side.
# Allocation is done *within each (user, DEX)*, before summing across wallets.
SQL = """
WITH positions AS (
    SELECT
        user,
        CASE WHEN contains(market, ':') THEN split_part(market, ':', 1)
             ELSE 'hyperliquid' END AS dex,
        CASE WHEN size > 0 THEN 'long' ELSE 'short' END AS side,
        abs(notional) AS gross_notional,
        account_value,
        CASE WHEN entry_price IS NOT NULL AND isfinite(entry_price)
                  AND funding_pnl IS NOT NULL AND isfinite(funding_pnl)
             THEN size * (abs(notional) / abs(size) - entry_price)
        END AS unrealized_pnl,
        CASE WHEN entry_price IS NOT NULL AND isfinite(entry_price)
                  AND funding_pnl IS NOT NULL AND isfinite(funding_pnl)
             THEN funding_pnl
        END AS valid_funding_pnl
    FROM read_parquet(?)
    WHERE size IS NOT NULL AND isfinite(size) AND size != 0
      AND notional IS NOT NULL AND isfinite(notional) AND abs(notional) > 0
),
wallet_dex AS (
    SELECT user, dex, max(account_value) AS account_value,
           sum(gross_notional) AS wallet_gross
    FROM positions
    GROUP BY user, dex
),
wallet_side AS (
    SELECT p.user, p.dex, p.side,
           count(*) AS position_count,
           sum(p.gross_notional) AS gross_notional,
           w.account_value, w.wallet_gross,
           sum(p.unrealized_pnl) AS unrealized_pnl,
           sum(p.valid_funding_pnl) AS funding_pnl,
           count(p.unrealized_pnl) AS pnl_position_count
    FROM positions p
    JOIN wallet_dex w USING (user, dex)
    GROUP BY p.user, p.dex, p.side, w.account_value, w.wallet_gross
),
allocated AS (
    SELECT *,
           coalesce(account_value > 0 AND isfinite(account_value), false) AS valid_account,
           account_value * gross_notional / wallet_gross AS allocated_equity
    FROM wallet_side
),
expanded AS (
    SELECT 'platform' AS scope, 'ALL' AS scope_dex, * FROM allocated
    UNION ALL
    SELECT 'dex' AS scope, dex AS scope_dex, * FROM allocated
)
SELECT scope, scope_dex, side,
       sum(position_count) AS position_count,
       count(*) AS wallet_dex_count,
       count(DISTINCT user) AS distinct_wallet_count,
       sum(gross_notional) AS gross_notional,
       sum(gross_notional) FILTER (WHERE valid_account) AS eligible_gross,
       sum(allocated_equity) FILTER (WHERE valid_account) AS allocated_equity,
       sum((gross_notional / allocated_equity) * gross_notional)
           FILTER (WHERE valid_account)
           / nullif(sum(gross_notional) FILTER (WHERE valid_account), 0)
           AS weighted_leverage,
       count(*) FILTER (WHERE NOT valid_account) AS excluded_account_count,
       sum(unrealized_pnl) AS unrealized_pnl,
       sum(funding_pnl) AS funding_pnl,
       sum(unrealized_pnl + funding_pnl) AS net_open_pnl,
       sum(pnl_position_count) AS pnl_position_count
FROM expanded
GROUP BY scope, scope_dex, side
ORDER BY CASE WHEN scope = 'platform' THEN 0 ELSE 1 END, scope_dex, side
"""


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non-finite value in side leverage/PnL output")
    return result


def calculate_snapshot(connection: Any, path: Path) -> list[dict[str, Any]]:
    """Return platform and DEX side metrics for a single daily snapshot."""
    snapshot_date = date.fromisoformat(path.parent.name.removeprefix("date="))
    timestamp = _timestamp_from_path(path).isoformat()
    # Check the repeated account value before aggregating across sides.
    inconsistent = connection.execute(
        """SELECT user, CASE WHEN contains(market, ':') THEN split_part(market, ':', 1)
                         ELSE 'hyperliquid' END AS dex
           FROM read_parquet(?)
           WHERE size IS NOT NULL AND isfinite(size) AND size != 0
             AND notional IS NOT NULL AND isfinite(notional) AND abs(notional) > 0
           GROUP BY user, dex HAVING count(DISTINCT account_value) > 1 LIMIT 1""",
        [str(path)],
    ).fetchone()
    if inconsistent is not None:
        raise ValueError(f"account_value is inconsistent within a user/DEX in snapshot {path}")

    rows: list[dict[str, Any]] = []
    for record in connection.execute(SQL, [str(path)]).fetchall():
        (scope, dex, side, positions, wallets, users, gross, eligible, equity,
         weighted_leverage, excluded, unrealized, funding, net, pnl_positions) = record
        equity_value = _optional_float(equity)
        eligible_value = _optional_float(eligible)
        rows.append({
            "snapshot_date": snapshot_date.isoformat(),
            "timestamp": timestamp,
            "scope": scope,
            "dex": dex,
            "side": side,
            "position_count": int(positions),
            "wallet_dex_count": int(wallets),
            "distinct_wallet_count": int(users),
            "gross_notional_usd": _optional_float(gross),
            "eligible_gross_notional_usd": eligible_value,
            "allocated_account_value": equity_value,
            "aggregate_effective_leverage": (
                eligible_value / equity_value
                if eligible_value is not None and equity_value is not None and equity_value > 0
                else None
            ),
            "notional_weighted_effective_leverage": _optional_float(weighted_leverage),
            "excluded_nonpositive_account_count": int(excluded),
            "unrealized_pnl_usd": _optional_float(unrealized),
            "cumulative_funding_pnl_usd": _optional_float(funding),
            "net_open_pnl_usd": _optional_float(net),
            "pnl_position_count": int(pnl_positions),
            "excluded_pnl_position_count": int(positions) - int(pnl_positions),
        })
    return rows


def build_figure(rows: Sequence[dict[str, Any]]) -> Any:
    """Three-panel platform time series, leaving missing snapshots as gaps."""
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    platform = [row for row in rows if row["scope"] == "platform"]
    if not platform:
        raise ValueError("no platform snapshots to plot")
    by_side = {
        side: {str(row["snapshot_date"]): row for row in platform if row["side"] == side}
        for side in ("long", "short")
    }
    start = date.fromisoformat(min(str(row["snapshot_date"]) for row in platform))
    end = date.fromisoformat(max(str(row["snapshot_date"]) for row in platform))
    # A daily calendar plus connectgaps=False makes missing snapshot dates visible.
    from datetime import timedelta

    days = [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]
    figure = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.08,
        subplot_titles=("Gross open notional (USD billions)",
                        "Notional-weighted effective leverage by side",
                        "Net open-position PnL (USD millions)"),
    )
    metrics = (("gross_notional_usd", 1e9), ("notional_weighted_effective_leverage", 1),
               ("net_open_pnl_usd", 1e6))
    for side, color in (("long", "#145DA0"), ("short", "#F3722C")):
        for panel, (field, divisor) in enumerate(metrics, 1):
            series = by_side[side]
            figure.add_trace(
                go.Scatter(
                    x=days,
                    y=[(float(series[day][field]) / divisor
                        if day in series and series[day][field] is not None else None)
                       for day in days],
                    name=side.title(), legendgroup=side, showlegend=panel == 2,
                    mode="lines", line={"color": color, "width": 2}, connectgaps=False,
                    hovertemplate="%{x}: %{y:,.3f}<extra>" + side.title() + "</extra>",
                ), row=panel, col=1,
            )
    figure.add_hline(y=0, row=3, col=1, line_color="gray", line_width=1)
    figure.update_layout(
        title="Long vs short positioning, allocated leverage & open PnL",
        height=1000, template="plotly_white", hovermode="x unified",
        legend={"orientation": "h", "x": 1, "xanchor": "right", "y": 0.65},
        margin={"l": 75, "r": 35, "t": 105, "b": 105},
        annotations=[
            {"text": "PnL = unrealized mark-to-entry + cumulative funding on currently open positions; "
                     "not realized/daily trading PnL. Mixed-side wallet/DEX equity allocated by gross notional.",
             "xref": "paper", "yref": "paper", "x": 0, "y": -0.08,
             "xanchor": "left", "showarrow": False, "font": {"size": 11}},
        ],
    )
    for panel in (1, 2, 3):
        figure.update_xaxes(
            title_text="Snapshot date (UTC)",
            showticklabels=True,
            tickformat="%b %Y",
            row=panel, col=1,
        )
    figure.update_yaxes(title_text="Notional (USD billions)", row=1, col=1)
    figure.update_yaxes(title_text="Notional-weighted effective leverage (×)", row=2, col=1)
    figure.update_yaxes(title_text="Net open PnL (USD millions)", row=3, col=1)
    figure.update_xaxes(rangeslider={"visible": True}, row=3, col=1)
    return figure


def main(arguments: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_date, default=EARLIEST_SNAPSHOT_DATE)
    parser.add_argument("--end", type=parse_date, default=datetime.now(UTC).date())
    parser.add_argument("--cache-dir", type=Path, default=Path(__file__).parent / "data" / "reservoir")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "output")
    options = parser.parse_args(arguments)
    if options.start > options.end:
        parser.error("--start must not be after --end")
    paths = cached_snapshot_paths(options.cache_dir, options.start, options.end)
    if not paths:
        parser.error("no cached position snapshots found")
    import duckdb

    connection = duckdb.connect()
    rows: list[dict[str, Any]] = []
    try:
        connection.execute("SET threads TO 4")
        for number, path in enumerate(paths, 1):
            print(f"Calculating side metrics {number}/{len(paths)} ({path.parent.name})", flush=True)
            rows.extend(calculate_snapshot(connection, path))
    finally:
        connection.close()
    options.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = options.output_dir / "daily_side_leverage_pnl.csv"
    html_path = options.output_dir / "daily_side_leverage_pnl.html"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    write_html(html_path, build_figure(rows))
    print(f"Wrote {csv_path}\nWrote {html_path}")
    print(f"Coverage: {paths[0].parent.name[5:]} through {paths[-1].parent.name[5:]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
