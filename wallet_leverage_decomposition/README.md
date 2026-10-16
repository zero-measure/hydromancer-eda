# Hyperliquid wallet leverage decomposition

This folder builds a daily, platform-wide view of **gross open perpetual-position
notional by configured leverage**. It produces:

- `output/daily_leverage_decomposition.html` — a self-contained Plotly report with
  an absolute USD-millions stacked bar chart and a normalized 100% stacked chart.
- `output/daily_leverage_decomposition.csv` — the chart data, including bucket and
  platform totals, position counts, distinct wallet counts, source, and exact
  snapshot timestamp.

The buckets are mutually exclusive: `1x`, `2x`, …, `9x`, `10x–25x`, and `>25x`.
Every available day reconciles to 100% of gross notional.

## Data coverage and sources

Historical data comes from the free Hydromancer Reservoir requester-pays S3
bucket:

```text
s3://hydromancer-reservoir/global/snapshots/perp/all/date=YYYY-MM-DD/
```

The global path includes native Hyperliquid perpetuals and HIP-3 DEX positions.
Reservoir position snapshots are available from **August 2025 onward**, are taken
from end-of-day ABCI state, and are updated daily. Some dates are legitimately
missing. In particular, Hydromancer documents a known gap from late October to
mid-December 2025. The analysis leaves missing dates missing; it never creates
zero-notional bars. If multiple objects exist for one UTC date, the object with
the latest embedded state timestamp is selected.

`--include-current-api` optionally adds the latest Hydromancer `perpSnapshot`
across `ALL:ALL_DEXES`. That endpoint is a separately purchased add-on and is
polled through `perpSnapshotTimestamp` before the heavy download. If an API point
and Reservoir point share a date, only the later exact timestamp is retained.
This API option extends the series to the present; it does not manufacture
historical observations.

## Metric definition

For each open position row:

```text
gross notional = abs(notional)
bucket = configured leverage multiplier
```

Notional is intentionally gross. Long and short positions must not cancel.
“Leverage” is the user's configured position leverage supplied by Hydromancer,
not an inferred wallet-wide ratio such as gross exposure/account equity. A wallet
with positions in multiple buckets is counted once in each relevant bucket, so
bucket wallet counts are not additive. `total_wallet_count` is distinct across the
whole snapshot.

## Prerequisites

- Python 3.11+
- AWS CLI with AWS credentials and permission to pay requester-pays S3 charges
- The Python packages in `requirements.txt`
- Optional: `HYDROMANCER_API_KEY` with `perpSnapshot` access

From this folder (or the repository virtual environment):

```bash
python -m pip install -r wallet_leverage_decomposition/requirements.txt
```

Reservoir itself requires no Hydromancer API key, but AWS requires a billed
identity because the bucket is requester pays.

## Run the full history

From the repository root:

```bash
python wallet_leverage_decomposition/leverage_decomposition.py --download
```

The first run lists Reservoir once, downloads the latest snapshot object for each
available date, caches Parquet under `data/reservoir/`, aggregates with DuckDB,
and writes the CSV and HTML report. Later runs can reuse the cache:

```bash
python wallet_leverage_decomposition/leverage_decomposition.py
```

Select an inclusive UTC date range:

```bash
python wallet_leverage_decomposition/leverage_decomposition.py \
  --download --start 2025-08-01 --end 2026-03-31
```

Append the current API snapshot:

```bash
export HYDROMANCER_API_KEY='...'
python wallet_leverage_decomposition/leverage_decomposition.py \
  --download --include-current-api
```

The API token is sent only in the authorization header and is never printed.
Custom cache/output paths are available through `--cache-dir` and `--output-dir`.

## Output interpretation

The upper panel is the requested time-varying stacked bar chart in USD millions.
Its full stack is all gross open-position notional in that snapshot. The lower
panel displays the same data normalized to exactly 100%. Hovering a segment shows
its notional, share, position count, distinct bucket wallets, platform totals,
and source.

The CSV's `snapshot_date` is the S3 partition date while `timestamp` is the exact
state/API timestamp in UTC. Use `timestamp`, not an assumed midnight, when joining
to other datasets.

## Account-equity-based effective leverage

The position snapshots also carry account value. After caching the position
snapshots, generate effective-leverage summaries without another download:

```bash
python wallet_leverage_decomposition/effective_leverage.py
```

This writes:

- `output/daily_effective_leverage.csv` — daily platform and per-DEX ratio of
  total gross notional to total account value, wallet/DEX percentiles from p10
  through p99, and the gross-notional-weighted mean effective leverage.
- `output/daily_effective_leverage_deciles.csv` — ten equal-count bins per day
  for both the platform and each DEX, including leverage bounds, median, gross
  notional, account value, ratio of totals, weighted mean, and each decile's
  share of notional and account value.
- `output/platform_decile_notional_weighted_effective_leverage.html` — an
  interactive full-history time series of gross-notional-weighted effective
  leverage for all ten platform deciles, with hover details, date controls,
  linear/log scale buttons, explicit breaks for missing snapshots, and a range
  slider.
- `output/platform_decile_leverage_wave.html` — a GIF-like animated distribution
  across deciles. Wave height is each decile's notional-weighted effective
  leverage, marker size is its share of platform notional, and play/pause plus a
  draggable snapshot timeline control the animation.

The unit of observation is `(user, dex)`, as account value is DEX/collateral
specific. Account value repeated across a user's position rows is deduplicated
before any sums. Observations with non-positive or non-finite account value
cannot have a meaningful leverage ratio and are excluded, with their count
reported explicitly.

The platform rows sum account values across collateral tokens. Native
Hyperliquid (`scope=dex,dex=hyperliquid`) is directly USD-comparable because it
uses USDC. HIP-3 account values are denominated in each DEX's collateral token;
therefore, all-DEX platform ratios assume those collateral tokens are at USD
parity. For a strict all-DEX USD ratio, convert each account value at the exact
snapshot timestamp before combining DEXes.

## Long/short notional, effective leverage, and open-position PnL

With the cached snapshots, run:

```bash
python wallet_leverage_decomposition/side_leverage_pnl.py
```

This writes `output/daily_side_leverage_pnl.csv` (platform and per-DEX rows for
both sides) and `output/daily_side_leverage_pnl.html` (three-panel interactive
platform time series). `size > 0` is long and `size < 0` is short; `notional`
is unsigned. Gross notional includes every valid open position, even when its
wallet has nonpositive equity. Leverage only uses positioned `(user, DEX)`
accounts with positive finite equity. The CSV contains both the **aggregate
ratio** (eligible side notional / allocated side equity) and **notional-weighted
effective leverage** (gross-notional-weighted mean of each positioned wallet/DEX
side's notional / allocated side equity). The chart's middle panel shows the
notional-weighted measure. A mixed-side account's equity is split between its long and short
positions in proportion to its gross notional on each side. This is a *modelled
allocation*, not independent long/short margin observed on-chain. Because the
allocation is proportional, an individual mixed-side account's long and short
leverage are the same before aggregation. Account value is deduplicated within
`(user, DEX)`; platform totals assume HIP-3 collateral tokens are USD-equivalent
(see the effective-leverage caveat above).

PnL is a **stock at each snapshot**, not daily PnL earned between dates:
`size * (abs(notional)/abs(size) - entry_price) + funding_pnl` per open
position. The first term is mark-to-entry unrealized PnL, and `funding_pnl` is
cumulative funding PnL. Net open PnL sums both components per side. This omits
realized PnL from closed positions and trading fees; changes across snapshots
also reflect opens, closes, and funding resets, so **do not interpret daily
differences as trading returns**. Positions with missing/non-finite entry price
or funding are excluded from both PnL components and counted in
`excluded_pnl_position_count` (but still count toward gross notional).

## Reproducibility and caveats

- Reservoir is the furthest-back complete all-wallet position source documented by
  Hydromancer for this metric. Fill history cannot reconstruct configured leverage
  reliably and is not substituted for missing state snapshots.
- Open interest is often quoted one-sided. This report instead sums every wallet's
  absolute position notional, so its platform total is approximately twice
  one-sided open interest in a balanced market.
- Snapshot rows are positions, not unique wallets. A wallet can hold many markets.
- Daily state snapshots do not show intraday leverage changes.
- S3 requester-pays transfer/request charges accrue to the configured AWS account.

## Test

```bash
pytest -q wallet_leverage_decomposition/test_leverage_decomposition.py
```
