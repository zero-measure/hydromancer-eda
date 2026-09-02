# hydromancer-eda

Real-time wallet toxic-flow classification from Hydromancer executions, L2 book
diffs, L4 order lifecycle updates, and market-wide leverage changes.

## Data streams

The service runs four independently checkpointed WebSocket consumers:

- `allFills`: participant executions used for wallet outcomes and trade behavior.
- `l2BookDiff`: one atomic per-coin book event per changed block.
- `l4BookUpdates`: wallet-attributed order lifecycle behavior.
- `allLeverageUpdates`: leverage settings plus isolated-margin and top-up changes.

Every connection responds to application `ping` messages with `pong`. Session IDs
and opaque cursors are stored only in `ingestion_checkpoints`; token-bearing
WebSocket URLs are never logged. Replay/live overlap is deduplicated before
lifetime aggregates change.

The L2 client subscribes before fetching `l2BookDiffSnapshot`, aligns buffered
diffs on height, and re-bootstraps on a per-coin sequence gap, epoch change, or
resync signal. The L4 snapshot seeds wallet attribution for orders already
resting when the process starts.

## Classifier data

`orderflow_events` retains only execution fields relevant to classification.
Liquidation is true only when the participant wallet equals
`liquidation.liquidatedUser`; a liquidation counterparty is not mislabeled.

One compact L2 row is stored after each complete per-coin diff. Raw retention is:

- compact L2 ticks: 7 days
- raw L4 lifecycle events: 1 day
- fills, matured markout samples, wallet order aggregates, and leverage: indefinite

The current L2 reconstruction (`l2_book_state` and `l2_book_levels`) is also
stored durably, so a process restart can apply replay to the exact prior book
instead of pretending a fresh snapshot preserves missed event counts.

Event-time markouts use future per-coin L2 event numbers rather than wall-clock
book speed:

| Horizon | Maximum elapsed time |
|---:|---:|
| 50 book events | 5 minutes |
| 100 book events | 30 minutes |
| 1,000 book events | 60 minutes |

For a buy, raw markout is `ln(future_mid / fill_price)`; for a sell the ratio is
reversed. The normalized outcome is:

```text
signed_log_markout / (trailing_symbol_event_volatility * sqrt(event_horizon))
```

The volatility estimate is stored on the latest L2 tick available before the
fill, so no post-fill volatility enters the normalization. Samples crossing an
L2 epoch or exceeding the elapsed-time bound are excluded. Matured samples are
retained after source L2 ticks expire. Wallet labels store p10 through p90 and
sample count for each horizon in `markout_metrics`.

## Prerequisites

- Python 3.11+
- A Hydromancer API key with access to the four configured streams plus
  `l2BookDiffSnapshot` and `l4Book` REST snapshots
- PostgreSQL 16 with TimescaleDB
- Docker for the recommended local setup

## Local TimescaleDB

```bash
docker run --name hydromancer-timescaledb \
  --detach \
  --publish 5432:5432 \
  --env POSTGRES_USER=postgres \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=hydromancer \
  --volume hydromancer-timescaledb-data:/var/lib/postgresql/data \
  timescale/timescaledb:latest-pg16
```

Verify readiness:

```bash
docker exec hydromancer-timescaledb \
  pg_isready --username postgres --dbname hydromancer
```

## Installation and configuration

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

For the Jupyter debugging environment, install and register the project kernel:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[notebooks]'
python -m ipykernel install --user \
  --name hydromancer-eda \
  --display-name 'Hydromancer EDA (.venv)'
jupyter lab notebooks/debug_hydromancer_snapshot_403.ipynb
```

The notebook is configured to select `Hydromancer EDA (.venv)` automatically.

Set at least:

```dotenv
HYDROMANCER_API_KEY=replace-with-your-api-key
HYDROMANCER_COINS=xyz:KORU
PG_DSN=postgresql://postgres:postgres@localhost:5432/hydromancer
```

Coin names are case-sensitive. Do not commit `.env`.

## Required one-time analytical reset

Schema version 2 intentionally does not migrate old analytical rows. Stop
running ingestion, then explicitly remove all analytical history:

```bash
docker exec -i hydromancer-timescaledb \
  psql -U postgres -d hydromancer \
  < sql/reset_analytics.sql
```

This drops fills, books, leverage, markout samples, order aggregates, and labels.
It preserves operational checkpoints and rejects. Startup fails with a reset
instruction when it detects the obsolete schema; `schema.sql` never contains an
unconditional `DROP` or `TRUNCATE`.

## Run ingestion

```bash
PYTHONPATH=src python -m hydromancer_eda.main
```

Inspect live state without exposing sessions or cursors:

```bash
docker exec -i hydromancer-timescaledb \
  psql -U postgres -d hydromancer \
  < sql/inspect_live.sql
```

Inspect classifier labels:

```bash
docker exec -i hydromancer-timescaledb \
  psql -U postgres -d hydromancer \
  < sql/inspect_labels.sql
```

## Verification

```bash
ruff check .
mypy src
pytest -q
```

Optional PostgreSQL numerical markout test:

```bash
TEST_PG_DSN="$PG_DSN" pytest -q tests/test_markout_postgres.py
```
