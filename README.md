# hydromancer-eda

Real-time wallet toxic-flow classification from Hydromancer executions, L2 book
diffs, L4 order lifecycle updates, and market-wide leverage changes.

## Data streams

The service runs four independently checkpointed WebSocket consumers:

- `allFills`: stream all fills. Each fill contains an address and fill details. Fills are batched per block.
- `l2BookDiff`: all changed levels for your subscribed coins in a single block. A message is sent only for blocks that change something you're subscribed to — quiet blocks are skipped.
- `l4BookUpdates`: updates streamed per-block with order changes for subscribed markets.
- `allLeverageUpdates`: updates to leverage settings plus isolated-margin and top-up changes.

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

### Stop and restart TimescaleDB safely

Give PostgreSQL up to 30 seconds to checkpoint and shut down cleanly instead of
force-killing the Docker VM:

```bash
docker stop --time 30 hydromancer-timescaledb
```

Confirm that the container stopped normally. A clean shutdown reports
`status=exited`, `running=false`, and `exit_code=0`:

```bash
docker inspect hydromancer-timescaledb \
  --format 'status={{.State.Status}} running={{.State.Running}} exit_code={{.State.ExitCode}}'
```

If Docker Desktop is no longer needed, quit it after the container has stopped
to release the memory retained by its virtual machine:

```bash
osascript -e 'tell application "Docker" to quit'
```

Start Docker Desktop and the existing container again with:

```bash
open -a Docker
# Wait until Docker Desktop is ready.
docker start hydromancer-timescaledb
```

The container uses the `hydromancer-timescaledb-data` volume, so stopping the
container or quitting Docker Desktop does not delete the database.

## Installation and configuration

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

For optional Jupyter and Plotly exploration, install and register the project kernel:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[notebooks]'
python -m ipykernel install --user \
  --name hydromancer-eda \
  --display-name 'Hydromancer EDA (.venv)'
```

The L2/L4 ingestion wire formats and wallet-monitoring feature boundaries are
documented in `src/hydromancer_eda/ingestion/orderbook/README.md`.

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

On reconnect, each channel logs when cursor-based backfill starts, a chunk progress
bar with persisted-item and buffered-live counts, and a completion message. While
replay is active it also emits periodic status. If replay stops advancing for 30
seconds, the connection is closed and retried from the last durable cursor; retry
attempts include their backoff delay. Tune this behavior with:

```dotenv
HYDROMANCER_REPLAY_STATUS_INTERVAL_SECONDS=5
HYDROMANCER_REPLAY_STALL_TIMEOUT_SECONDS=30
```

A `hasGap=true` replay is persisted but reported as an error because no
market-wide REST endpoint can repair the missing interval.

### Monitor the live L4 order book

The wallet-attributed order-book display runs as a Jupyter `FigureWidget`. Start
it from the repository root with:

```bash
PYTHONPATH=src .venv/bin/jupyter lab
```

In a notebook using the `Hydromancer EDA (.venv)` kernel, run:

```python
import asyncio

from hydromancer_eda.config import get_settings
from hydromancer_eda.ingestion.marketdata import fetch_l4_book_snapshot
from hydromancer_eda.visualization.l4 import animate_l4_book, stream_l4_book_updates

settings = get_settings()
coin = settings.coins[0]

# Seed the widget, then subscribe and fetch a fresh aligned snapshot while
# WebSocket updates queue. Do not poll the rate-limited REST snapshot.
initial = await asyncio.to_thread(
    fetch_l4_book_snapshot, settings.hydromancer_api_key, [coin]
)
animator = animate_l4_book(initial, coin=coin, level_count=12, minimum_named_share=1.0)
live_l4_task = asyncio.create_task(
    stream_l4_book_updates(
        animator,
        settings.hydromancer_api_key,
        snapshot_fetcher=lambda: fetch_l4_book_snapshot(
            settings.hydromancer_api_key, [coin]
        ),
        max_fps=5.0,
    )
)
```

To stop the live monitor without shutting down Jupyter:

```python
live_l4_task.cancel()
await asyncio.gather(live_l4_task, return_exceptions=True)
```

This displays L4 wallet-attributed orders grouped into price levels. L2 remains
the aggregate source used for spread, imbalance, event volatility, and markouts.

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

## Run ad hoc database queries

Run the editable multiline query in `query_db.sh`:

```bash
./query_db.sh
```

Edit the SQL block near the bottom of that script to change the query without
having to quote SQL on the command line. The script checks that the TimescaleDB
container is running and connects to the `hydromancer` database as `postgres`.

It can also execute a separate SQL file:

```bash
./query_db.sh sql/inspect_live.sql
./query_db.sh sql/inspect_labels.sql
```

The container, database, and user can be overridden when needed:

```bash
CONTAINER=hydromancer-timescaledb \
DATABASE=hydromancer \
DB_USER=postgres \
./query_db.sh
```

## Publication safety

See [SECURITY.md](SECURITY.md) before making a fork or repository public. Local
credentials, wallet signing material, raw datasets, generated notebooks/reports,
and agent configuration are excluded from Git by default. The GitHub secret-scan
workflow checks committed history; it does not replace a local pre-push review.

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
