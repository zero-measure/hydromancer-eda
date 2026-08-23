# hydromancer-eda

Market-wide BTC/ETH orderflow ingestion and wallet-flow labeling with Hydromancer and TimescaleDB.

## Ingestion protocol

The application maintains **one** mainnet connection to `wss://api.hydromancer.xyz/ws`. It authenticates with the token query parameter (the resulting URL is never logged), requests `liveFormat=chunked-v1` on every connection, and sends one case-preserving `allFills` subscription for `HYDROMANCER_COINS` (default `BTC,ETH`). It does not use `builderFills` or any REST fallback.

It replies to JSON application pings with JSON pongs. The durable `ingestion_checkpoints` row for channel `allFills` stores both `session_id` and the opaque `cursor`; reconnects provide both. A resumed session restores its subscription and replays automatically. If the server instead reports `connected`, the new session is saved and a warning records that the old session expired/a gap may exist.

Replay chunks are committed before buffered live messages, which are then processed in arrival order. Fills use the documented `[walletAddress, fillObject]` shape. `(time, txIndex)` bounds replay/live overlap; within an equal key, the wallet/trade/order identity retains every market-wide participant. The database backstop uses that same per-fill identity. Every committed frame, including cursor-only and duplicate-only frames, updates its checkpoint in the same transaction. `hasGap` and sequence gaps are warnings only: there is no builder REST semantic fallback for market-wide gaps.

## Prerequisites

The application does not have a file, SQLite, or in-memory production fallback. It must connect to PostgreSQL with the TimescaleDB extension before it opens the Hydromancer WebSocket. If database initialization fails, ingestion does not start.

Install or obtain:

- Python 3.11 or newer
- A Hydromancer API key
- PostgreSQL with the TimescaleDB extension
- Docker, if using the recommended local database setup below

## Local TimescaleDB with Docker

For local development, run the official TimescaleDB image. The named volume preserves database data when the container is replaced.

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

Wait for PostgreSQL to accept connections:

```bash
docker exec hydromancer-timescaledb pg_isready \
  --username postgres \
  --dbname hydromancer
```

Verify that TimescaleDB can be enabled:

```bash
docker exec hydromancer-timescaledb \
  psql --username postgres --dbname hydromancer \
  --command='CREATE EXTENSION IF NOT EXISTS timescaledb;'
```

Useful container commands:

```bash
# Stop the database without deleting its data.
docker stop hydromancer-timescaledb

# Start it again later.
docker start hydromancer-timescaledb

# Follow PostgreSQL logs.
docker logs --follow hydromancer-timescaledb
```

For a native or externally hosted PostgreSQL installation, install TimescaleDB using the [official self-hosted installation guide](https://docs.timescale.com/self-hosted/latest/install/). The database user in `PG_DSN` must be able to run the packaged schema, including `CREATE EXTENSION IF NOT EXISTS timescaledb` and TimescaleDB's `create_hypertable` function.

## Application setup

Create and activate a virtual environment, then install the project:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

Create local configuration:

```bash
cp .env.example .env
```

Edit `.env` and set at least:

```dotenv
HYDROMANCER_API_KEY=replace-with-your-api-key
HYDROMANCER_COINS=BTC,ETH
PG_DSN=postgresql://postgres:postgres@localhost:5432/hydromancer
```

Do not commit `.env`; it contains the API token and possibly database credentials. The file is ignored by Git.

## Run ingestion

With TimescaleDB running and `.env` configured:

```bash
PYTHONPATH=src python -m hydromancer_eda.main
```

At startup, the application reads `src/hydromancer_eda/sql/schema.sql` from the installed package and creates or updates:

- `orderflow_events` for normalized participant-level fills
- `ingestion_checkpoints` for the durable session and opaque cursor
- `ingestion_rejects` for malformed upstream entries
- `wallet_flow_labels` for computed wallet metrics

Inspect row counts from the Docker database:

```bash
docker exec hydromancer-timescaledb \
  psql --username postgres --dbname hydromancer \
  --command='SELECT COUNT(*) FROM orderflow_events;'
```

`HYDROMANCER_BUILDER_ADDRESS` is not used or required.

## Verification

```bash
ruff check .
mypy src
pytest -q
```
