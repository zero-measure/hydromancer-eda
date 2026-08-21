# hydromancer-eda

Market-wide BTC/ETH orderflow ingestion and wallet-flow labeling with Hydromancer and TimescaleDB.

## Ingestion protocol

The application maintains **one** mainnet connection to `wss://api.hydromancer.xyz/ws`. It authenticates with the token query parameter (the resulting URL is never logged), requests `liveFormat=chunked-v1` on every connection, and sends one case-preserving `allFills` subscription for `HYDROMANCER_COINS` (default `BTC,ETH`). It does not use `builderFills` or any REST fallback.

It replies to JSON application pings with JSON pongs. The durable `ingestion_checkpoints` row for channel `allFills` stores both `session_id` and the opaque `cursor`; reconnects provide both. A resumed session restores its subscription and replays automatically. If the server instead reports `connected`, the new session is saved and a warning records that the old session expired/a gap may exist.

Replay chunks are committed before buffered live messages, which are then processed in arrival order. Fills use the documented `[walletAddress, fillObject]` shape. `(time, txIndex)` bounds replay/live overlap; within an equal key, the wallet/trade/order identity retains every market-wide participant. The database backstop uses that same per-fill identity. Every committed frame, including cursor-only and duplicate-only frames, updates its checkpoint in the same transaction. `hasGap` and sequence gaps are warnings only: there is no builder REST semantic fallback for market-wide gaps.

## Quick start

```bash
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
PYTHONPATH=src python -m hydromancer_eda.main
```

`HYDROMANCER_BUILDER_ADDRESS` is not used or required.

## Verification

```bash
ruff check .
mypy src
pytest -q
```
