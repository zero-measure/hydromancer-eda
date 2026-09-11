# L2/L4 order-book ingestion

This package is the wire-format boundary for order-book snapshots and updates.
It keeps book decoding separate from WebSocket orchestration, PostgreSQL
persistence, wallet feature computation, and visualization.

## Responsibilities

- `l2.py`: decode `l2BookDiffSnapshot` bootstrap payloads. L2 is the canonical
  aggregate market state for bid/ask depth, spread, imbalance, event numbering,
  and trailing event-time volatility.
- `l4.py`: decode `l4Book` bootstrap payloads without dropping order ID, wallet,
  side, price, or size. L4 is the canonical source for wallet-attributed resting
  orders and lifecycle activity.
- `ingestion/marketdata.py`: subscribe before snapshotting, align replay/live
  data, enforce height/epoch/sequence continuity, and persist updates.

Both books must preserve sides explicitly: L2 uses `bid`/`ask`; normalized fills
and wallet order events use `buy`/`sell`. Conversion belongs at the ingestion
boundary and must never be inferred from price movement.

## Wallet-monitoring feature plan

The durable classifier should combine streams rather than ask either book to
answer questions it cannot answer alone:

| Feature | Inputs | Notes |
|---|---|---|
| Quoting share and depth | L4 + L2 | Wallet size divided by total size, separately for bids and asks and by distance from mid. |
| Add/update/remove cadence | L4 | Compute separately by wallet, symbol, and side. |
| Cancel rate | L4 + `allFills` | A `remove` is not necessarily a cancel. Match order IDs to fills first; report unmatched-remove rate and executed-remove rate separately. |
| Resting lifetime | L4 | Keep side-aware distributions; snapshot-seeded orders are left-censored and must not enter lifetime aggregates. |
| Markout | `allFills` + L2 | Signed log return at fixed L2 event horizons, scaled only by volatility known before the fill. Keep maker/taker and buy/sell cohorts separate. |
| Book response | L2 + fills/L4 | Measure post-event spread, imbalance, and same/opposite-side depth changes. |

## Flow labels

`retail`, `market_maker`, and `informed` should be probabilistic labels computed
under `labeling/`, not facts attached during ingestion. Market making and
informed trading are not opposites: a wallet can quote both sides frequently
and still exhibit informed inventory changes or toxic markouts.

Useful evidence includes:

- **Retail-like:** round sizing, lower message cadence, directional/taker-heavy
  flow, sparse two-sided quoting, and longer reaction times.
- **Market-maker-like:** persistent two-sided quoting, high update/cancel
  cadence, balanced bid/ask participation, short quote lifetimes, and maker
  executions.
- **Informed/toxic:** consistently favorable volatility-normalized markouts,
  side-specific depth withdrawal before moves, and adverse outcomes for the
  passive counterparties.

Always publish sample counts, observation windows, side/symbol breakdowns, and
an `unknown` result when evidence is insufficient. Do not merge `market_maker`
and `informed` into one label.

## Invariants

1. Subscribe before fetching a REST snapshot, then discard buffered updates
   covered by the snapshot height.
2. Treat replay cursors as opaque and deduplicate replay/live overlap.
3. Re-bootstrap L2 on sequence gaps, epoch changes, or resync signals.
4. Never log token-bearing URLs, API keys, session IDs, or cursors.
5. Keep REST snapshots bootstrap-only; do not poll them for live monitoring.
