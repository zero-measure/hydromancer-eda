# Notebook order book

`animate_l4_book` is the existing price-level order-book visualization. Its data
comes from wallet-attributed **L4** orders grouped into levels, not a separate L2
feed or web frontend. Install the project's `notebooks` extras to use it.

```python
from hydromancer_eda.visualization import animate_l4_book

# Use your existing decoded L4 snapshot.
animator = animate_l4_book(snapshot, coin="HYPE", level_count=12)

# Update the same FigureWidget, or continue using stream_l4_book_updates.
animator.update(new_snapshot)
```

## Reading the panel

- Nearest asks appear above the spread, with the best ask at the bottom.
  Bids appear below, best bid first.
- Price is in the market's quote units. **Size and Total are in base-coin units**,
  not quote notional. Total accumulates from the best price outward on each side,
  across the requested visible levels only.
- Depth shading uses the same maximum cumulative size for both sides.
- Spread is best ask minus best bid. Percentage is spread divided by midpoint.
  An absent side has an explicit empty state and an unavailable spread (`—`).
- Each row retains its wallet ownership labels (shares >=5%). Labels wrap rather
  than disappearing inside small segments. The muted strip encodes wallet share,
  separately from the red/green cumulative depth shading.
- Hover a numeric value, ownership label, or wallet segment for exact price,
  level size, cumulative size, full wallet addresses, and percentages. The
  existing `minimum_named_share` grouping into `Other` is unchanged.
- Expand **Exact values & wallet shares** for a selectable semantic table,
  accessible by keyboard without hovering. The disclosure stays open during
  updates; its contents refresh at the same rate as the figure.

The live panel is capped at 480px and shrinks with its notebook output to 320px.
Smaller notebook panes scroll horizontally rather than crushing the columns.
A zero-height sizing companion handles parent resizes across FigureWidget
versions. Static figures default to 480px; Plotly layout overrides still work. Level count,
wallet-share threshold, render throttling, streaming, and the animator's
`.figure` API are retained. The renderer does not label snapshots as live or
invent a tick-size/trades control when those capabilities are unavailable.
