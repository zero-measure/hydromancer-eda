# Hydromancer development rules

- Read https://docs.hydromancer.xyz/llms.txt before changing API integrations.
- Market-wide fills use the allFills WebSocket, not builderFills REST.
- Never log WebSocket URLs because they contain the API token.
- Always respond to ping with pong.
- Treat cursors as opaque strings.
- Deduplicate replay/live overlap before inserting.
- Run: `ruff check .`, `mypy src`, `pytest -q`.
- Do not weaken or delete tests to make a change pass.
