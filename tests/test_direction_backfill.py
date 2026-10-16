from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from hydromancer_eda.backfill import backfill_wallet_directions

WALLET = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"


class Client:
    def __init__(self, pages=None, error: Exception | None = None) -> None:
        self.pages = pages or []
        self.error = error
        self.calls = []

    def user_fills_pages(self, wallet, start_ms, end_ms):
        self.calls.append((wallet, start_ms, end_ms))
        if self.error:
            raise self.error
        yield from self.pages


class Transaction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *args):
        return None


class Pool:
    def __init__(self, candidates: int = 2) -> None:
        self.values = iter([candidates, 99])
        self.executions = []

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    def transaction(self):
        return Transaction()

    async def fetchval(self, query, *args):
        return next(self.values)

    async def execute(self, query, *args):
        self.executions.append((query, args))
        if query.lstrip().startswith("UPDATE orderflow_events"):
            return "UPDATE 1"
        return "UPDATE 1"


def fill(ts: int) -> dict[str, object]:
    return {
        "dir": "Open Long", "startPosition": "0", "time": ts, "txIndex": 1,
        "tid": 2, "oid": 3, "coin": "BTC", "side": "B", "px": "10", "sz": "1",
    }


@pytest.mark.asyncio
async def test_backfill_exact_match_preserves_per_field_provenance_and_records_coverage() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    timestamp = int(start.timestamp() * 1000)
    pool = Pool(candidates=2)
    client = Client([[fill(timestamp)]])

    assert await backfill_wallet_directions(
        pool, client, f" {WALLET.upper()} ", start, start + timedelta(hours=1)
    ) == (1, 1)
    update, args = pool.executions[0]
    assert "TIMESTAMPTZ 'epoch'" in update
    assert "direction_provenance=CASE WHEN direction IS NULL" in update
    assert "start_position_provenance=CASE WHEN start_position IS NULL" in update
    assert args[2] == WALLET
    completed = pool.executions[-1]
    assert "source_complete=TRUE" in completed[0]
    assert completed[1] == (1, 1, 0, 0, 99)


@pytest.mark.asyncio
async def test_backfill_failure_is_audited_and_leaves_rows_untouched() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    pool = Pool()
    client = Client(error=RuntimeError("secret transport detail"))

    with pytest.raises(RuntimeError):
        await backfill_wallet_directions(pool, client, WALLET, start, start + timedelta(hours=1))
    assert len(pool.executions) == 1
    query, args = pool.executions[0]
    assert "status='failed'" in query
    assert args[3] == "RuntimeError"


@pytest.mark.asyncio
async def test_backfill_records_unusable_source_rows_without_inference() -> None:
    start = datetime(2025, 8, 1, tzinfo=UTC)
    pool = Pool(candidates=1)
    client = Client([[{"time": int(start.timestamp() * 1000), "side": "B"}]])

    assert await backfill_wallet_directions(
        pool, client, WALLET, start, start + timedelta(hours=1)
    ) == (1, 0)
    assert len(pool.executions) == 1
    query, args = pool.executions[0]
    assert "unusable_count=$3" in query
    assert args == (1, 0, 1, 0, 99)
