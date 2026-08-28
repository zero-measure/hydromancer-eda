import asyncio
import math
import os
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from hydromancer_eda.config import Settings
from hydromancer_eda.labeling.flow_labels import FlowLabeler


@pytest.mark.skipif(not os.getenv("TEST_PG_DSN"), reason="TEST_PG_DSN is not configured")
def test_event_time_markout_sign_scaling_and_elapsed_limit() -> None:
    async def run() -> None:
        pool = await asyncpg.create_pool(os.environ["TEST_PG_DSN"], min_size=1, max_size=1)
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TEMP TABLE orderflow_events (
                    id BIGINT, ts TIMESTAMPTZ, wallet TEXT, symbol TEXT, side TEXT,
                    aggressor TEXT, price NUMERIC, size NUMERIC, notional NUMERIC,
                    realized_pnl NUMERIC, liquidation BOOLEAN
                );
                CREATE TEMP TABLE l2_book_ticks (
                    ts TIMESTAMPTZ, symbol TEXT, event_no BIGINT, epoch TEXT,
                    mid_price NUMERIC, trailing_event_volatility FLOAT8
                );
                CREATE TEMP TABLE wallet_markout_samples (
                    fill_id BIGINT, fill_ts TIMESTAMPTZ, wallet TEXT, symbol TEXT,
                    horizon_events INT, elapsed_seconds FLOAT8, raw_log_markout FLOAT8,
                    normalized_markout FLOAT8, volatility_scale FLOAT8,
                    PRIMARY KEY (fill_id, horizon_events)
                );
            """)
            start = datetime(2025, 1, 1, tzinfo=UTC)
            await conn.executemany(
                "INSERT INTO orderflow_events VALUES ($1,$2,$3,'COIN',$4,'taker',100,1,100,0,FALSE)",
                [(1, start, "buyer", "buy"), (2, start, "seller", "sell")],
            )
            await conn.executemany(
                "INSERT INTO l2_book_ticks VALUES ($1,'COIN',$2,'epoch',99,0.01)",
                [
                    (start - timedelta(seconds=1), 0),
                    (start + timedelta(seconds=100), 50),
                    (start + timedelta(seconds=4000), 1000),
                ],
            )
        settings = Settings(
            HYDROMANCER_API_KEY="test", PG_DSN=os.environ["TEST_PG_DSN"]
        )
        inserted = await FlowLabeler(settings).materialize_markout_samples(
            pool, start + timedelta(hours=2)
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM wallet_markout_samples ORDER BY wallet")
        assert inserted == 2
        assert len(rows) == 2
        expected = math.log(0.99) / (0.01 * math.sqrt(50))
        assert rows[0]["normalized_markout"] == pytest.approx(expected)
        assert rows[1]["normalized_markout"] == pytest.approx(-expected)
        assert {row["horizon_events"] for row in rows} == {50}
        await pool.close()

    asyncio.run(run())
