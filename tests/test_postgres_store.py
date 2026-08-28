import asyncio
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

from hydromancer_eda.config import Settings
from hydromancer_eda.models import WalletLabelSnapshot
from hydromancer_eda.storage.postgres import PostgresStore


class Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class Connection:
    def __init__(self) -> None:
        self.query = ""
        self.rows: list[tuple[object, ...]] = []

    def transaction(self) -> Transaction:
        return Transaction()

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> None:
        self.query = query
        self.rows = rows


class AcquireConnection:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class Pool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def acquire(self) -> AcquireConnection:
        return AcquireConnection(self.connection)


def store_with_connection() -> tuple[PostgresStore, Connection]:
    settings = Settings(
        HYDROMANCER_API_KEY="test-token",
        PG_DSN="postgresql://example/test",
    )
    connection = Connection()
    store = PostgresStore(settings)
    store._pool = cast(Any, Pool(connection))
    return store, connection


def test_schema_contains_only_explicit_reset_for_destructive_changes() -> None:
    schema = files("hydromancer_eda.sql").joinpath("schema.sql").read_text(encoding="utf-8")

    assert "l2_book_ticks" in schema
    assert "l2_book_state" in schema
    assert "l2_book_levels" in schema
    assert "l4_order_events" in schema
    assert "wallet_order_aggregates" in schema
    assert "leverage_updates" in schema
    assert "wallet_leverage_state" in schema
    assert "wallet_markout_samples" in schema
    assert "markout_metrics JSONB" in schema
    assert "DROP TABLE" not in schema
    assert "TRUNCATE" not in schema


def test_explicit_reset_removes_all_analytics_but_not_operational_state() -> None:
    reset = (Path(__file__).parents[1] / "sql" / "reset_analytics.sql").read_text()
    executable = "\n".join(
        line for line in reset.splitlines() if not line.startswith("--")
    )

    for table in (
        "orderflow_events",
        "l2_book_ticks",
        "l2_event_counters",
        "l2_book_state",
        "l2_book_levels",
        "l4_order_events",
        "wallet_order_aggregates",
        "leverage_updates",
        "wallet_leverage_state",
        "wallet_markout_samples",
        "wallet_flow_labels",
    ):
        assert f"DROP TABLE IF EXISTS public.{table} CASCADE" in executable
    assert "ingestion_checkpoints" not in executable
    assert "ingestion_rejects" not in executable
    assert "TRUNCATE" not in executable


def test_label_upsert_persists_compact_classifier_features() -> None:
    store, connection = store_with_connection()
    snapshot = WalletLabelSnapshot(
        bucket_ts=datetime(2025, 1, 1, tzinfo=UTC),
        wallet="0xabc",
        trade_count=100,
        taker_ratio=0.9,
        liquidation_ratio=0.1,
        max_leverage=20,
        order_remove_ratio=0.5,
        markout_metrics={"50": {"sample_count": 40, "p50": -1.2}},
    )

    count = asyncio.run(store.upsert_label_snapshots([snapshot]))

    assert count == 1
    assert "ON CONFLICT (wallet) DO UPDATE" in connection.query
    assert "markout_metrics = EXCLUDED.markout_metrics" in connection.query
    assert "holding_time_sec_median" not in connection.query
    assert "cancel_to_fill_ratio" not in connection.query
    assert connection.rows[0][1] == "0xabc"
    assert '"sample_count": 40' in cast(str, connection.rows[0][19])


def test_empty_label_result_never_prunes_wallets() -> None:
    store, connection = store_with_connection()

    assert asyncio.run(store.upsert_label_snapshots([])) == 0
    assert connection.query == ""
