import asyncio
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any, cast

from hydromancer_eda.config import Settings
from hydromancer_eda.models import WalletLabelSnapshot
from hydromancer_eda.storage.postgres import PostgresStore


class Transaction:
    def __init__(self, connection: "Connection") -> None:
        self.connection = connection

    async def __aenter__(self) -> None:
        self.connection.calls.append("transaction_enter")

    async def __aexit__(self, *_args: object) -> None:
        self.connection.calls.append("transaction_exit")


class Connection:
    def __init__(self) -> None:
        self.batch_query = ""
        self.batch_rows: list[tuple[object, ...]] = []
        self.execute_query = ""
        self.execute_arguments: tuple[object, ...] = ()
        self.calls: list[str] = []

    def transaction(self) -> Transaction:
        return Transaction(self)

    async def executemany(self, query: str, rows: list[tuple[object, ...]]) -> None:
        self.calls.append("executemany")
        self.batch_query = query
        self.batch_rows = rows

    async def execute(self, query: str, *arguments: object) -> None:
        self.calls.append("execute")
        self.execute_query = query
        self.execute_arguments = arguments


class AcquireConnection:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        self.connection.calls.append("acquire_enter")
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        self.connection.calls.append("acquire_exit")


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


def test_schema_defines_lifetime_labels_without_destructive_migration() -> None:
    schema = files("hydromancer_eda.sql").joinpath("schema.sql").read_text(encoding="utf-8")

    assert "wallet TEXT PRIMARY KEY" in schema
    assert "trade_count BIGINT" in schema
    assert "markout_sample_count BIGINT" in schema
    assert "markout_p10 DOUBLE PRECISION" in schema
    assert "markout_p90 DOUBLE PRECISION" in schema
    assert "liquidation_ratio DOUBLE PRECISION" in schema
    assert "RAISE EXCEPTION" in schema
    assert "run sql/reset_wallet_labels.sql before startup" in schema
    assert "DROP TABLE wallet_flow_labels" not in schema
    assert "TRUNCATE" not in schema


def test_explicit_reset_drops_only_wallet_labels() -> None:
    reset_path = Path(__file__).parents[1] / "sql" / "reset_wallet_labels.sql"
    reset = reset_path.read_text(encoding="utf-8")
    statements = [line for line in reset.splitlines() if not line.startswith("--")]
    executable = "\n".join(statements)

    assert "DROP TABLE IF EXISTS public.wallet_flow_labels CASCADE" in executable
    assert "orderflow_events" not in executable
    assert "ingestion_checkpoints" not in executable
    assert "TRUNCATE" not in executable


def test_label_upsert_updates_without_pruning_lifetime_wallets() -> None:
    store, connection = store_with_connection()
    snapshot = WalletLabelSnapshot(
        bucket_ts=datetime(2025, 1, 1, tzinfo=UTC),
        wallet="0xabc",
        trade_count=100,
        markout_sample_count=50,
        markout_p10=-0.02,
        markout_p90=0.03,
        liquidation_ratio=0.01,
        realized_pnl=12.5,
    )

    count = asyncio.run(store.upsert_label_snapshots([snapshot]))

    assert count == 1
    assert "ON CONFLICT (wallet) DO UPDATE" in connection.batch_query
    assert connection.batch_rows[0][1] == "0xabc"
    assert connection.batch_rows[0][2] == 100
    assert connection.batch_rows[0][3] == 50
    assert "markout_p10 = EXCLUDED.markout_p10" in connection.batch_query
    assert "markout_p90 = EXCLUDED.markout_p90" in connection.batch_query
    assert "liquidation_ratio = EXCLUDED.liquidation_ratio" in connection.batch_query
    assert "taker_markout" not in connection.batch_query
    assert "liquidation_count" not in connection.batch_query
    assert "DELETE FROM wallet_flow_labels" not in connection.batch_query
    assert connection.execute_query == ""
    assert connection.calls == [
        "acquire_enter",
        "transaction_enter",
        "executemany",
        "transaction_exit",
        "acquire_exit",
    ]


def test_empty_label_set_does_not_prune_lifetime_wallets() -> None:
    store, connection = store_with_connection()

    count = asyncio.run(store.upsert_label_snapshots([]))

    assert count == 0
    assert connection.batch_query == ""
    assert connection.execute_query == ""
    assert connection.calls == []
