import asyncio
from datetime import UTC, datetime
from importlib.resources import files
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


def test_schema_replaces_only_obsolete_label_history() -> None:
    schema = files("hydromancer_eda.sql").joinpath("schema.sql").read_text(encoding="utf-8")

    assert schema.index("DROP TABLE wallet_flow_labels CASCADE") < schema.index(
        "CREATE TABLE IF NOT EXISTS wallet_flow_labels"
    )
    assert "wallet TEXT PRIMARY KEY" in schema
    assert "DROP TABLE orderflow_events" not in schema
    assert "DROP TABLE ingestion_checkpoints" not in schema


def test_label_upsert_replaces_current_wallet_set() -> None:
    store, connection = store_with_connection()
    snapshot = WalletLabelSnapshot(
        bucket_ts=datetime(2025, 1, 1, tzinfo=UTC),
        wallet="0xabc",
        realized_pnl=12.5,
    )

    count = asyncio.run(store.upsert_label_snapshots([snapshot]))

    assert count == 1
    assert "ON CONFLICT (wallet) DO UPDATE" in connection.batch_query
    assert connection.batch_rows[0][1] == "0xabc"
    assert "DELETE FROM wallet_flow_labels" in connection.execute_query
    assert connection.execute_arguments == (["0xabc"],)
    assert connection.calls == [
        "acquire_enter",
        "transaction_enter",
        "executemany",
        "execute",
        "transaction_exit",
        "acquire_exit",
    ]


def test_empty_label_set_removes_all_current_wallets() -> None:
    store, connection = store_with_connection()

    count = asyncio.run(store.upsert_label_snapshots([]))

    assert count == 0
    assert connection.batch_query == ""
    assert connection.execute_arguments == ([],)
    assert connection.calls == [
        "acquire_enter",
        "transaction_enter",
        "execute",
        "transaction_exit",
        "acquire_exit",
    ]
