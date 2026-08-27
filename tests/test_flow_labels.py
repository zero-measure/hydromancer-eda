import asyncio
from datetime import UTC, datetime
from decimal import Decimal

from hydromancer_eda.config import Settings
from hydromancer_eda.labeling.flow_labels import FlowLabeler


class CapturingConnection:
    def __init__(self) -> None:
        self.query = ""
        self.arguments = ()

    async def fetch(self, query, *arguments):
        self.query = query
        self.arguments = arguments
        return []


class AcquireConnection:
    def __init__(self, connection: CapturingConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> CapturingConnection:
        return self.connection

    async def __aexit__(self, *_args) -> None:
        return None


class Pool:
    def __init__(self, connection: CapturingConnection) -> None:
        self.connection = connection

    def acquire(self) -> AcquireConnection:
        return AcquireConnection(self.connection)


def test_label_defaults_use_hourly_current_state_threshold() -> None:
    assert Settings.model_fields["label_interval_seconds"].default == 3600
    assert Settings.model_fields["label_min_notional_24h"].default == Decimal(10000)


def test_round_notional_ratio_uses_exact_whole_dollar_moduli() -> None:
    connection = CapturingConnection()
    settings = Settings(
        HYDROMANCER_API_KEY="test-token",
        PG_DSN="postgresql://example/test",
        LABEL_MIN_NOTIONAL_24H="12345",
    )

    snapshots = asyncio.run(
        FlowLabeler(settings).compute_snapshots(
            Pool(connection),  # type: ignore[arg-type]
            datetime(2025, 1, 1, tzinfo=UTC),
        )
    )

    assert snapshots == []
    assert "MOD(ABS(notional), r.modulus) = 0" in connection.query
    assert "ABS(COALESCE(notional,0) - ROUND" not in connection.query
    assert "HAVING SUM(ABS(COALESCE(notional, 0))) >= $4::numeric" in connection.query
    assert "JOIN qualified q ON q.wallet = t.wallet" in connection.query
    assert "JOIN qualified q ON q.wallet = o.wallet" in connection.query
    assert connection.arguments[3] == Decimal(12345)
    for modulus in (10, 100, 1000, 10000):
        assert f"({modulus}::numeric)" in connection.query


def test_round_size_ratio_uses_exact_tenth_token_increments() -> None:
    connection = CapturingConnection()
    settings = Settings(
        HYDROMANCER_API_KEY="test-token",
        PG_DSN="postgresql://example/test",
    )

    asyncio.run(
        FlowLabeler(settings).compute_snapshots(
            Pool(connection),  # type: ignore[arg-type]
            datetime(2025, 1, 1, tzinfo=UTC),
        )
    )

    assert "MOD(ABS(size), 0.1::numeric) = 0" in connection.query
    assert "COUNT(*) FILTER (WHERE size IS NOT NULL)" in connection.query
