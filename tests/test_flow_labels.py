import asyncio
from datetime import UTC, datetime

from hydromancer_eda.config import Settings
from hydromancer_eda.labeling.flow_labels import FlowLabeler


class CapturingConnection:
    def __init__(self) -> None:
        self.fetch_query = ""
        self.execute_query = ""
        self.fetch_arguments = ()

    async def execute(self, query, *_arguments):
        self.execute_query = query
        return "INSERT 0 0"

    async def fetch(self, query, *arguments):
        self.fetch_query = query
        self.fetch_arguments = arguments
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


def captured_queries() -> CapturingConnection:
    connection = CapturingConnection()
    settings = Settings(
        HYDROMANCER_API_KEY="test-token",
        PG_DSN="postgresql://example/test",
    )
    snapshots = asyncio.run(
        FlowLabeler(settings).compute_snapshots(
            Pool(connection),  # type: ignore[arg-type]
            datetime(2025, 1, 1, tzinfo=UTC),
        )
    )
    assert snapshots == []
    return connection


def test_label_defaults_use_hourly_event_time_snapshots() -> None:
    assert Settings.model_fields["label_interval_seconds"].default == 3600
    assert Settings.model_fields["l2_retention_hours"].default == 168
    assert Settings.model_fields["l4_retention_hours"].default == 24


def test_markout_uses_confirmed_event_horizons_and_elapsed_limits() -> None:
    query = captured_queries().execute_query

    assert "VALUES (50, 300), (100, 1800), (1000, 3600)" in query
    assert "future.event_no = candidate.event_no + horizon.horizon_events" in query
    assert "future.epoch = candidate.epoch" in query
    assert "BETWEEN 0 AND horizon.max_elapsed_seconds" in query


def test_markout_is_signed_log_return_scaled_by_past_event_volatility() -> None:
    query = captured_queries().execute_query

    assert "LN((future.mid_price / candidate.price)::float8)" in query
    assert "LN((candidate.price / future.mid_price)::float8)" in query
    assert "candidate.trailing_event_volatility * SQRT" in query
    assert "tick.ts < fill.ts" in query


def test_labels_keep_classifier_features_and_deciles() -> None:
    query = captured_queries().fetch_query

    assert "AS trade_count" in query
    assert "AS taker_ratio" in query
    assert "AS liquidation_ratio" in query
    assert "MAX(ts) - MIN(ts)" in query
    assert "wallet_leverage_state" in query
    assert "wallet_order_aggregates" in query
    for decile in range(1, 10):
        assert f"percentile_cont(0.{decile})" in query


def test_round_size_and_notional_features_remain_exact() -> None:
    query = captured_queries().fetch_query

    assert "MOD(ABS(notional), 10::numeric) = 0" in query
    assert "MOD(ABS(size), 0.1::numeric) = 0" in query
