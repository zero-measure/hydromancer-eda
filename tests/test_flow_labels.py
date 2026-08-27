import asyncio
from datetime import UTC, datetime

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


def captured_label_query() -> CapturingConnection:
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


def test_label_defaults_use_hourly_lifetime_snapshots() -> None:
    assert Settings.model_fields["label_interval_seconds"].default == 3600
    assert "label_agg_window_hours" not in Settings.model_fields
    assert "label_min_notional_24h" not in Settings.model_fields


def test_label_query_uses_all_observed_history_without_volume_filter() -> None:
    connection = captured_label_query()

    assert "WHERE ts <= $1::timestamptz" in connection.query
    assert "make_interval" not in connection.query
    assert "qualified" not in connection.query
    assert "HAVING SUM(ABS" not in connection.query
    assert len(connection.arguments) == 2


def test_trade_ratios_include_liquidations_and_store_counts() -> None:
    query = captured_label_query().query

    trade_population = "event_type IN ('trade', 'fill', 'liquidation')"
    assert query.count(trade_population) >= 4
    assert "AS trade_count" in query
    assert "WHERE liquidation\n                      AND event_type IN" in query
    assert "AS liquidation_ratio" in query
    assert "AS taker_ratio" in query


def test_markout_query_produces_deciles_and_sample_count() -> None:
    query = captured_label_query().query

    assert "COUNT(*) AS markout_sample_count" in query
    assert "ORDER BY next_event.ts ASC, next_event.id ASC" in query
    for decile in range(1, 10):
        quantile = f"0.{decile}"
        assert f"percentile_cont({quantile})" in query
        assert f"AS markout_p{decile}0" in query


def test_message_cadence_uses_observed_timestamp_span() -> None:
    query = captured_label_query().query

    assert "MAX(ts) - MIN(ts)" in query
    assert "message_cadence_per_min" in query


def test_round_notional_ratio_uses_exact_whole_dollar_moduli() -> None:
    query = captured_label_query().query

    assert "MOD(ABS(notional), r.modulus) = 0" in query
    assert "ABS(COALESCE(notional,0) - ROUND" not in query
    for modulus in (10, 100, 1000, 10000):
        assert f"({modulus}::numeric)" in query


def test_round_size_ratio_uses_exact_tenth_token_increments() -> None:
    query = captured_label_query().query

    assert "MOD(ABS(size), 0.1::numeric) = 0" in query
    assert "COUNT(*) FILTER (WHERE size IS NOT NULL)" in query
