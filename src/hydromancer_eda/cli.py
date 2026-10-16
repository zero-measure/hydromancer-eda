"""Installable batch CLI for funding-rate versus hourly taker position flow."""
from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

import asyncpg

from hydromancer_eda.analysis.position_flow import load_hourly_position_flow
from hydromancer_eda.backfill import LIMITATIONS, backfill_wallet_directions
from hydromancer_eda.client.rest import HydromancerAPIError, HydromancerRestClient
from hydromancer_eda.config import Settings
from hydromancer_eda.funding import fetch_current_funding_rates, fetch_funding_history
from hydromancer_eda.storage.postgres import PostgresStore
from hydromancer_eda.visualization.funding_flow import (
    build_funding_flow_figure,
    write_analysis_csv,
    write_analysis_html,
)


def parse_interval_value(value: str, *, is_end: bool) -> datetime:
    """Parse UTC date/date-time; date ends advance to next exclusive midnight."""
    try:
        if len(value) == 10:
            parsed_date = date.fromisoformat(value)
            result = datetime.combine(parsed_date, time.min, tzinfo=UTC)
            return result + timedelta(days=1) if is_end else result
        normalized = value[:-1] + "+00:00" if value.endswith(("Z", "z")) else value
        result = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date/timestamp: {value}") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    """Construct the command parser without reading secrets or the environment."""
    parser = argparse.ArgumentParser(prog="hydromancer-funding-flow")
    parser.add_argument("--coin", action="append", required=True,
                        help="case-sensitive coin identifier; repeat for multiple coins")
    parser.add_argument("--start", required=True, help="inclusive ISO UTC date or timestamp")
    parser.add_argument("--end", required=True,
                        help="inclusive named date, or exact exclusive ISO timestamp")
    parser.add_argument("--html", required=True, help="output HTML path")
    parser.add_argument("--csv", help="optional CSV output path")
    parser.add_argument("--allow-partial", action="store_true",
                        help="show net values from classified subset of incomplete hours")
    parser.add_argument("--current-rate", action="store_true",
                        help="wait briefly for one optional live fundingRates event")
    parser.add_argument("--current-timeout", type=float, default=8.0)
    parser.add_argument("--backfill-wallet", action="append", default=[],
                        help="known wallet whose directions may be reconstructed; repeatable")
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--pg-dsn", default=None, help="defaults to PG_DSN")
    return parser


async def _run(args: argparse.Namespace) -> int:
    start = parse_interval_value(args.start, is_end=False)
    end = parse_interval_value(args.end, is_end=True)
    if start >= end:
        raise ValueError("start must precede end")
    if args.current_timeout <= 0 or args.timeout <= 0:
        raise ValueError("timeouts must be positive")
    if args.retries < 0:
        raise ValueError("retries must be non-negative")
    coins = list(args.coin)
    if any(not coin or coin != coin.strip() for coin in coins):
        raise ValueError("coins must be nonempty and must not contain surrounding whitespace")
    if len(set(coins)) != len(coins):
        raise ValueError("duplicate --coin values are not allowed")

    # Settings loads secrets from the environment or .env. CLI coins remain exact argv values.
    if args.pg_dsn is None:
        settings = Settings(HYDROMANCER_COINS=",".join(coins))  # type: ignore[call-arg]
    else:
        settings = Settings(
            PG_DSN=args.pg_dsn,
            HYDROMANCER_COINS=",".join(coins),
        )  # type: ignore[call-arg]
    token = settings.hydromancer_api_key
    client = HydromancerRestClient(token, timeout=args.timeout, max_retries=args.retries)
    funding = []
    # Keep weighted fundingHistory requests sequential to avoid avoidable throttling.
    for coin in coins:
        print(f"funding history {coin}: retrieving")
        observations = await asyncio.to_thread(
            fetch_funding_history, client, coin, start, end
        )
        funding.extend(observations)
        print(f"funding history {coin}: retrieved {len(observations)} observations")
    if args.current_rate:
        now = datetime.now(UTC)
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        if start < current_hour + timedelta(hours=1) and end > current_hour:
            current = await fetch_current_funding_rates(
                token, coins, timeout=args.current_timeout
            )
            existing = {(item.coin, item.ts) for item in funding}
            funding.extend(
                item
                for item in current
                if start <= item.ts < end and (item.coin, item.ts) not in existing
            )
        else:
            print("current funding rate skipped: interval does not overlap current UTC hour")
    store = PostgresStore(settings)
    await store.connect()
    try:
        await store.init_schema()
        for wallet in args.backfill_wallet:
            fetched, matched = await backfill_wallet_directions(
                store.pool, client, wallet, start, end
            )
            print(f"direction backfill {wallet}: fetched={fetched}, matched={matched}")
        flows = await load_hourly_position_flow(
            store.pool, coins, start, end, allow_partial=args.allow_partial
        )
    finally:
        await store.close()

    output = Path(args.html)
    output.parent.mkdir(parents=True, exist_ok=True)
    figure = build_funding_flow_figure(coins, flows, funding)
    write_analysis_html(output, figure)
    if args.csv:
        csv_path = Path(args.csv)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        write_analysis_csv(csv_path, flows, funding)
    incomplete = sum(
        item.total_notional is not None
        and item.unclassified_notional is not None
        and item.unclassified_notional > 0
        for item in flows
    )
    print(f"wrote {output}; incomplete flow hours={incomplete}")
    if incomplete and not args.allow_partial:
        print("net metrics are null for incomplete hours; use --allow-partial explicitly to override")
    if args.backfill_wallet:
        print(LIMITATIONS)
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the batch command with concise sanitized failures."""
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except (ValueError, HydromancerAPIError) as exc:
        parser.error(str(exc))
    except (asyncpg.PostgresError, OSError):
        # Database and filesystem exception text can contain DSNs, hosts, or paths.
        parser.error("database or output operation failed; details suppressed")
    return 2
