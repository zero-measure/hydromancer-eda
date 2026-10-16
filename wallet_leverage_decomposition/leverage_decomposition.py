"""Daily gross open-position notional grouped by configured leverage.

Historical snapshots come from Hydromancer Reservoir.  An optional current point
comes from the Hydromancer perpSnapshot REST add-on.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import struct
import subprocess
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

BUCKET = "hydromancer-reservoir"
PREFIX = "global/snapshots/perp/all/"
REGION = "ap-northeast-1"
EARLIEST_SNAPSHOT_DATE = date(2025, 8, 1)
BUCKET_LABELS = tuple([f"{value}x" for value in range(1, 10)] + ["10x–25x", ">25x"])
COLORS = (
    "#0B3C5D", "#145DA0", "#1E81B0", "#2A9D8F", "#52B788", "#90BE6D",
    "#F9C74F", "#F8961E", "#F3722C", "#E85D75", "#9D4EDD",
)
KEY_RE = re.compile(
    r"^global/snapshots/perp/all/date=(\d{4}-\d{2}-\d{2})/"
    r"(\d+)_(\d+)\.parquet$"
)


@dataclass(frozen=True)
class SnapshotObject:
    snapshot_date: date
    timestamp_ms: int
    key: str
    size: int
    etag: str


@dataclass(frozen=True)
class AggregateRow:
    snapshot_date: date
    timestamp: datetime
    bucket: str
    bucket_order: int
    notional_usd: float
    share_pct: float
    position_count: int
    wallet_count: int
    total_notional_usd: float
    total_position_count: int
    total_wallet_count: int
    source: str


def leverage_bucket(leverage: object) -> tuple[str, int]:
    """Map configured leverage to the requested mutually exclusive buckets."""
    if isinstance(leverage, bool):
        raise TypeError("leverage must be a positive integer")
    try:
        value = int(leverage)  # type: ignore[call-overload]
        numeric = float(leverage)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        raise ValueError("leverage must be a positive integer") from None
    if not math.isfinite(numeric) or numeric != value or value < 1:
        raise ValueError("leverage must be a positive integer")
    if value <= 9:
        return f"{value}x", value - 1
    if value <= 25:
        return "10x–25x", 9
    return ">25x", 10


def _run_aws(arguments: Sequence[str]) -> str:
    command = ["aws", *arguments, "--region", REGION]
    try:
        completed = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        raise RuntimeError("AWS CLI is required to download Reservoir snapshots") from None
    except subprocess.CalledProcessError as error:
        detail = (error.stderr or "").strip().splitlines()
        suffix = f": {detail[-1]}" if detail else ""
        raise RuntimeError(f"AWS CLI request failed{suffix}") from None
    return completed.stdout


def list_snapshot_objects() -> list[SnapshotObject]:
    """List Reservoir objects, preserving AWS continuation tokens as opaque strings."""
    objects: list[SnapshotObject] = []
    continuation: str | None = None
    while True:
        arguments = [
            "s3api", "list-objects-v2", "--bucket", BUCKET, "--prefix", PREFIX,
            "--request-payer", "requester", "--output", "json",
        ]
        if continuation is not None:
            arguments.extend(["--continuation-token", continuation])
        try:
            payload = json.loads(_run_aws(arguments))
        except json.JSONDecodeError:
            raise RuntimeError("AWS CLI returned malformed listing JSON") from None
        if not isinstance(payload, dict):
            raise TypeError("AWS CLI returned malformed listing JSON")
        contents = payload.get("Contents", [])
        if not isinstance(contents, list):
            raise TypeError("AWS CLI listing had malformed Contents")
        for item in contents:
            if not isinstance(item, dict):
                continue
            key = item.get("Key")
            match = KEY_RE.fullmatch(key) if isinstance(key, str) else None
            if match is None:
                continue
            try:
                objects.append(
                    SnapshotObject(
                        snapshot_date=date.fromisoformat(match.group(1)),
                        timestamp_ms=int(match.group(3)),
                        key=str(key),
                        size=int(item["Size"]),
                        etag=str(item.get("ETag", "")).strip('"'),
                    )
                )
            except (KeyError, TypeError, ValueError):
                raise RuntimeError("AWS CLI listing contained malformed object metadata") from None
        if not payload.get("IsTruncated", False):
            break
        next_token = payload.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token:
            raise RuntimeError("truncated AWS listing did not include a continuation token")
        continuation = next_token
    return objects


def select_daily_snapshots(
    objects: Iterable[SnapshotObject], start: date, end: date
) -> list[SnapshotObject]:
    """Select the latest state timestamp per UTC date over an inclusive interval."""
    latest: dict[date, SnapshotObject] = {}
    for item in objects:
        if not start <= item.snapshot_date <= end:
            continue
        previous = latest.get(item.snapshot_date)
        if previous is None or (item.timestamp_ms, item.key) > (previous.timestamp_ms, previous.key):
            latest[item.snapshot_date] = item
    return [latest[value] for value in sorted(latest)]


def download_snapshots(objects: Sequence[SnapshotObject], cache_dir: Path) -> list[Path]:
    """Download selected Parquet objects from the requester-pays bucket."""
    paths: list[Path] = []
    for number, item in enumerate(objects, 1):
        destination = cache_dir / f"date={item.snapshot_date.isoformat()}" / Path(item.key).name
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists() or destination.stat().st_size != item.size:
            print(f"Downloading snapshot {number}/{len(objects)} ({item.snapshot_date})")
            _run_aws(
                [
                    "s3api", "get-object", "--bucket", BUCKET, "--key", item.key,
                    "--request-payer", "requester", str(destination),
                ]
            )
        paths.append(destination)
    return paths


def cached_snapshot_paths(cache_dir: Path, start: date, end: date) -> list[Path]:
    """Find the latest cached snapshot file per date."""
    latest: dict[date, tuple[int, Path]] = {}
    for path in cache_dir.glob("date=*/*.parquet"):
        try:
            snapshot_date = date.fromisoformat(path.parent.name.removeprefix("date="))
            timestamp_ms = int(path.stem.split("_")[-1])
        except ValueError:
            continue
        if start <= snapshot_date <= end:
            previous = latest.get(snapshot_date)
            if previous is None or (timestamp_ms, str(path)) > (previous[0], str(previous[1])):
                latest[snapshot_date] = (timestamp_ms, path)
    return [latest[value][1] for value in sorted(latest)]


def _timestamp_from_path(path: Path) -> datetime:
    try:
        timestamp_ms = int(path.stem.split("_")[-1])
        return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
    except (ValueError, OSError, OverflowError):
        raise ValueError(f"snapshot filename has no valid millisecond timestamp: {path}") from None


def aggregate_parquet(paths: Sequence[Path]) -> list[AggregateRow]:
    """Aggregate each daily snapshot without loading wallet-level rows into Python."""
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb is required; install requirements.txt") from None

    output: list[AggregateRow] = []
    connection = duckdb.connect()
    try:
        for path in paths:
            counts = connection.execute(
                """
                SELECT
                    CASE
                        WHEN leverage BETWEEN 1 AND 9 THEN leverage - 1
                        WHEN leverage BETWEEN 10 AND 25 THEN 9
                        WHEN leverage > 25 THEN 10
                    END AS bucket_order,
                    sum(abs(notional)) AS notional_usd,
                    count(*) AS position_count,
                    count(DISTINCT user) AS wallet_count
                FROM read_parquet(?)
                WHERE size != 0 AND notional IS NOT NULL AND isfinite(notional)
                      AND leverage >= 1
                GROUP BY bucket_order
                """,
                [str(path)],
            ).fetchall()
            total = connection.execute(
                """
                SELECT sum(abs(notional)), count(*), count(DISTINCT user)
                FROM read_parquet(?)
                WHERE size != 0 AND notional IS NOT NULL AND isfinite(notional)
                """,
                [str(path)],
            ).fetchone()
            if total is None or total[0] is None or float(total[0]) <= 0:
                raise ValueError(f"snapshot has no positive open-position notional: {path}")
            total_notional = float(total[0])
            by_bucket: dict[int, list[float | int | str]] = {
                index: [label, 0.0, 0, 0] for index, label in enumerate(BUCKET_LABELS)
            }
            wallet_counts: dict[int, int] = {}
            for order_value, notional, position_count, wallet_count in counts:
                if order_value is None:
                    raise ValueError(f"snapshot contains invalid leverage: {path}")
                order = int(order_value)
                values = by_bucket[order]
                values[1] = float(notional)
                values[2] = int(position_count)
                wallet_counts[order] = int(wallet_count)
            snapshot_date = date.fromisoformat(path.parent.name.removeprefix("date="))
            timestamp = _timestamp_from_path(path)
            for order, values in by_bucket.items():
                notional = float(values[1])
                output.append(
                    AggregateRow(
                        snapshot_date=snapshot_date,
                        timestamp=timestamp,
                        bucket=str(values[0]),
                        bucket_order=order,
                        notional_usd=notional,
                        share_pct=notional / total_notional * 100,
                        position_count=int(values[2]),
                        wallet_count=wallet_counts.get(order, 0),
                        total_notional_usd=total_notional,
                        total_position_count=int(total[1]),
                        total_wallet_count=int(total[2]),
                        source="reservoir",
                    )
                )
    finally:
        connection.close()
    return output


def _request_json(url: str, payload: Mapping[str, object], token: str) -> Any:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        raise RuntimeError("Hydromancer metadata request failed") from None


def _request_binary(url: str, payload: Mapping[str, object], token: str) -> tuple[bytes, str]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            return response.read(), response.headers.get("x-payload-format", "")
    except (urllib.error.URLError, TimeoutError):
        raise RuntimeError("Hydromancer perp snapshot request failed") from None


def _decode_snapshot_frames(payload: bytes, payload_format: str) -> list[Any]:
    try:
        import msgpack
        import zstandard
    except ImportError:
        raise RuntimeError("msgpack and zstandard are required; install requirements.txt") from None

    decompressor = zstandard.ZstdDecompressor()
    try:
        if payload_format == "multi-zstd":
            if len(payload) < 4:
                raise ValueError
            count = struct.unpack_from("<I", payload, 0)[0]
            offset = 4
            frames: list[Any] = []
            for _ in range(count):
                if offset + 4 > len(payload):
                    raise ValueError
                length = struct.unpack_from("<I", payload, offset)[0]
                offset += 4
                if length <= 0 or offset + length > len(payload):
                    raise ValueError
                frames.append(msgpack.unpackb(decompressor.decompress(payload[offset:offset + length])))
                offset += length
            if offset != len(payload):
                raise ValueError
            return frames
        try:
            unpacked = decompressor.decompress(payload)
        except zstandard.ZstdError:
            unpacked = payload
        return [msgpack.unpackb(unpacked)]
    except (ValueError, struct.error, zstandard.ZstdError):
        raise RuntimeError("Hydromancer perp snapshot payload was malformed") from None


def _frame_positions(frame: Any) -> tuple[Sequence[Any], Sequence[Any]]:
    if isinstance(frame, (list, tuple)) and len(frame) >= 4:
        positions, addresses = frame[2], frame[3]
    elif isinstance(frame, dict):
        positions = frame.get("p", frame.get(b"p"))
        addresses = frame.get("a", frame.get(b"a"))
    else:
        raise RuntimeError("Hydromancer perp snapshot frame was malformed")
    if not isinstance(positions, (list, tuple)) or not isinstance(addresses, (list, tuple)):
        raise TypeError("Hydromancer perp snapshot frame was malformed")
    if len(positions) != len(addresses):
        raise RuntimeError("Hydromancer perp snapshot positions/addresses were misaligned")
    return positions, addresses


def aggregate_api_snapshot(
    frames: Sequence[Any], timestamp: datetime, *, source: str = "perpSnapshot"
) -> list[AggregateRow]:
    """Aggregate decoded all-market API frames, deduplicating wallets per bucket."""
    notionals = [0.0] * len(BUCKET_LABELS)
    positions = [0] * len(BUCKET_LABELS)
    wallets: list[set[str]] = [set() for _ in BUCKET_LABELS]
    all_wallets: set[str] = set()
    for frame in frames:
        frame_positions, addresses = _frame_positions(frame)
        for position, address in zip(frame_positions, addresses, strict=True):
            if not isinstance(position, (list, tuple)) or len(position) < 8:
                raise RuntimeError("Hydromancer perp snapshot position tuple was malformed")
            try:
                size = float(position[0])
                notional = abs(float(position[1]))
                _, order = leverage_bucket(position[5])
            except (TypeError, ValueError, OverflowError):
                raise RuntimeError("Hydromancer perp snapshot position tuple was malformed") from None
            if not math.isfinite(size) or not math.isfinite(notional):
                raise RuntimeError("Hydromancer perp snapshot had non-finite position values")
            if size == 0:
                continue
            wallet = address.decode() if isinstance(address, bytes) else str(address)
            notionals[order] += notional
            positions[order] += 1
            wallets[order].add(wallet)
            all_wallets.add(wallet)
    total_notional = sum(notionals)
    if total_notional <= 0:
        raise RuntimeError("Hydromancer perp snapshot had no positive open-position notional")
    total_positions = sum(positions)
    return [
        AggregateRow(
            snapshot_date=timestamp.date(),
            timestamp=timestamp,
            bucket=label,
            bucket_order=order,
            notional_usd=notionals[order],
            share_pct=notionals[order] / total_notional * 100,
            position_count=positions[order],
            wallet_count=len(wallets[order]),
            total_notional_usd=total_notional,
            total_position_count=total_positions,
            total_wallet_count=len(all_wallets),
            source=source,
        )
        for order, label in enumerate(BUCKET_LABELS)
    ]


def fetch_current_api_snapshot(token: str, base_url: str) -> list[AggregateRow]:
    """Fetch one current all-DEX snapshot, polling metadata before the heavy endpoint."""
    endpoint = f"{base_url.rstrip('/')}/info"
    metadata = _request_json(endpoint, {"type": "perpSnapshotTimestamp"}, token)
    if not isinstance(metadata, dict):
        raise TypeError("Hydromancer snapshot metadata was malformed")
    timestamp_value = metadata.get("timestamp")
    if not isinstance(timestamp_value, (int, float)) or isinstance(timestamp_value, bool):
        raise TypeError("Hydromancer snapshot metadata had no numeric timestamp")
    seconds = float(timestamp_value)
    if seconds > 100_000_000_000:
        seconds /= 1000
    try:
        timestamp = datetime.fromtimestamp(seconds, tz=UTC)
    except (OSError, OverflowError, ValueError):
        raise RuntimeError("Hydromancer snapshot metadata timestamp was invalid") from None
    payload, payload_format = _request_binary(
        endpoint,
        {"type": "perpSnapshots", "market_names": ["ALL:ALL_DEXES"]},
        token,
    )
    return aggregate_api_snapshot(_decode_snapshot_frames(payload, payload_format), timestamp)


def prefer_latest_per_date(rows: Sequence[AggregateRow]) -> list[AggregateRow]:
    """Avoid double counting when Reservoir and API points share a UTC date."""
    snapshots: dict[tuple[date, datetime, str], list[AggregateRow]] = defaultdict(list)
    for row in rows:
        snapshots[(row.snapshot_date, row.timestamp, row.source)].append(row)
    selected: dict[date, tuple[datetime, int, list[AggregateRow]]] = {}
    for (snapshot_date, timestamp, source), snapshot_rows in snapshots.items():
        # Prefer the API only if exact timestamps happen to tie.
        candidate = (timestamp, int(source == "perpSnapshot"), snapshot_rows)
        previous = selected.get(snapshot_date)
        if previous is None or candidate[:2] > previous[:2]:
            selected[snapshot_date] = candidate
    return sorted(
        [row for _, _, snapshot_rows in selected.values() for row in snapshot_rows],
        key=lambda row: (row.timestamp, row.bucket_order),
    )


def validate_aggregates(rows: Sequence[AggregateRow]) -> None:
    grouped: dict[datetime, list[AggregateRow]] = defaultdict(list)
    for row in rows:
        grouped[row.timestamp].append(row)
    if not grouped:
        raise ValueError("no aggregate rows")
    for timestamp, daily in grouped.items():
        if sorted(row.bucket_order for row in daily) != list(range(len(BUCKET_LABELS))):
            raise ValueError(f"snapshot {timestamp.isoformat()} does not have every bucket")
        share_sum = sum(row.share_pct for row in daily)
        notional_sum = sum(row.notional_usd for row in daily)
        if not math.isclose(share_sum, 100.0, abs_tol=1e-8):
            raise ValueError(f"snapshot {timestamp.isoformat()} does not total 100%")
        if not math.isclose(notional_sum, daily[0].total_notional_usd, rel_tol=1e-12):
            raise ValueError(f"snapshot {timestamp.isoformat()} notional does not reconcile")


def write_csv(path: Path, rows: Sequence[AggregateRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [field.name for field in AggregateRow.__dataclass_fields__.values()]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            values = {field: getattr(row, field) for field in fields}
            values["snapshot_date"] = row.snapshot_date.isoformat()
            values["timestamp"] = row.timestamp.isoformat()
            writer.writerow(values)


def build_figure(rows: Sequence[AggregateRow]):  # type: ignore[no-untyped-def]
    """Build absolute USD-million and normalized 100% stacked daily bars."""
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except ImportError:
        raise RuntimeError("plotly is required; install requirements.txt") from None

    figure = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.11,
        subplot_titles=("Gross open-position notional", "Share of gross open-position notional"),
    )
    for order, (label, color) in enumerate(zip(BUCKET_LABELS, COLORS, strict=True)):
        selected = [row for row in rows if row.bucket_order == order]
        x = [row.timestamp for row in selected]
        custom = [
            [
                row.share_pct,
                row.notional_usd,
                row.position_count,
                row.wallet_count,
                row.total_notional_usd,
                row.total_position_count,
                row.total_wallet_count,
                row.source,
            ]
            for row in selected
        ]
        hover = (
            "%{x|%Y-%m-%d %H:%M UTC}<br><b>%{fullData.name}</b><br>"
            "Notional: $%{customdata[1]:,.0f}<br>Share: %{customdata[0]:.2f}%<br>"
            "Positions: %{customdata[2]:,}<br>Wallets in bucket: %{customdata[3]:,}<br>"
            "All notional: $%{customdata[4]:,.0f}<br>All positions: %{customdata[5]:,}<br>"
            "All wallets: %{customdata[6]:,}<br>Source: %{customdata[7]}<extra></extra>"
        )
        figure.add_trace(
            go.Bar(
                name=label,
                x=x,
                y=[row.notional_usd / 1_000_000 for row in selected],
                marker_color=color,
                customdata=custom,
                hovertemplate=hover,
                legendgroup=label,
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Bar(
                name=label,
                x=x,
                y=[row.share_pct for row in selected],
                marker_color=color,
                customdata=custom,
                hovertemplate=hover,
                legendgroup=label,
                showlegend=False,
            ),
            row=2,
            col=1,
        )
    figure.update_layout(
        title=(
            "Hyperliquid wallet leverage decomposition — daily open positions"
            "<br><sup>Gross notional = sum(abs(position notional)); configured leverage buckets</sup>"
        ),
        barmode="stack",
        template="plotly_white",
        hovermode="x unified",
        height=850,
        legend={"orientation": "h", "y": 1.09, "x": 0},
        bargap=0.12,
    )
    figure.update_yaxes(title_text="Notional (USD millions)", rangemode="tozero", row=1, col=1)
    figure.update_yaxes(title_text="Share (%)", range=[0, 100], ticksuffix="%", row=2, col=1)
    figure.update_xaxes(title_text="Snapshot time (UTC)", row=2, col=1)
    return figure


def write_html(path: Path, figure: Any) -> None:
    import plotly.io as pio

    path.parent.mkdir(parents=True, exist_ok=True)
    pio.write_html(
        figure,
        file=path,
        include_plotlyjs=True,
        full_html=True,
        auto_open=False,
        div_id="hydromancer-wallet-leverage-decomposition",
    )


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=parse_date, default=EARLIEST_SNAPSHOT_DATE)
    parser.add_argument("--end", type=parse_date, default=datetime.now(UTC).date())
    parser.add_argument("--download", action="store_true", help="list and download Reservoir data")
    parser.add_argument("--include-current-api", action="store_true")
    parser.add_argument("--cache-dir", type=Path, default=Path(__file__).parent / "data" / "reservoir")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "output")
    parser.add_argument(
        "--api-base-url",
        default=os.getenv("HYDROMANCER_API_URL", "https://api.hydromancer.xyz"),
    )
    return parser


def main(arguments: Sequence[str] | None = None) -> int:
    options = build_parser().parse_args(arguments)
    if options.start < EARLIEST_SNAPSHOT_DATE:
        print(
            f"warning: Reservoir position snapshots start in August 2025; "
            f"using {EARLIEST_SNAPSHOT_DATE}",
            file=sys.stderr,
        )
        options.start = EARLIEST_SNAPSHOT_DATE
    if options.start > options.end:
        raise SystemExit("--start must not be after --end")

    if options.download:
        selected = select_daily_snapshots(list_snapshot_objects(), options.start, options.end)
        if not selected:
            raise SystemExit("no Reservoir snapshots found in the requested interval")
        paths = download_snapshots(selected, options.cache_dir)
    else:
        paths = cached_snapshot_paths(options.cache_dir, options.start, options.end)
        if not paths:
            raise SystemExit("no cached snapshots found; rerun with --download")

    rows = aggregate_parquet(paths)
    if options.include_current_api:
        token = os.getenv("HYDROMANCER_API_KEY")
        if not token:
            raise SystemExit("HYDROMANCER_API_KEY is required with --include-current-api")
        rows.extend(fetch_current_api_snapshot(token, options.api_base_url))
    rows = prefer_latest_per_date(rows)
    validate_aggregates(rows)

    csv_path = options.output_dir / "daily_leverage_decomposition.csv"
    html_path = options.output_dir / "daily_leverage_decomposition.html"
    write_csv(csv_path, rows)
    write_html(html_path, build_figure(rows))
    dates = sorted({row.snapshot_date for row in rows})
    print(f"Wrote {csv_path}")
    print(f"Wrote {html_path}")
    print(f"Coverage: {dates[0]} through {dates[-1]} ({len(dates)} available daily snapshots)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
