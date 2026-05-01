"""Offline parquet-to-TimescaleDB ingest command."""

from __future__ import annotations

import argparse
import json
import logging
from typing import cast

from infra.timescaledb import save_parquet_lake_to_timescaledb

_ALLOWED_TIMEFRAME_VALUES = {"1m", "m1"}


def add_ingest_timescaledb_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``ingest-timescaledb`` parser."""

    parser = subparsers.add_parser(
        "ingest-timescaledb",
        help="Read existing parquet lake files and ingest them into TimescaleDB (no exchange fetch)",
    )
    parser.add_argument("--lake-root", default="lake/bronze", help="Root directory for parquet lake files")
    parser.add_argument("--timescaledb-schema", default="market_data", help="Target TimescaleDB schema")
    parser.add_argument(
        "--timescaledb-no-bootstrap",
        action="store_true",
        help="Skip TimescaleDB schema/table bootstrap and write into existing tables",
    )
    parser.add_argument("--exchanges", nargs="+", choices=["deribit"])
    parser.add_argument("--symbols", nargs="+", help="Optional symbol filter")
    parser.add_argument("--timeframes", nargs="+", default=["1m"], help="Timeframe filter (default: 1m)")
    parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        help="Optional instrument filter",
    )
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_ingest_timescaledb(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run ``ingest-timescaledb`` command."""

    requested_timeframes = cast(list[str] | None, args.timeframes)
    if requested_timeframes:
        invalid_values = [
            value for value in requested_timeframes if value.strip().lower() not in _ALLOWED_TIMEFRAME_VALUES
        ]
        if invalid_values:
            raise ValueError(
                "ingest-timescaledb supports only 1m timeframe. Invalid values: " + ", ".join(invalid_values)
            )

    def _progress(info: dict[str, str]) -> None:
        logger.info(
            "Ingest progress dataset=%s symbol=%s timeframe=%s time_range=%s exchange=%s instrument_type=%s",
            info.get("dataset", "unknown"),
            info.get("symbol", "unknown"),
            info.get("timeframe", "unknown"),
            info.get("time_range", "unknown"),
            info.get("exchange", "unknown"),
            info.get("instrument_type", "unknown"),
        )

    summary = save_parquet_lake_to_timescaledb(
        lake_root=cast(str, args.lake_root),
        schema=cast(str, args.timescaledb_schema),
        create_schema=not bool(args.timescaledb_no_bootstrap),
        exchanges=cast(list[str] | None, args.exchanges),
        symbols=cast(list[str] | None, args.symbols),
        timeframes=cast(list[str] | None, args.timeframes),
        instrument_types=cast(list[str] | None, args.instrument_types),
        progress_callback=_progress,
    )
    if not bool(args.no_json_output):
        print(json.dumps(summary, indent=2))
    logger.info(
        (
            "Command complete: ingest-timescaledb "
            "spot_rows=%s perp_rows=%s ohlcv_rows=%s oi_rows=%s funding_rows=%s "
            "spot_files=%s perp_files=%s ohlcv_files=%s oi_files=%s funding_files=%s "
            "spot_skipped_rows=%s perp_skipped_rows=%s ohlcv_skipped_rows=%s oi_skipped_rows=%s funding_skipped_rows=%s"
        ),
        summary.get("spot_rows", 0),
        summary.get("perp_rows", 0),
        summary["ohlcv_rows"],
        summary["oi_rows"],
        summary["funding_rows"],
        summary.get("spot_files", 0),
        summary.get("perp_files", 0),
        summary.get("ohlcv_files", 0),
        summary.get("oi_files", 0),
        summary.get("funding_files", 0),
        summary.get("spot_skipped_rows", 0),
        summary.get("perp_skipped_rows", 0),
        summary.get("ohlcv_skipped_rows", 0),
        summary.get("oi_skipped_rows", 0),
        summary.get("funding_skipped_rows", 0),
    )
