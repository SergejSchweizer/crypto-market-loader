"""Offline parquet-to-TimescaleDB ingest command."""

from __future__ import annotations

import argparse
import json
import logging
from typing import cast

from infra.timescaledb import save_parquet_lake_to_timescaledb


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
    parser.add_argument("--timeframes", nargs="+", help="Optional timeframe filter")
    parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        help="Optional instrument filter",
    )
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_ingest_timescaledb(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run ``ingest-timescaledb`` command."""

    summary = save_parquet_lake_to_timescaledb(
        lake_root=cast(str, args.lake_root),
        schema=cast(str, args.timescaledb_schema),
        create_schema=not bool(args.timescaledb_no_bootstrap),
        exchanges=cast(list[str] | None, args.exchanges),
        symbols=cast(list[str] | None, args.symbols),
        timeframes=cast(list[str] | None, args.timeframes),
        instrument_types=cast(list[str] | None, args.instrument_types),
    )
    if not bool(args.no_json_output):
        print(json.dumps(summary, indent=2))
    logger.info(
        "Command complete: ingest-timescaledb ohlcv_rows=%s open_interest_rows=%s",
        summary["ohlcv_rows"],
        summary["open_interest_rows"],
    )
