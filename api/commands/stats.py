"""Descriptive statistics export command."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import cast

import pandas as pd

from ingestion.lake import load_combined_dataframe_from_lake


def add_export_descriptive_stats_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``export-descriptive-stats`` parser."""

    parser = subparsers.add_parser(
        "export-descriptive-stats",
        help="Export reproducible OHLCV descriptive statistics from parquet lake",
    )
    parser.add_argument("--lake-root", default="lake/bronze", help="Root directory for parquet lake files")
    parser.add_argument("--output-csv", default="docs/tables/descriptive_stats_baseline.csv")
    parser.add_argument("--start-time", default="2026-01-01T00:00:00+00:00")
    parser.add_argument("--end-time", default="2026-01-31T23:59:59+00:00")
    parser.add_argument("--exchanges", nargs="+", choices=["deribit"])
    parser.add_argument("--symbols", nargs="+", help="Optional symbol filter")
    parser.add_argument("--timeframes", nargs="+", help="Optional timeframe filter")
    parser.add_argument(
        "--instrument-types",
        nargs="+",
        choices=["spot", "perp"],
        default=["spot", "perp"],
    )
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_export_descriptive_stats(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Export deterministic descriptive stats table from parquet-lake OHLCV rows."""

    start_time = datetime.fromisoformat(cast(str, args.start_time))
    end_time = datetime.fromisoformat(cast(str, args.end_time))
    if start_time.tzinfo is None or end_time.tzinfo is None:
        raise ValueError("start-time and end-time must include timezone offset (for example +00:00)")
    if start_time > end_time:
        raise ValueError("start-time must be <= end-time")

    dataframe = load_combined_dataframe_from_lake(
        lake_root=cast(str, args.lake_root),
        exchanges=cast(list[str] | None, args.exchanges),
        symbols=cast(list[str] | None, args.symbols),
        timeframes=cast(list[str] | None, args.timeframes),
        instrument_types=cast(list[str] | None, args.instrument_types),
        start_time=start_time,
        end_time=end_time,
        include_open_interest=False,
    )

    stats_variables = ["open", "high", "low", "close", "volume"]
    records: list[dict[str, object]] = []
    for variable in stats_variables:
        if variable not in dataframe.columns:
            records.append({"Variable": variable, "Mean": None, "Std": None, "Min": None, "Max": None})
            continue
        series = dataframe[variable].astype("float64")
        records.append(
            {
                "Variable": variable,
                "Mean": float(series.mean()) if len(series) else None,
                "Std": float(series.std()) if len(series) else None,
                "Min": float(series.min()) if len(series) else None,
                "Max": float(series.max()) if len(series) else None,
            }
        )

    stats_df = pd.DataFrame(records, columns=["Variable", "Mean", "Std", "Min", "Max"])
    output_path = Path(cast(str, args.output_csv))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats_df.to_csv(output_path, index=False)

    result = {
        "output_csv": str(output_path.resolve()),
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "row_count": int(len(dataframe)),
        "variables": stats_variables,
    }
    if not bool(args.no_json_output):
        print(json.dumps(result, indent=2))
    logger.info(
        "Command complete: export-descriptive-stats rows=%s output=%s",
        len(dataframe),
        output_path,
    )
