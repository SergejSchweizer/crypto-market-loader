"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
from typing import Any, cast

from api.commands import loader as loader_cmd
from api.commands import stats as stats_cmd
from api.commands.loader import add_loader_parser
from api.commands.stats import add_export_descriptive_stats_parser, run_export_descriptive_stats
from api.commands.timeframes import add_list_spot_timeframes_parser, run_list_spot_timeframes
from api.commands.timescaledb_ingest import add_ingest_timescaledb_parser, run_ingest_timescaledb
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from application.services.runtime_service import (
    SingleInstanceError,
    SingleInstanceLock,
    configure_logging,
    fetch_concurrency,
    load_env_file,
)
from infra.timescaledb import save_market_data_to_timescaledb
from ingestion.funding import (
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import (
    latest_open_time_in_lake,
    latest_open_time_in_lake_by_dataset,
    load_combined_dataframe_from_lake,  # noqa: F401 - backward-compatible test monkeypatch surface
    open_times_in_lake,
    open_times_in_lake_by_dataset,
)
from ingestion.open_interest import (
    fetch_open_interest_all_history,
    fetch_open_interest_range,
    normalize_open_interest_timeframe,
    open_interest_interval_to_milliseconds,
)
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    normalize_storage_symbol,
)

__all__ = ["SingleInstanceError", "SingleInstanceLock", "build_parser", "main"]
_TAIL_DELTA_ONLY = True


# Backward-compatible wrappers used by tests.
def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[SpotCandle]:
    _sync_loader_runtime_overrides()
    return loader_cmd._fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
    )


def _write_loader_samples(*args: Any, **kwargs: Any) -> None:
    loader_cmd._write_loader_samples(*args, **kwargs)


def _sync_loader_runtime_overrides() -> None:
    """Mirror runtime symbols into loader module to preserve monkeypatch behavior."""

    loader_any = cast(Any, loader_cmd)
    loader_any.SingleInstanceLock = SingleInstanceLock
    loader_any.SingleInstanceError = SingleInstanceError
    loader_any.fetch_concurrency = fetch_concurrency
    loader_any._last_closed_open_ms = _last_closed_open_ms
    loader_any._missing_ranges_ms = _missing_ranges_ms
    loader_any.open_times_in_lake = open_times_in_lake
    loader_any.open_times_in_lake_by_dataset = open_times_in_lake_by_dataset
    loader_any.latest_open_time_in_lake = latest_open_time_in_lake
    loader_any.latest_open_time_in_lake_by_dataset = latest_open_time_in_lake_by_dataset
    loader_any._TAIL_DELTA_ONLY = _TAIL_DELTA_ONLY
    loader_any.normalize_storage_symbol = normalize_storage_symbol
    loader_any.interval_to_milliseconds = interval_to_milliseconds
    loader_any.open_interest_interval_to_milliseconds = open_interest_interval_to_milliseconds
    loader_any.funding_interval_to_milliseconds = funding_interval_to_milliseconds
    loader_any.normalize_open_interest_timeframe = normalize_open_interest_timeframe
    loader_any.normalize_funding_timeframe = normalize_funding_timeframe
    loader_any.fetch_candles_all_history = fetch_candles_all_history
    loader_any.fetch_candles_range = fetch_candles_range
    loader_any.fetch_open_interest_all_history = fetch_open_interest_all_history
    loader_any.fetch_open_interest_range = fetch_open_interest_range
    loader_any.fetch_funding_all_history = fetch_funding_all_history
    loader_any.fetch_funding_range = fetch_funding_range
    loader_any.save_market_data_to_timescaledb = save_market_data_to_timescaledb


def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="crypto-market-loader CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_loader_parser(subparsers)
    add_list_spot_timeframes_parser(subparsers)
    add_ingest_timescaledb_parser(subparsers)
    add_export_descriptive_stats_parser(subparsers)

    return parser


def main() -> None:
    """CLI entrypoint."""

    load_env_file()
    parser = build_parser()
    args = parser.parse_args()
    logger = configure_logging(module_name=str(args.command))
    logger.info("Command start: %s", args.command)

    if args.command == "loader":
        _sync_loader_runtime_overrides()
        loader_cmd.run_loader(args=args, logger=logger)
    elif args.command == "list-spot-timeframes":
        run_list_spot_timeframes(args=args, logger=logger)
    elif args.command == "ingest-timescaledb":
        run_ingest_timescaledb(args=args, logger=logger)
    elif args.command == "export-descriptive-stats":
        cast(Any, stats_cmd).load_combined_dataframe_from_lake = load_combined_dataframe_from_lake
        run_export_descriptive_stats(args=args, logger=logger)


if __name__ == "__main__":
    main()
