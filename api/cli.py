"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
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
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to YAML configuration file for command defaults",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_loader_parser(subparsers)
    add_list_spot_timeframes_parser(subparsers)
    add_ingest_timescaledb_parser(subparsers)
    add_export_descriptive_stats_parser(subparsers)

    return parser


def _load_yaml_config(path: str) -> dict[str, object]:
    """Load YAML config file into a dictionary."""

    config_path = Path(path)
    if not config_path.exists():
        return {}
    try:
        import yaml
    except ImportError:
        # Keep CLI usable in minimal environments; YAML config is optional.
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("config file must contain a top-level mapping")
    return cast(dict[str, object], loaded)


def _subparser_for_command(parser: argparse.ArgumentParser, command: str) -> argparse.ArgumentParser | None:
    """Return subparser object for selected command."""

    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):  # type: ignore[attr-defined]
            candidate = action.choices.get(command)
            if isinstance(candidate, argparse.ArgumentParser):
                return candidate
    return None


def _collect_explicit_cli_dests(command_parser: argparse.ArgumentParser, argv: list[str]) -> set[str]:
    """Collect argparse destination names explicitly provided via CLI flags."""

    provided: set[str] = set()
    option_to_dest: dict[str, str] = {}
    for action in command_parser._actions:
        for option in action.option_strings:
            option_to_dest[option] = action.dest
    for token in argv:
        if token == "--":
            break
        if token.startswith("--"):
            option_name = token.split("=", 1)[0]
            dest = option_to_dest.get(option_name)
            if dest:
                provided.add(dest)
    return provided


def _apply_yaml_defaults(
    args: argparse.Namespace,
    command: str,
    config: dict[str, object],
    explicit_dests: set[str],
) -> None:
    """Apply global and command-level YAML defaults unless overridden by CLI."""

    global_config = config.get("global")
    if isinstance(global_config, dict):
        for key, value in global_config.items():
            if key in explicit_dests or not hasattr(args, key):
                continue
            setattr(args, key, value)
    command_config = config.get(command)
    if isinstance(command_config, dict):
        for key, value in command_config.items():
            if key in explicit_dests or not hasattr(args, key):
                continue
            setattr(args, key, value)


def main() -> None:
    """CLI entrypoint."""

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default="config.yaml")
    pre_parser.add_argument("command", nargs="?")
    pre_args, _ = pre_parser.parse_known_args(sys.argv[1:])
    config_data = _load_yaml_config(pre_args.config)

    parser = build_parser()
    args = parser.parse_args()
    command = cast(str, args.command)
    command_parser = _subparser_for_command(parser, command)
    if command_parser is not None:
        explicit = _collect_explicit_cli_dests(command_parser, sys.argv[1:])
        _apply_yaml_defaults(args=args, command=command, config=config_data, explicit_dests=explicit)
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
