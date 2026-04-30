"""Main loader command implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import asdict
from datetime import datetime
from typing import Literal, cast

from application.dto import (
    ArtifactOptionsDTO,
    CandleFetchTaskDTO,
    FundingFetchTaskDTO,
    LoaderStorageDTO,
    OpenInterestFetchTaskDTO,
    PersistOptionsDTO,
)
from application.services.artifact_service import write_loader_samples_dto
from application.services.fetch_service import (
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_funding,
    fetch_symbol_open_interest,
)
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock, fetch_concurrency
from application.services.storage_service import persist_loader_outputs_dto
from infra.timescaledb import save_market_data_to_timescaledb
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import (
    load_funding_from_lake,
    load_open_interest_from_lake,
    load_spot_candles_from_lake,
    open_times_in_lake,
    open_times_in_lake_by_dataset,
)
from ingestion.open_interest import (
    OpenInterestPoint,
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
    normalize_timeframe,
)

DataType = Literal["spot", "perp", "oi", "funding"]


def _env_concurrency(name: str, default: int) -> int:
    """Read bounded concurrency value from environment with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return max(1, default)
    try:
        value = int(raw)
    except ValueError:
        return max(1, default)
    return max(1, value)


def add_loader_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``loader`` parser."""

    parser = subparsers.add_parser("loader", help="Fetch candles from supported exchanges")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["deribit"],
        help="Optional list of exchanges to fetch in one run",
    )
    parser.add_argument(
        "--market",
        nargs="+",
        choices=["spot", "perp", "oi", "funding"],
        default=["spot"],
        help="One or more data types to fetch, e.g. --market spot perp oi funding",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols or instrument aliases (exchange specific)",
    )
    parser.add_argument(
        "--timeframe",
        "--interval",
        dest="timeframe",
        default="1h",
        help="Candle timeframe, e.g. M1, M5, H1, D1, 1m, 1h, 1d",
    )
    parser.add_argument(
        "--timeframes",
        nargs="+",
        help="Optional list of timeframes. When set, fetch runs for each timeframe sequentially.",
    )
    parser.add_argument("--plot", action="store_true", help="Create and save price/volume plots")
    parser.add_argument(
        "--plot-price",
        choices=["spot", "close", "open", "high", "low"],
        default="close",
        help="Price field to plot (spot maps to close)",
    )
    parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    parser.add_argument(
        "--save-timescaledb",
        action="store_true",
        help="Save fetched candles/open-interest rows to TimescaleDB",
    )
    parser.add_argument(
        "--timescaledb-schema",
        default="market_data",
        help="Target TimescaleDB schema for market tables",
    )
    parser.add_argument(
        "--timescaledb-no-bootstrap",
        action="store_true",
        help="Skip TimescaleDB schema/table bootstrap and write into existing tables",
    )
    parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from loader command",
    )


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[SpotCandle]:
    return fetch_symbol_candles(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_candles_all_history,
        range_fetcher=fetch_candles_range,
    )


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[OpenInterestPoint]:
    return fetch_symbol_open_interest(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_open_interest_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=open_interest_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_open_interest_all_history,
        range_fetcher=fetch_open_interest_range,
    )


def _fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
) -> list[FundingPoint]:
    return fetch_symbol_funding(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        lake_root=lake_root,
        open_times_reader=open_times_in_lake_by_dataset,
        timeframe_normalizer=normalize_funding_timeframe,
        symbol_normalizer=normalize_storage_symbol,
        interval_ms_resolver=funding_interval_to_milliseconds,
        now_open_resolver=_last_closed_open_ms,
        ranges_builder=_missing_ranges_ms,
        history_fetcher=fetch_funding_all_history,
        range_fetcher=fetch_funding_range,
    )


async def _fetch_candle_tasks_parallel(
    tasks: list[tuple[Exchange, Market, str, str]], lake_root: str, concurrency: int, logger: logging.Logger
) -> tuple[dict[tuple[Exchange, Market, str, str], list[SpotCandle]], dict[tuple[Exchange, Market, str, str], str]]:
    service_tasks = [
        CandleFetchTaskDTO(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
        for exchange, market, symbol, timeframe in tasks
    ]
    result = await fetch_candle_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_candles,
    )
    return result.rows, result.errors


async def _fetch_open_interest_tasks_parallel(
    oi_tasks: list[tuple[Exchange, str, str]], lake_root: str, concurrency: int, logger: logging.Logger
) -> tuple[dict[tuple[Exchange, str, str], list[OpenInterestPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        OpenInterestFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in oi_tasks
    ]
    result = await fetch_open_interest_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_open_interest,
    )
    return result.rows, result.errors


async def _fetch_funding_tasks_parallel(
    funding_tasks: list[tuple[Exchange, str, str]], lake_root: str, concurrency: int, logger: logging.Logger
) -> tuple[dict[tuple[Exchange, str, str], list[FundingPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        FundingFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in funding_tasks
    ]
    result = await fetch_funding_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_funding,
    )
    return result.rows, result.errors


async def _fetch_all_task_groups(
    tasks: list[tuple[Exchange, Market, str, str]],
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    candle_concurrency: int,
    oi_concurrency: int,
    funding_concurrency: int,
    logger: logging.Logger,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], list[FundingPoint]],
    dict[tuple[Exchange, str, str], str],
]:
    """Fetch OHLCV, OI, and funding task groups concurrently."""

    candle_job = _fetch_candle_tasks_parallel(
        tasks=tasks,
        lake_root=lake_root,
        concurrency=candle_concurrency,
        logger=logger,
    )
    oi_job = (
        _fetch_open_interest_tasks_parallel(
            oi_tasks=oi_tasks,
            lake_root=lake_root,
            concurrency=oi_concurrency,
            logger=logger,
        )
        if oi_tasks
        else asyncio.sleep(0, result=({}, {}))
    )
    funding_job = (
        _fetch_funding_tasks_parallel(
            funding_tasks=funding_tasks,
            lake_root=lake_root,
            concurrency=funding_concurrency,
            logger=logger,
        )
        if funding_tasks
        else asyncio.sleep(0, result=({}, {}))
    )
    (task_results, task_errors), (oi_results, oi_errors), (funding_results, funding_errors) = await asyncio.gather(
        candle_job,
        oi_job,
        funding_job,
    )
    return task_results, task_errors, oi_results, oi_errors, funding_results, funding_errors


def _write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    generate_plots: bool = True,
) -> None:
    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
        ),
        logger=logger,
        options=ArtifactOptionsDTO(generate_plots=generate_plots),
    )


def run_loader(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run loader command."""

    try:
        with SingleInstanceLock(".run/crypto-market-loader.lock"):
            exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
            data_types = cast(list[DataType], args.market)
            ohlcv_markets = [item for item in data_types if item in {"spot", "perp"}]
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            multi_market = len(data_types) > 1
            requested_timeframes = cast(list[str], args.timeframes if args.timeframes else [args.timeframe])
            multi_timeframe = len(requested_timeframes) > 1
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []

            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
                normalized_timeframes: list[str] = []
                for timeframe_value in requested_timeframes:
                    try:
                        normalized_timeframes.append(normalize_timeframe(exchange=exchange, value=timeframe_value))
                    except Exception as exc:  # noqa: BLE001
                        exchange_output[f"_timeframe_error_{timeframe_value}"] = str(exc)
                        logger.exception(
                            "Failed to normalize timeframe exchange=%s timeframe=%s",
                            exchange,
                            timeframe_value,
                        )
                if not normalized_timeframes:
                    continue
                for market in cast(list[Market], ohlcv_markets):
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            tasks.append((exchange, market, symbol, timeframe))
                if oi_requested:
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            oi_tasks.append((exchange, symbol, timeframe))
                if funding_requested:
                    for timeframe in normalized_timeframes:
                        for symbol in args.symbols:
                            funding_tasks.append((exchange, symbol, timeframe))

            base_concurrency = fetch_concurrency()
            candle_concurrency = _env_concurrency("LOADER_OHLCV_CONCURRENCY", base_concurrency)
            oi_concurrency = _env_concurrency("LOADER_OI_CONCURRENCY", base_concurrency)
            funding_concurrency = _env_concurrency("LOADER_FUNDING_CONCURRENCY", base_concurrency)
            logger.info(
                "Parallel fetch enabled with asyncio concurrency ohlcv=%s oi=%s funding=%s base=%s",
                candle_concurrency,
                oi_concurrency,
                funding_concurrency,
                base_concurrency,
            )
            task_results, task_errors, oi_results, oi_errors, funding_results, funding_errors = asyncio.run(
                _fetch_all_task_groups(
                    tasks=tasks,
                    oi_tasks=oi_tasks,
                    funding_tasks=funding_tasks,
                    lake_root=cast(str, args.lake_root),
                    candle_concurrency=candle_concurrency,
                    oi_concurrency=oi_concurrency,
                    funding_concurrency=funding_concurrency,
                    logger=logger,
                )
            )

            for exchange, market, symbol, timeframe in tasks:
                exchange_output = cast(dict[str, object], output[exchange])
                symbol_key = symbol.upper()
                result_key = (exchange, market, symbol, timeframe)
                if multi_market:
                    market_bucket = cast(dict[str, object], exchange_output.setdefault(market, {}))
                else:
                    market_bucket = exchange_output
                if multi_timeframe:
                    timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                else:
                    timeframe_bucket = market_bucket
                if result_key in task_errors:
                    timeframe_bucket[symbol_key] = {"error": task_errors[result_key]}
                    continue
                candles = task_results.get(result_key, [])
                timeframe_bucket[symbol_key] = [_serialize_candle(item) for item in candles]
                by_market = candles_for_storage.setdefault(market, {})
                exchange_candles = by_market.setdefault(exchange, {})
                if multi_market and multi_timeframe:
                    plot_key = f"{market}_{symbol_key}__{timeframe}"
                elif multi_market:
                    plot_key = f"{market}_{symbol_key}"
                elif multi_timeframe:
                    plot_key = f"{symbol_key}__{timeframe}"
                else:
                    plot_key = symbol_key
                exchange_candles[plot_key] = candles

            if oi_requested:
                for exchange, symbol, timeframe in oi_tasks:
                    symbol_key = symbol.upper()
                    oi_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("oi", {}))
                    else:
                        market_bucket = exchange_output
                    if multi_timeframe:
                        timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                    else:
                        timeframe_bucket = market_bucket
                    if oi_key in oi_errors:
                        timeframe_bucket[symbol_key] = {"error": oi_errors[oi_key]}
                        continue
                    oi_rows = oi_results.get(oi_key, [])
                    timeframe_bucket[symbol_key] = [
                        {
                            "exchange": item.exchange,
                            "symbol": item.symbol,
                            "interval": item.interval,
                            "open_time": item.open_time.isoformat(),
                            "close_time": item.close_time.isoformat(),
                            "open_interest": item.open_interest,
                            "open_interest_value": item.open_interest_value,
                        }
                        for item in oi_rows
                    ]
                    oi_by_market = open_interest_for_storage.setdefault("perp", {})
                    oi_exchange_rows = oi_by_market.setdefault(exchange, {})
                    if multi_timeframe:
                        oi_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        oi_plot_key = symbol_key
                    oi_exchange_rows[oi_plot_key] = oi_rows

            if funding_requested:
                for exchange, symbol, timeframe in funding_tasks:
                    symbol_key = symbol.upper()
                    funding_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("funding", {}))
                    else:
                        market_bucket = exchange_output
                    if multi_timeframe:
                        timeframe_bucket = cast(dict[str, object], market_bucket.setdefault(timeframe, {}))
                    else:
                        timeframe_bucket = market_bucket
                    if funding_key in funding_errors:
                        timeframe_bucket[symbol_key] = {"error": funding_errors[funding_key]}
                        continue
                    funding_rows = funding_results.get(funding_key, [])
                    timeframe_bucket[symbol_key] = [
                        {
                            "exchange": item.exchange,
                            "symbol": item.symbol,
                            "interval": item.interval,
                            "open_time": item.open_time.isoformat(),
                            "close_time": item.close_time.isoformat(),
                            "funding_rate": item.funding_rate,
                            "index_price": item.index_price,
                            "mark_price": item.mark_price,
                        }
                        for item in funding_rows
                    ]
                    funding_by_market = funding_for_storage.setdefault("perp", {})
                    funding_exchange_rows = funding_by_market.setdefault(exchange, {})
                    if multi_timeframe:
                        funding_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        funding_plot_key = symbol_key
                    funding_exchange_rows[funding_plot_key] = funding_rows

            if args.save_parquet_lake:
                try:
                    storage_result = persist_loader_outputs_dto(
                        storage=LoaderStorageDTO(
                            candles=candles_for_storage,
                            open_interest=open_interest_for_storage,
                            funding=funding_for_storage,
                        ),
                        options=PersistOptionsDTO(
                            save_parquet_lake=True,
                            save_timescaledb=False,
                            lake_root=cast(str, args.lake_root),
                            timescaledb_schema=cast(str, args.timescaledb_schema),
                            create_schema=not bool(args.timescaledb_no_bootstrap),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                        ),
                        save_tsdb_fn=save_market_data_to_timescaledb,
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_parquet_error"] = str(exc)
                    logger.exception("Parquet lake write failed")

            if args.save_timescaledb:
                try:
                    storage_result = persist_loader_outputs_dto(
                        storage=LoaderStorageDTO(
                            candles=candles_for_storage,
                            open_interest=open_interest_for_storage,
                            funding=funding_for_storage,
                        ),
                        options=PersistOptionsDTO(
                            save_parquet_lake=False,
                            save_timescaledb=True,
                            lake_root=cast(str, args.lake_root),
                            timescaledb_schema=cast(str, args.timescaledb_schema),
                            create_schema=not bool(args.timescaledb_no_bootstrap),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                        ),
                        save_tsdb_fn=save_market_data_to_timescaledb,
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_timescaledb_error"] = str(exc)
                    logger.exception("TimescaleDB write failed")

            artifact_candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            artifact_oi: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            artifact_funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            for exchange, market, symbol, timeframe in tasks:
                symbol_key = symbol.upper()
                if multi_market and multi_timeframe:
                    plot_key = f"{market}_{symbol_key}__{timeframe}"
                elif multi_market:
                    plot_key = f"{market}_{symbol_key}"
                elif multi_timeframe:
                    plot_key = f"{symbol_key}__{timeframe}"
                else:
                    plot_key = symbol_key

                result_key = (exchange, market, symbol, timeframe)
                fetched = task_results.get(result_key, [])
                merged_by_open_time = {item.open_time: item for item in fetched}
                try:
                    storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
                    stored_times = open_times_in_lake(
                        lake_root=args.lake_root,
                        market=market,
                        exchange=exchange,
                        symbol=storage_symbol,
                        timeframe=timeframe,
                    )
                    if stored_times:
                        lake_candles = load_spot_candles_from_lake(
                            lake_root=args.lake_root,
                            market=market,
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=timeframe,
                        )
                        for candle_row in lake_candles:
                            merged_by_open_time[candle_row.open_time] = candle_row
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "Failed to load full-history OHLCV source exchange=%s symbol=%s timeframe=%s",
                        exchange,
                        symbol,
                        timeframe,
                    )
                merged = [merged_by_open_time[key] for key in sorted(merged_by_open_time)]
                artifact_candles.setdefault(market, {}).setdefault(exchange, {})[plot_key] = merged

            if oi_requested:
                for exchange, symbol, timeframe in oi_tasks:
                    symbol_key = symbol.upper()
                    if multi_timeframe:
                        oi_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        oi_plot_key = symbol_key
                    oi_key = (exchange, symbol, timeframe)
                    fetched_oi = oi_results.get(oi_key, [])
                    merged_oi_by_open_time: dict[datetime, OpenInterestPoint] = {
                        oi_row.open_time: oi_row for oi_row in fetched_oi
                    }
                    try:
                        storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market="perp")
                        normalized_oi_timeframe = normalize_open_interest_timeframe(exchange=exchange, value=timeframe)
                        stored_oi_times = open_times_in_lake_by_dataset(
                            lake_root=args.lake_root,
                            dataset_type="open_interest",
                            market="perp",
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=normalized_oi_timeframe,
                        )
                        if stored_oi_times:
                            lake_oi = load_open_interest_from_lake(
                                lake_root=args.lake_root,
                                market="perp",
                                exchange=exchange,
                                symbol=storage_symbol,
                                timeframe=normalized_oi_timeframe,
                            )
                            for oi_row in lake_oi:
                                merged_oi_by_open_time[oi_row.open_time] = oi_row
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to load full-history OI source exchange=%s symbol=%s timeframe=%s",
                            exchange,
                            symbol,
                            timeframe,
                        )
                    merged_oi = [merged_oi_by_open_time[key] for key in sorted(merged_oi_by_open_time)]
                    artifact_oi.setdefault("perp", {}).setdefault(exchange, {})[oi_plot_key] = merged_oi

            if funding_requested:
                for exchange, symbol, timeframe in funding_tasks:
                    symbol_key = symbol.upper()
                    funding_key = (exchange, symbol, timeframe)
                    if multi_timeframe:
                        funding_plot_key = f"{symbol_key}__{timeframe}"
                    else:
                        funding_plot_key = symbol_key
                    fetched_funding = funding_results.get(funding_key, [])
                    merged_funding_by_open_time: dict[datetime, FundingPoint] = {
                        item.open_time: item for item in fetched_funding
                    }
                    try:
                        storage_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market="perp")
                        normalized_funding_timeframe = normalize_funding_timeframe(exchange=exchange, value=timeframe)
                        stored_funding_times = open_times_in_lake_by_dataset(
                            lake_root=args.lake_root,
                            dataset_type="funding",
                            market="perp",
                            exchange=exchange,
                            symbol=storage_symbol,
                            timeframe=normalized_funding_timeframe,
                        )
                        if stored_funding_times:
                            lake_funding = load_funding_from_lake(
                                lake_root=args.lake_root,
                                market="perp",
                                exchange=exchange,
                                symbol=storage_symbol,
                                timeframe=normalized_funding_timeframe,
                            )
                            for item in lake_funding:
                                merged_funding_by_open_time[item.open_time] = item
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "Failed to load full-history funding source exchange=%s symbol=%s timeframe=%s",
                            exchange,
                            symbol,
                            timeframe,
                        )
                    merged_funding = [merged_funding_by_open_time[key] for key in sorted(merged_funding_by_open_time)]
                    artifact_funding.setdefault("perp", {}).setdefault(exchange, {})[funding_plot_key] = merged_funding

            try:
                _write_loader_samples(
                    candles_for_storage=artifact_candles,
                    open_interest_for_storage=artifact_oi,
                    funding_for_storage=artifact_funding,
                    logger=logger,
                    generate_plots=bool(args.plot),
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to generate loader samples")

            if not args.no_json_output:
                print(json.dumps(output, indent=2))
            logger.info("Command complete: loader")
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc
