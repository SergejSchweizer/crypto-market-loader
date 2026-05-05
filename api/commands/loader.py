"""Main loader command implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
from collections.abc import Callable
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
from application.schema import dataset_contract
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
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock
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
    latest_open_time_in_lake,
    latest_open_time_in_lake_by_dataset,
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
_TAIL_DELTA_ONLY = True
OI_DATASET_TYPE = dataset_contract("oi").dataset_type
LOADER_FIXED_TIMEFRAME = "1m"


def _items_in_random_order(items: list[str]) -> list[str]:
    """Return a shuffled copy of input items for randomized scheduling."""

    if len(items) <= 1:
        return list(items)
    return random.SystemRandom().sample(items, k=len(items))


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
    parser.set_defaults(tail_delta_only=True)
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
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks instead of default tail-only delta mode.",
    )


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _extract_date_partition(file_path: str) -> str | None:
    """Extract ``YYYY-MM`` from parquet partition path segment ``date=YYYY-MM``."""

    marker = "/date="
    if marker not in file_path:
        return None
    tail = file_path.split(marker, 1)[1]
    return tail.split("/", 1)[0] if tail else None


def _fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
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
        latest_open_time_reader=latest_open_time_in_lake,
        tail_delta_only=_TAIL_DELTA_ONLY,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
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
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_TAIL_DELTA_ONLY,
        on_history_chunk=on_history_chunk,
    )


def _fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
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
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_TAIL_DELTA_ONLY,
        on_history_chunk=on_history_chunk,
    )


async def _fetch_candle_tasks_parallel(
    tasks: list[tuple[Exchange, Market, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: asyncio.Semaphore | None = None,
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
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
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


async def _fetch_open_interest_tasks_parallel(
    oi_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: asyncio.Semaphore | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
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
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


async def _fetch_funding_tasks_parallel(
    funding_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: asyncio.Semaphore | None = None,
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
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
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


async def _fetch_all_task_groups(
    tasks_by_market: dict[Market, list[tuple[Exchange, Market, str, str]]],
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
    data_type_order: list[DataType],
    lake_root: str,
    candle_concurrency: int,
    oi_concurrency: int,
    funding_concurrency: int,
    logger: logging.Logger,
    on_candle_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_oi_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_candle_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_oi_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], list[FundingPoint]],
    dict[tuple[Exchange, str, str], str],
]:
    """Fetch task groups in the requested data-type order."""

    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    oi_errors: dict[tuple[Exchange, str, str], str] = {}
    funding_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    funding_errors: dict[tuple[Exchange, str, str], str] = {}

    for data_type in data_type_order:
        if data_type in {"spot", "perp"}:
            market_tasks = tasks_by_market.get(cast(Market, data_type), [])
            if not market_tasks:
                continue
            rows, errors = await _fetch_candle_tasks_parallel(
                tasks=market_tasks,
                lake_root=lake_root,
                concurrency=candle_concurrency,
                logger=logger,
                on_task_complete=on_candle_task_complete,
                on_task_chunk=on_candle_task_chunk,
            )
            task_results.update(rows)
            task_errors.update(errors)
            continue
        if data_type == "oi":
            if not oi_tasks:
                continue
            rows, errors = await _fetch_open_interest_tasks_parallel(
                oi_tasks=oi_tasks,
                lake_root=lake_root,
                concurrency=oi_concurrency,
                logger=logger,
                on_task_complete=on_oi_task_complete,
                on_task_chunk=on_oi_task_chunk,
            )
            oi_results.update(rows)
            oi_errors.update(errors)
            continue
        if data_type == "funding":
            if not funding_tasks:
                continue
            rows, errors = await _fetch_funding_tasks_parallel(
                funding_tasks=funding_tasks,
                lake_root=lake_root,
                concurrency=funding_concurrency,
                logger=logger,
                on_task_complete=on_funding_task_complete,
                on_task_chunk=on_funding_task_chunk,
            )
            funding_results.update(rows)
            funding_errors.update(errors)

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

    global _TAIL_DELTA_ONLY
    _TAIL_DELTA_ONLY = bool(args.tail_delta_only)

    try:
        with SingleInstanceLock(".run/crypto-market-loader.lock"):
            exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
            randomized_data_types = cast(list[DataType], _items_in_random_order(cast(list[str], args.market)))
            ohlcv_markets = [item for item in randomized_data_types if item in {"spot", "perp"}]
            data_types = randomized_data_types
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            multi_market = len(data_types) > 1
            multi_timeframe = False
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            tasks_by_market: dict[Market, list[tuple[Exchange, Market, str, str]]] = {"spot": [], "perp": []}
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []
            randomized_symbols = _items_in_random_order(cast(list[str], args.symbols))
            logger.info("Randomized schedule markets=%s symbols=%s", data_types, randomized_symbols)

            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
                normalized_timeframe = normalize_timeframe(exchange=exchange, value=LOADER_FIXED_TIMEFRAME)
                for market in cast(list[Market], ohlcv_markets):
                    for symbol in randomized_symbols:
                        task = (exchange, market, symbol, normalized_timeframe)
                        tasks.append(task)
                        tasks_by_market[market].append(task)
                if oi_requested:
                    for symbol in randomized_symbols:
                        oi_tasks.append((exchange, symbol, normalized_timeframe))
                if funding_requested:
                    for symbol in randomized_symbols:
                        funding_tasks.append((exchange, symbol, normalized_timeframe))

            candle_concurrency = 1
            oi_concurrency = 1
            funding_concurrency = 1
            incremental_parquet_on_fetch = bool(args.save_parquet_lake)
            incremental_parquet_files: list[str] = []
            logged_monthly_partitions: set[tuple[str, str, str, str, str, str]] = set()
            streamed_candle_tasks: set[tuple[Exchange, Market, str, str]] = set()
            streamed_oi_tasks: set[tuple[Exchange, str, str]] = set()
            streamed_funding_tasks: set[tuple[Exchange, str, str]] = set()
            logger.info("Sequential fetch mode enabled for spot/perp, oi, and funding")
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _log_new_monthly_partitions(
                *,
                data_type: str,
                exchange: str,
                market: str,
                symbol: str,
                timeframe: str,
                parquet_files: list[str],
            ) -> None:
                months = sorted(
                    {
                        month
                        for month in (_extract_date_partition(path) for path in parquet_files)
                        if month is not None
                    }
                )
                new_months = [
                    month
                    for month in months
                    if (data_type, exchange, market, symbol.upper(), timeframe, month) not in logged_monthly_partitions
                ]
                if not new_months:
                    return
                for month in new_months:
                    logged_monthly_partitions.add((data_type, exchange, market, symbol.upper(), timeframe, month))
                logger.info(
                    "Parquet monthly file saved type=%s exchange=%s market=%s symbol=%s timeframe=%s months=%s",
                    data_type,
                    exchange,
                    market,
                    symbol.upper(),
                    timeframe,
                    ",".join(new_months),
                )

            def _persist_candle_task(task: CandleFetchTaskDTO, rows: list[SpotCandle]) -> None:
                if not rows:
                    return
                storage_result = persist_loader_outputs_dto(
                    storage=LoaderStorageDTO(candles={task.market: {task.exchange: {task.symbol.upper(): rows}}}),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        save_timescaledb=False,
                        lake_root=cast(str, args.lake_root),
                        timescaledb_schema=cast(str, args.timescaledb_schema),
                        create_schema=not bool(args.timescaledb_no_bootstrap),
                        oi_requested=False,
                        funding_requested=False,
                    ),
                    save_tsdb_fn=save_market_data_to_timescaledb,
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_monthly_partitions(
                    data_type="ohlcv",
                    exchange=task.exchange,
                    market=task.market,
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    parquet_files=storage_result.parquet_files,
                )

            def _persist_oi_task(task: OpenInterestFetchTaskDTO, rows: list[OpenInterestPoint]) -> None:
                if not rows:
                    return
                storage_result = persist_loader_outputs_dto(
                    storage=LoaderStorageDTO(open_interest={"perp": {task.exchange: {task.symbol.upper(): rows}}}),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        save_timescaledb=False,
                        lake_root=cast(str, args.lake_root),
                        timescaledb_schema=cast(str, args.timescaledb_schema),
                        create_schema=not bool(args.timescaledb_no_bootstrap),
                        oi_requested=True,
                        funding_requested=False,
                    ),
                    save_tsdb_fn=save_market_data_to_timescaledb,
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_monthly_partitions(
                    data_type="oi",
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    parquet_files=storage_result.parquet_files,
                )

            def _persist_funding_task(task: FundingFetchTaskDTO, rows: list[FundingPoint]) -> None:
                if not rows:
                    return
                storage_result = persist_loader_outputs_dto(
                    storage=LoaderStorageDTO(funding={"perp": {task.exchange: {task.symbol.upper(): rows}}}),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        save_timescaledb=False,
                        lake_root=cast(str, args.lake_root),
                        timescaledb_schema=cast(str, args.timescaledb_schema),
                        create_schema=not bool(args.timescaledb_no_bootstrap),
                        oi_requested=False,
                        funding_requested=True,
                    ),
                    save_tsdb_fn=save_market_data_to_timescaledb,
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_monthly_partitions(
                    data_type="funding",
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    parquet_files=storage_result.parquet_files,
                )

            def _persist_candle_chunk(task: CandleFetchTaskDTO, rows: list[SpotCandle]) -> None:
                if not rows:
                    return
                streamed_candle_tasks.add((task.exchange, task.market, task.symbol, task.timeframe))
                _persist_candle_task(task, rows)

            def _persist_oi_chunk(task: OpenInterestFetchTaskDTO, rows: list[OpenInterestPoint]) -> None:
                if not rows:
                    return
                streamed_oi_tasks.add((task.exchange, task.symbol, task.timeframe))
                _persist_oi_task(task, rows)

            def _persist_funding_chunk(task: FundingFetchTaskDTO, rows: list[FundingPoint]) -> None:
                if not rows:
                    return
                streamed_funding_tasks.add((task.exchange, task.symbol, task.timeframe))
                _persist_funding_task(task, rows)

            task_results, task_errors, oi_results, oi_errors, funding_results, funding_errors = asyncio.run(
                _fetch_all_task_groups(
                    tasks_by_market=tasks_by_market,
                    oi_tasks=oi_tasks,
                    funding_tasks=funding_tasks,
                    data_type_order=data_types,
                    lake_root=cast(str, args.lake_root),
                    candle_concurrency=candle_concurrency,
                    oi_concurrency=oi_concurrency,
                    funding_concurrency=funding_concurrency,
                    logger=logger,
                    on_candle_task_complete=(
                        lambda task, rows: _persist_candle_task(task, rows)
                        if (task.exchange, task.market, task.symbol, task.timeframe) not in streamed_candle_tasks
                        else None
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_oi_task_complete=(
                        lambda task, rows: _persist_oi_task(task, rows)
                        if (task.exchange, task.symbol, task.timeframe) not in streamed_oi_tasks
                        else None
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_funding_task_complete=(
                        lambda task, rows: _persist_funding_task(task, rows)
                        if (task.exchange, task.symbol, task.timeframe) not in streamed_funding_tasks
                        else None
                    )
                    if incremental_parquet_on_fetch
                    else None,
                    on_candle_task_chunk=_persist_candle_chunk if incremental_parquet_on_fetch else None,
                    on_oi_task_chunk=_persist_oi_chunk if incremental_parquet_on_fetch else None,
                    on_funding_task_chunk=_persist_funding_chunk if incremental_parquet_on_fetch else None,
                )
            )
            ohlcv_success_count = len(task_results)
            ohlcv_error_count = len(task_errors)
            oi_success_count = len(oi_results)
            oi_error_count = len(oi_errors)
            funding_success_count = len(funding_results)
            funding_error_count = len(funding_errors)
            logger.info(
                "Fetch summary spot/perp: success=%s failed=%s | "
                "oi: success=%s failed=%s | funding: success=%s failed=%s",
                ohlcv_success_count,
                ohlcv_error_count,
                oi_success_count,
                oi_error_count,
                funding_success_count,
                funding_error_count,
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

            if args.save_parquet_lake and not incremental_parquet_on_fetch:
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
            elif incremental_parquet_on_fetch:
                output["_parquet_files"] = incremental_parquet_files

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

            artifact_candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = candles_for_storage
            artifact_oi: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = open_interest_for_storage
            artifact_funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = funding_for_storage

            if bool(args.plot):
                artifact_candles = {}
                artifact_oi = {}
                artifact_funding = {}
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
                            normalized_oi_timeframe = normalize_open_interest_timeframe(
                                exchange=exchange, value=timeframe
                            )
                            stored_oi_times = open_times_in_lake_by_dataset(
                                lake_root=args.lake_root,
                                dataset_type=OI_DATASET_TYPE,
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
                            normalized_funding_timeframe = normalize_funding_timeframe(
                                exchange=exchange, value=timeframe
                            )
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
                        merged_funding = [
                            merged_funding_by_open_time[key] for key in sorted(merged_funding_by_open_time)
                        ]
                        artifact_funding.setdefault("perp", {}).setdefault(exchange, {})[
                            funding_plot_key
                        ] = merged_funding
            else:
                logger.info("Plot generation disabled; skipping full-history lake merge for sample artifacts")

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
