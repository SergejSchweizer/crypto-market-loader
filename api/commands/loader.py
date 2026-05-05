"""Bronze ingest command implementation."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime
from typing import Literal, TypeVar, cast

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
from application.services.runtime_service import SingleInstanceError, SingleInstanceLock, fetch_concurrency
from application.services.storage_service import persist_loader_outputs_dto
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
BRONZE_FIXED_TIMEFRAME = "1m"
T = TypeVar("T")


def _items_in_random_order(items: list[T]) -> list[T]:
    """Return a shuffled copy of input items for randomized scheduling."""

    if len(items) <= 1:
        return list(items)
    return random.SystemRandom().sample(items, k=len(items))


def _add_ingest_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
    *,
    command_name: str,
    help_text: str,
) -> None:
    """Register ingest parser."""

    parser = subparsers.add_parser(command_name, help=help_text)
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
    parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from bronze-ingest command",
    )
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks instead of default tail-only delta mode.",
    )


def add_bronze_ingest_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register canonical ``bronze-ingest`` parser."""

    _add_ingest_parser(
        subparsers,
        command_name="bronze-ingest",
        help_text="Bronze medallion ingest from supported exchanges",
    )


def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def _extract_date_partition(file_path: str) -> str | None:
    """Extract ``YYYY-MM-DD`` from parquet partition path segment ``date=YYYY-MM-DD``."""

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
    candle_tasks: list[tuple[Exchange, Market, str, str]],
    oi_tasks: list[tuple[Exchange, str, str]],
    funding_tasks: list[tuple[Exchange, str, str]],
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
    """Fetch task groups concurrently for better cross-type scheduling."""

    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    oi_errors: dict[tuple[Exchange, str, str], str] = {}
    funding_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    funding_errors: dict[tuple[Exchange, str, str], str] = {}

    async def _run_candles() -> tuple[
        dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
        dict[tuple[Exchange, Market, str, str], str],
    ]:
        return await _fetch_candle_tasks_parallel(
            tasks=candle_tasks,
            lake_root=lake_root,
            concurrency=candle_concurrency,
            logger=logger,
            on_task_complete=on_candle_task_complete,
            on_task_chunk=on_candle_task_chunk,
        )

    async def _run_oi() -> tuple[
        dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
        dict[tuple[Exchange, str, str], str],
    ]:
        return await _fetch_open_interest_tasks_parallel(
            oi_tasks=oi_tasks,
            lake_root=lake_root,
            concurrency=oi_concurrency,
            logger=logger,
            on_task_complete=on_oi_task_complete,
            on_task_chunk=on_oi_task_chunk,
        )

    async def _run_funding() -> tuple[
        dict[tuple[Exchange, str, str], list[FundingPoint]],
        dict[tuple[Exchange, str, str], str],
    ]:
        return await _fetch_funding_tasks_parallel(
            funding_tasks=funding_tasks,
            lake_root=lake_root,
            concurrency=funding_concurrency,
            logger=logger,
            on_task_complete=on_funding_task_complete,
            on_task_chunk=on_funding_task_chunk,
        )

    jobs: list[tuple[str, asyncio.Task[object]]] = []
    if candle_tasks:
        jobs.append(("candle", asyncio.create_task(_run_candles())))
    if oi_tasks:
        jobs.append(("oi", asyncio.create_task(_run_oi())))
    if funding_tasks:
        jobs.append(("funding", asyncio.create_task(_run_funding())))

    for task_type, job in jobs:
        rows_any, errors_any = await job
        if task_type == "candle":
            task_results.update(cast(dict[tuple[Exchange, Market, str, str], list[SpotCandle]], rows_any))
            task_errors.update(cast(dict[tuple[Exchange, Market, str, str], str], errors_any))
        elif task_type == "oi":
            oi_results.update(cast(dict[tuple[Exchange, str, str], list[OpenInterestPoint]], rows_any))
            oi_errors.update(cast(dict[tuple[Exchange, str, str], str], errors_any))
        else:
            funding_results.update(cast(dict[tuple[Exchange, str, str], list[FundingPoint]], rows_any))
            funding_errors.update(cast(dict[tuple[Exchange, str, str], str], errors_any))

    return task_results, task_errors, oi_results, oi_errors, funding_results, funding_errors


def _write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
) -> None:
    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
        ),
        logger=logger,
        options=ArtifactOptionsDTO(generate_plots=False),
    )


def run_bronze_ingest(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bronze-ingest command."""

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
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []
            randomized_symbols = _items_in_random_order(cast(list[str], args.symbols))
            logger.info("Randomized schedule markets=%s symbols=%s", data_types, randomized_symbols)

            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
                normalized_timeframe = normalize_timeframe(exchange=exchange, value=BRONZE_FIXED_TIMEFRAME)
                for market in cast(list[Market], ohlcv_markets):
                    for symbol in randomized_symbols:
                        task = (exchange, market, symbol, normalized_timeframe)
                        tasks.append(task)
                if oi_requested:
                    for symbol in randomized_symbols:
                        oi_tasks.append((exchange, symbol, normalized_timeframe))
                if funding_requested:
                    for symbol in randomized_symbols:
                        funding_tasks.append((exchange, symbol, normalized_timeframe))
            tasks = _items_in_random_order(tasks)

            concurrency_value = fetch_concurrency()
            candle_concurrency = concurrency_value
            oi_concurrency = concurrency_value
            funding_concurrency = concurrency_value
            incremental_parquet_on_fetch = bool(args.save_parquet_lake)
            incremental_parquet_files: list[str] = []
            logged_daily_partitions: set[tuple[str, str, str, str, str, str]] = set()
            streamed_candle_tasks: set[tuple[Exchange, Market, str, str]] = set()
            streamed_oi_tasks: set[tuple[Exchange, str, str]] = set()
            streamed_funding_tasks: set[tuple[Exchange, str, str]] = set()
            logger.info(
                "Fetch mode enabled for spot/perp, oi, and funding with concurrency=%s",
                concurrency_value,
            )
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _log_new_daily_partitions(
                *,
                data_type: str,
                exchange: str,
                market: str,
                symbol: str,
                timeframe: str,
                parquet_files: list[str],
            ) -> None:
                days = sorted(
                    {
                        day
                        for day in (_extract_date_partition(path) for path in parquet_files)
                        if day is not None
                    }
                )
                new_days = [
                    day
                    for day in days
                    if (data_type, exchange, market, symbol.upper(), timeframe, day) not in logged_daily_partitions
                ]
                if not new_days:
                    return
                for day in new_days:
                    logged_daily_partitions.add((data_type, exchange, market, symbol.upper(), timeframe, day))
                    logger.info(
                        "Parquet daily file saved type=%s exchange=%s market=%s symbol=%s timeframe=%s day=%s",
                        data_type,
                        exchange,
                        market,
                        symbol.upper(),
                        timeframe,
                        day,
                    )

            def _persist_candle_task(task: CandleFetchTaskDTO, rows: list[SpotCandle]) -> None:
                if not rows:
                    return
                storage_result = persist_loader_outputs_dto(
                    storage=LoaderStorageDTO(candles={task.market: {task.exchange: {task.symbol.upper(): rows}}}),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        lake_root=cast(str, args.lake_root),
                        oi_requested=False,
                        funding_requested=False,
                    ),
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_daily_partitions(
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
                        lake_root=cast(str, args.lake_root),
                        oi_requested=True,
                        funding_requested=False,
                    ),
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_daily_partitions(
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
                        lake_root=cast(str, args.lake_root),
                        oi_requested=False,
                        funding_requested=True,
                    ),
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_daily_partitions(
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
                    candle_tasks=tasks,
                    oi_tasks=oi_tasks,
                    funding_tasks=funding_tasks,
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
                if result_key in task_errors:
                    market_bucket[symbol_key] = {"error": task_errors[result_key]}
                    continue
                candles = task_results.get(result_key, [])
                market_bucket[symbol_key] = [_serialize_candle(item) for item in candles]
                by_market = candles_for_storage.setdefault(market, {})
                exchange_candles = by_market.setdefault(exchange, {})
                exchange_candles[symbol_key] = candles

            if oi_requested:
                for exchange, symbol, timeframe in oi_tasks:
                    symbol_key = symbol.upper()
                    oi_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("oi", {}))
                    else:
                        market_bucket = exchange_output
                    if oi_key in oi_errors:
                        market_bucket[symbol_key] = {"error": oi_errors[oi_key]}
                        continue
                    oi_rows = oi_results.get(oi_key, [])
                    market_bucket[symbol_key] = [
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
                    oi_exchange_rows[symbol_key] = oi_rows

            if funding_requested:
                for exchange, symbol, timeframe in funding_tasks:
                    symbol_key = symbol.upper()
                    funding_key = (exchange, symbol, timeframe)
                    exchange_output = cast(dict[str, object], output[exchange])
                    if multi_market:
                        market_bucket = cast(dict[str, object], exchange_output.setdefault("funding", {}))
                    else:
                        market_bucket = exchange_output
                    if funding_key in funding_errors:
                        market_bucket[symbol_key] = {"error": funding_errors[funding_key]}
                        continue
                    funding_rows = funding_results.get(funding_key, [])
                    market_bucket[symbol_key] = [
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
                    funding_exchange_rows[symbol_key] = funding_rows

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
                            lake_root=cast(str, args.lake_root),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                        ),
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_parquet_error"] = str(exc)
                    logger.exception("Parquet lake write failed")
            elif incremental_parquet_on_fetch:
                output["_parquet_files"] = incremental_parquet_files

            artifact_candles: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = candles_for_storage
            artifact_oi: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = open_interest_for_storage
            artifact_funding: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = funding_for_storage

            try:
                _write_loader_samples(
                    candles_for_storage=artifact_candles,
                    open_interest_for_storage=artifact_oi,
                    funding_for_storage=artifact_funding,
                    logger=logger,
                )
            except Exception:  # noqa: BLE001
                logger.exception("Failed to generate loader samples")

            if not args.no_json_output:
                print(json.dumps(output, indent=2))
            logger.info("Command complete: bronze-ingest")
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc


def run_loader(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Backward-compatible alias for bronze ingest execution."""

    run_bronze_ingest(args=args, logger=logger)
