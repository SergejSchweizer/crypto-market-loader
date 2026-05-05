"""Fetch orchestration services for OHLCV and open-interest datasets."""

from __future__ import annotations

import logging
import multiprocessing as mp
import os
import random
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import TypeVar

from application.dto import (
    CandleFetchResultDTO,
    CandleFetchTaskDTO,
    FundingFetchResultDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchResultDTO,
    OpenInterestFetchTaskDTO,
)
from application.schema import dataset_contract
from application.services.gapfill_service import _last_closed_open_ms, _missing_ranges_ms
from ingestion.funding import (
    FundingPoint,
    fetch_funding_all_history,
    fetch_funding_range,
    funding_interval_to_milliseconds,
    normalize_funding_timeframe,
)
from ingestion.lake import open_times_in_lake, open_times_in_lake_by_dataset
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
)

OI_DATASET_TYPE = dataset_contract("oi").dataset_type
TTimeout = TypeVar("TTimeout")


def _timeout_worker(
    result_queue: object,
    fn: Callable[..., object],
    kwargs: dict[str, object],
) -> None:
    """Execute one fetch call in a child process and return result state via queue."""

    queue = result_queue
    try:
        queue.put(("ok", fn(**kwargs)))
    except Exception as exc:  # noqa: BLE001
        queue.put(("err", (exc.__class__.__name__, str(exc))))


def _ranges_in_random_order(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Return missing time ranges in randomized order for unbiased backfill scheduling."""

    if len(ranges) <= 1:
        return ranges
    return random.SystemRandom().sample(ranges, k=len(ranges))


def _task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    raw = os.getenv("DEPTH_FETCH_TASK_TIMEOUT_S")
    if raw is None:
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    if value <= 0:
        return None
    return value


def _heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    raw = os.getenv("DEPTH_FETCH_HEARTBEAT_S")
    if raw is None:
        return 30.0
    try:
        value = float(raw)
    except ValueError:
        return 30.0
    if value <= 0:
        return 30.0
    return value


def _split_range_into_utc_days(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split an inclusive millisecond range into UTC day-bounded slices."""

    if end_open_ms < start_open_ms:
        return []
    start_dt = datetime.fromtimestamp(start_open_ms / 1000, tz=UTC)
    end_dt = datetime.fromtimestamp(end_open_ms / 1000, tz=UTC)
    cursor = start_dt
    windows: list[tuple[int, int]] = []
    while cursor.date() < end_dt.date():
        day_end = datetime.combine(cursor.date(), datetime.min.time(), tzinfo=UTC) + timedelta(days=1) - timedelta(
            milliseconds=1
        )
        windows.append((int(cursor.timestamp() * 1000), int(day_end.timestamp() * 1000)))
        cursor = day_end + timedelta(milliseconds=1)
    windows.append((int(cursor.timestamp() * 1000), end_open_ms))
    return windows


def _day_windows_in_random_order(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split range into UTC day windows and return them in random order."""

    windows = _split_range_into_utc_days(start_open_ms, end_open_ms)
    if len(windows) <= 1:
        return windows
    return random.SystemRandom().sample(windows, k=len(windows))


def _run_with_optional_timeout(
    fn: Callable[..., TTimeout],
    *,
    timeout_s: float | None,
    heartbeat_s: float,
    heartbeat: Callable[[int], None],
    use_process_timeout: bool = False,
    **kwargs: object,
) -> TTimeout:
    """Run callable in a worker process with optional hard timeout and heartbeat."""

    if timeout_s is None or not use_process_timeout:
        return fn(**kwargs)

    started = datetime.now(UTC)
    ctx = mp.get_context("fork")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_timeout_worker, args=(result_queue, fn, kwargs))
    process.start()
    deadline = None if timeout_s is None else (time.monotonic() + timeout_s)
    try:
        while True:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    process.terminate()
                    process.join(timeout=2)
                    raise TimeoutError(f"Fetch task timed out after {timeout_s:.1f}s")
                wait_s = min(max(0.1, heartbeat_s), remaining)
            else:
                wait_s = max(0.1, heartbeat_s)

            process.join(timeout=wait_s)
            if process.is_alive():
                elapsed_s = int((datetime.now(UTC) - started).total_seconds())
                heartbeat(elapsed_s)
                continue

            if result_queue.empty():
                raise RuntimeError(f"Fetch worker exited without result (exitcode={process.exitcode})")

            status, payload = result_queue.get_nowait()
            if status == "ok":
                return payload  # type: ignore[return-value]
            exc_name, exc_message = payload
            if exc_name == "TypeError":
                raise TypeError(exc_message)
            if exc_name == "ValueError":
                raise ValueError(exc_message)
            if exc_name == "TimeoutError":
                raise TimeoutError(exc_message)
            raise RuntimeError(f"{exc_name}: {exc_message}")
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=2)
        result_queue.close()
        result_queue.join_thread()


def fetch_symbol_candles(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_all_history,
    range_fetcher: Callable[..., list[SpotCandle]] = fetch_candles_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[SpotCandle]], None] | None = None,
) -> list[SpotCandle]:
    """Fetch candles for one symbol with auto bootstrap/gap-fill behavior."""

    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=timeframe)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=timeframe,
        )
        if latest_open_time is None:
            return history_fetcher(
                exchange=exchange,
                symbol=symbol,
                market=market,
                interval=timeframe,
                on_history_chunk=on_history_chunk,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms > end_open_ms:
            return []
        fetched_rows: list[SpotCandle] = []
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, end_open_ms):
            fetched_rows.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=timeframe,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=timeframe,
    )

    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            interval=timeframe,
            on_history_chunk=on_history_chunk,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[SpotCandle] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, gap_end_ms):
            fetched.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=timeframe,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_symbol_open_interest(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_open_interest_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = open_interest_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_all_history,
    range_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_open_interest_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
) -> list[OpenInterestPoint]:
    """Fetch open-interest for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type=OI_DATASET_TYPE,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=normalized_interval,
        )
        if latest_open_time is None:
            return history_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                market=market,
                on_history_chunk=on_history_chunk,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms > end_open_ms:
            return []
        fetched_rows: list[OpenInterestPoint] = []
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, end_open_ms):
            fetched_rows.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=normalized_interval,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type=OI_DATASET_TYPE,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            market=market,
            on_history_chunk=on_history_chunk,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[OpenInterestPoint] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, gap_end_ms):
            fetched.extend(
                range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    interval=normalized_interval,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                    market=market,
                )
            )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_symbol_funding(
    exchange: Exchange,
    market: Market,
    symbol: str,
    timeframe: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    timeframe_normalizer: Callable[..., str] = normalize_funding_timeframe,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    interval_ms_resolver: Callable[..., int] = funding_interval_to_milliseconds,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    history_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_all_history,
    range_fetcher: Callable[..., list[FundingPoint]] = fetch_funding_range,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
) -> list[FundingPoint]:
    """Fetch funding for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type="funding",
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe=normalized_interval,
        )
        if latest_open_time is None:
            return history_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                market=market,
                on_history_chunk=on_history_chunk,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms > end_open_ms:
            return []
        # Funding is naturally sparse (e.g. 8h on Deribit), so one ranged call is
        # substantially faster than many day-sized calls that each re-page backend data.
        fetched_rows = range_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            market=market,
        )
        unique_by_open_time = {item.open_time: item for item in fetched_rows}
        return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type="funding",
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe=normalized_interval,
    )

    if not stored_open_times:
        return history_fetcher(
            exchange=exchange,
            symbol=symbol,
            interval=normalized_interval,
            market=market,
            on_history_chunk=on_history_chunk,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = ranges_builder(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    if not missing_ranges:
        return []

    fetched: list[FundingPoint] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        fetched.extend(
            range_fetcher(
                exchange=exchange,
                symbol=symbol,
                interval=normalized_interval,
                start_open_ms=start_open_ms,
                end_open_ms=gap_end_ms,
                market=market,
            )
        )

    unique_by_open_time = {item.open_time: item for item in fetched}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_candle_tasks_parallel(
    tasks: list[CandleFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[SpotCandle]] = fetch_symbol_candles,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
) -> CandleFetchResultDTO:
    """Fetch OHLCV tasks sequentially."""

    del concurrency, shared_semaphore
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.market,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.market, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        hb_exchange = task.exchange
        hb_market = task.market
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe

        def _hb_ohlcv(
            elapsed_s: int,
            ex: str = hb_exchange,
            mk: str = hb_market,
            sy: str = hb_symbol,
            tf: str = hb_timeframe,
        ) -> None:
            logger.info(
                "Fetch heartbeat type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s elapsed_s=%s",
                ex,
                mk,
                sy,
                tf,
                elapsed_s,
            )
        try:
            try:
                candles = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_hb_ohlcv,
                    exchange=task.exchange,
                    market=task.market,
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                    on_history_chunk=(lambda rows, _task=task: on_task_chunk(_task, rows))
                    if on_task_chunk is not None
                    else None,
                )
            except TypeError as exc:
                if "on_history_chunk" not in str(exc):
                    raise
                candles = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    heartbeat=_hb_ohlcv,
                    exchange=task.exchange,
                    market=task.market,
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                )
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.info(
                "Fetch done [%s/%s] type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.market,
                task.symbol,
                task.timeframe,
                len(candles),
                elapsed_s,
            )
            task_results[key] = candles
            if on_task_complete is not None:
                on_task_complete(task, candles)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.exception(
                "Fetch error type=ohlcv exchange=%s market=%s symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.market,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)

    return CandleFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_open_interest_tasks_parallel(
    tasks: list[OpenInterestFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[OpenInterestPoint]] = fetch_symbol_open_interest,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> OpenInterestFetchResultDTO:
    """Fetch open-interest tasks sequentially."""

    del concurrency, shared_semaphore
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=oi exchange=%s market=perp symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        hb_exchange = task.exchange
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe
        try:
            try:
                rows = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    use_process_timeout=True,
                    heartbeat=lambda elapsed_s, ex=hb_exchange, sy=hb_symbol, tf=hb_timeframe: logger.info(
                        "Fetch heartbeat type=oi exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                        ex,
                        sy,
                        tf,
                        elapsed_s,
                    ),
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                    on_history_chunk=(lambda values, _task=task: on_task_chunk(_task, values))
                    if on_task_chunk is not None
                    else None,
                )
            except TypeError as exc:
                if "on_history_chunk" not in str(exc):
                    raise
                rows = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    use_process_timeout=True,
                    heartbeat=lambda elapsed_s, ex=hb_exchange, sy=hb_symbol, tf=hb_timeframe: logger.info(
                        "Fetch heartbeat type=oi exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                        ex,
                        sy,
                        tf,
                        elapsed_s,
                    ),
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                )
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.info(
                "Fetch done [%s/%s] type=oi exchange=%s market=perp symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.symbol,
                task.timeframe,
                len(rows),
                elapsed_s,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.exception(
                "Fetch error type=oi exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)
    return OpenInterestFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_funding_tasks_parallel(
    tasks: list[FundingFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[FundingPoint]] = fetch_symbol_funding,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> FundingFetchResultDTO:
    """Fetch funding tasks sequentially."""

    del concurrency, shared_semaphore
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    task_errors: dict[tuple[Exchange, str, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    for idx, task in enumerate(tasks, start=1):
        logger.info(
            "Fetch start [%s/%s] type=funding exchange=%s market=perp symbol=%s timeframe=%s mode=%s",
            idx,
            total_tasks,
            task.exchange,
            task.symbol,
            task.timeframe,
            "auto-bootstrap-or-gap-fill",
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        hb_exchange = task.exchange
        hb_symbol = task.symbol
        hb_timeframe = task.timeframe
        try:
            try:
                rows = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    use_process_timeout=False,
                    heartbeat=lambda elapsed_s, ex=hb_exchange, sy=hb_symbol, tf=hb_timeframe: logger.info(
                        "Fetch heartbeat type=funding exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                        ex,
                        sy,
                        tf,
                        elapsed_s,
                    ),
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                    on_history_chunk=(lambda values, _task=task: on_task_chunk(_task, values))
                    if on_task_chunk is not None
                    else None,
                )
            except TypeError as exc:
                if "on_history_chunk" not in str(exc):
                    raise
                rows = _run_with_optional_timeout(
                    symbol_fetcher,
                    timeout_s=task_timeout_s,
                    heartbeat_s=heartbeat_s,
                    use_process_timeout=False,
                    heartbeat=lambda elapsed_s, ex=hb_exchange, sy=hb_symbol, tf=hb_timeframe: logger.info(
                        "Fetch heartbeat type=funding exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                        ex,
                        sy,
                        tf,
                        elapsed_s,
                    ),
                    exchange=task.exchange,
                    market="perp",
                    symbol=task.symbol,
                    timeframe=task.timeframe,
                    lake_root=lake_root,
                )
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.info(
                "Fetch done [%s/%s] type=funding exchange=%s market=perp symbol=%s timeframe=%s rows=%s elapsed_s=%s",
                idx,
                total_tasks,
                task.exchange,
                task.symbol,
                task.timeframe,
                len(rows),
                elapsed_s,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = int((datetime.now(UTC) - task_started_at).total_seconds())
            logger.exception(
                "Fetch error type=funding exchange=%s market=perp symbol=%s timeframe=%s elapsed_s=%s",
                task.exchange,
                task.symbol,
                task.timeframe,
                elapsed_s,
            )
            task_errors[key] = str(exc)
    return FundingFetchResultDTO(rows=task_results, errors=task_errors)
