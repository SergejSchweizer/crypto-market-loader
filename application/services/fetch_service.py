"""Fetch orchestration services for OHLCV and open-interest datasets."""

from __future__ import annotations

import logging
import multiprocessing as mp
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, cast

from application.dto import (
    CandleFetchResultDTO,
    CandleFetchTaskDTO,
    FundingFetchResultDTO,
    FundingFetchTaskDTO,
    OpenInterestFetchResultDTO,
    OpenInterestFetchTaskDTO,
    OptionInstrumentFetchResultDTO,
    OptionInstrumentFetchTaskDTO,
    TradeFetchResultDTO,
    TradeFetchTaskDTO,
)
from application.schema import dataset_contract
from application.services.fetch_bootstrap import fetch_bootstrap_history_rows, fetch_bounded_daily_rows
from application.services.fetch_executors import elapsed_seconds, run_with_optional_history_chunk
from application.services.fetch_runtime_policy import heartbeat_seconds, task_timeout_seconds
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
from ingestion.option_instruments import OptionInstrumentMetadata, fetch_option_instruments
from ingestion.spot import (
    Exchange,
    Market,
    SpotCandle,
    fetch_candles_all_history,
    fetch_candles_range,
    interval_to_milliseconds,
    normalize_storage_symbol,
)
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick, fetch_trades_all_history, fetch_trades_range

OI_DATASET_TYPE = dataset_contract("oi").dataset_type
TTimeout = TypeVar("TTimeout")
TRow = TypeVar("TRow")


class _ResultQueueProtocol:
    def put(self, item: object) -> object: ...


def _classify_trade_fetch_error(exc: Exception) -> str:
    """Classify trade fetch errors for operational summaries."""

    message = str(exc).lower()
    network_markers = (
        "no route to host",
        "name or service not known",
        "temporary failure in name resolution",
        "network is unreachable",
        "connection refused",
    )
    if any(marker in message for marker in network_markers):
        return "NET_UNREACHABLE"
    if "timeout" in message:
        return "NET_TIMEOUT"
    return "OTHER"


def _trade_unique_key(item: TradeTick | OptionTradeTick) -> tuple[datetime, str, str]:
    """Return stable dedup key for perp/option trade ticks."""

    return (item.trade_time, item.trade_id, getattr(item, "instrument_name", ""))


def _dedupe_sort_trade_rows(rows: list[TradeTick | OptionTradeTick]) -> list[TradeTick | OptionTradeTick]:
    """Deduplicate trade rows and return deterministic time/id ordering."""

    unique = {_trade_unique_key(item): item for item in rows}
    return [unique[key] for key in sorted(unique)]


def _row_open_time_ms(row: TRow) -> int:
    """Return row open timestamp in epoch milliseconds."""

    row_any = cast(Any, row)
    timestamp = getattr(row_any, "open_time", None)
    if timestamp is None:
        timestamp = getattr(row_any, "trade_time", None)
    if not isinstance(timestamp, datetime):
        raise ValueError("row is missing open_time/trade_time datetime attribute")
    return int(timestamp.timestamp() * 1000)


def _filter_rows_by_start_bound(rows: list[TRow], start_open_ms_bound: int | None) -> list[TRow]:
    """Filter rows by inclusive start bound when provided."""

    if start_open_ms_bound is None:
        return rows
    return [item for item in rows if _row_open_time_ms(item) >= start_open_ms_bound]


def _filter_chunk_callback(
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> Callable[[list[TRow]], None] | None:
    """Wrap chunk callback with optional start-bound filtering."""

    if on_history_chunk is None:
        return None
    if start_open_ms_bound is None:
        return on_history_chunk

    def _filtered_chunk(rows: list[TRow]) -> None:
        filtered = _filter_rows_by_start_bound(rows, start_open_ms_bound)
        if filtered:
            on_history_chunk(filtered)

    return _filtered_chunk


def _timeout_worker(
    result_queue: _ResultQueueProtocol,
    fn: Callable[..., object],
    kwargs: dict[str, object],
) -> None:
    """Execute one fetch call in a child process and return result state via queue."""

    try:
        result_queue.put(("ok", fn(**kwargs)))
    except Exception as exc:  # noqa: BLE001
        result_queue.put(("err", (exc.__class__.__name__, str(exc))))


def _ranges_in_random_order(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Return missing time ranges in deterministic ascending order."""

    return sorted(ranges)


def _task_timeout_seconds() -> float | None:
    """Return optional per-task timeout in seconds from environment."""

    return task_timeout_seconds()


def _heartbeat_seconds() -> float:
    """Return heartbeat interval in seconds for long-running fetch tasks."""

    return heartbeat_seconds()


def _split_range_into_utc_days(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split an inclusive millisecond range into UTC day-bounded slices."""

    if end_open_ms < start_open_ms:
        return []
    start_dt = datetime.fromtimestamp(start_open_ms / 1000, tz=UTC)
    end_dt = datetime.fromtimestamp(end_open_ms / 1000, tz=UTC)
    cursor = start_dt
    windows: list[tuple[int, int]] = []
    while cursor.date() < end_dt.date():
        day_end = (
            datetime.combine(cursor.date(), datetime.min.time(), tzinfo=UTC)
            + timedelta(days=1)
            - timedelta(milliseconds=1)
        )
        windows.append((int(cursor.timestamp() * 1000), int(day_end.timestamp() * 1000)))
        cursor = day_end + timedelta(milliseconds=1)
    windows.append((int(cursor.timestamp() * 1000), end_open_ms))
    return windows


def _day_windows_in_random_order(start_open_ms: int, end_open_ms: int) -> list[tuple[int, int]]:
    """Split range into UTC day windows and return deterministic chronological order."""

    windows = _split_range_into_utc_days(start_open_ms, end_open_ms)
    return sorted(windows)


def _fetch_bounded_daily_rows(
    *,
    start_open_ms_bound: int,
    end_open_ms: int,
    range_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
) -> list[TRow]:
    """Fetch inclusive bounded history in UTC-day windows with deterministic deduplication."""

    return fetch_bounded_daily_rows(
        day_windows=_day_windows_in_random_order(start_open_ms_bound, end_open_ms),
        range_fetcher=range_fetcher,
        fetch_kwargs=fetch_kwargs,
        dedupe_key=_row_open_time_ms,
        on_history_chunk=on_history_chunk,
    )


def _fetch_bootstrap_history_rows(
    *,
    history_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
    start_open_ms_bound: int | None,
) -> list[TRow]:
    """Run history bootstrap fetch, then apply deterministic bound-filtered deduplication."""

    filtered_rows = fetch_bootstrap_history_rows(
        history_fetcher=history_fetcher,
        fetch_kwargs=fetch_kwargs,
        on_history_chunk=on_history_chunk,
        wrap_chunk_callback=lambda callback: _filter_chunk_callback(callback, start_open_ms_bound),
        filter_rows=lambda rows: _filter_rows_by_start_bound(rows, start_open_ms_bound),
    )
    unique_by_open_time = {_row_open_time_ms(item): item for item in filtered_rows}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def _build_missing_ranges_with_optional_head_gap(
    *,
    existing_open_times: list[datetime],
    interval_ms: int,
    end_open_ms: int,
    start_open_ms_bound: int | None,
    ranges_builder: Callable[..., list[tuple[int, int]]],
) -> list[tuple[int, int]]:
    """Build missing ranges with optional head-gap extension from explicit start bound.

    This applies one shared gap-planning policy across dataset fetchers: internal gaps,
    tail gap to ``end_open_ms``, and optional head-gap when an earlier explicit start
    boundary is configured.
    """

    missing_ranges = ranges_builder(
        existing_open_times=existing_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
    )
    earliest_existing_ms = int(min(existing_open_times).timestamp() * 1000)
    if start_open_ms_bound is not None and start_open_ms_bound < earliest_existing_ms:
        head_end_ms = min(earliest_existing_ms - interval_ms, end_open_ms)
        if start_open_ms_bound <= head_end_ms:
            missing_ranges.append((start_open_ms_bound, head_end_ms))
    return missing_ranges


def _log_fetch_start(
    *,
    logger: logging.Logger,
    idx: int,
    total_tasks: int,
    fetch_type: str,
    mode: str,
    context_parts: Sequence[tuple[str, object]],
) -> None:
    """Emit a normalized fetch start log line."""

    context = " ".join(f"{key}=%s" for key, _ in context_parts)
    values = [value for _, value in context_parts]
    logger.info(
        f"Fetch start [%s/%s] type={fetch_type} {context} mode=%s",
        idx,
        total_tasks,
        *values,
        mode,
    )


def _build_fetch_heartbeat(
    *,
    logger: logging.Logger,
    fetch_type: str,
    context_parts: Sequence[tuple[str, object]],
) -> Callable[[int], None]:
    """Build normalized heartbeat logger callback for one fetch task."""

    context = " ".join(f"{key}=%s" for key, _ in context_parts)
    values = [value for _, value in context_parts]

    def _heartbeat(elapsed_s: int) -> None:
        logger.info(
            f"Fetch heartbeat type={fetch_type} {context} elapsed_s=%s",
            *values,
            elapsed_s,
        )

    return _heartbeat


def _log_fetch_done(
    *,
    logger: logging.Logger,
    idx: int,
    total_tasks: int,
    fetch_type: str,
    rows: int,
    elapsed_s: int,
    context_parts: Sequence[tuple[str, object]],
) -> None:
    """Emit a normalized fetch done log line."""

    context = " ".join(f"{key}=%s" for key, _ in context_parts)
    values = [value for _, value in context_parts]
    logger.info(
        f"Fetch done [%s/%s] type={fetch_type} {context} rows=%s elapsed_s=%s",
        idx,
        total_tasks,
        *values,
        rows,
        elapsed_s,
    )


def _log_fetch_error(
    *,
    logger: logging.Logger,
    fetch_type: str,
    elapsed_s: int,
    context_parts: Sequence[tuple[str, object]],
    error_class: str | None = None,
    use_exception: bool = True,
) -> None:
    """Emit a normalized fetch error log line."""

    context = " ".join(f"{key}=%s" for key, _ in context_parts)
    values = [value for _, value in context_parts]
    class_fragment = " class=%s" if error_class is not None else ""
    args: list[object] = values.copy()
    if error_class is not None:
        args.insert(0, error_class)
    args.append(elapsed_s)
    message = f"Fetch error type={fetch_type}{class_fragment} {context} elapsed_s=%s"
    if use_exception:
        logger.exception(message, *args)
    else:
        logger.error(message, *args)


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

    def _run_inline_with_heartbeat() -> TTimeout:
        started = datetime.now(UTC)
        stop_event = threading.Event()

        def _heartbeat_loop() -> None:
            interval = max(0.1, heartbeat_s)
            while not stop_event.wait(interval):
                elapsed_s = int((datetime.now(UTC) - started).total_seconds())
                heartbeat(elapsed_s)

        watcher = threading.Thread(target=_heartbeat_loop, daemon=True)
        watcher.start()
        try:
            return fn(**kwargs)
        finally:
            stop_event.set()
            watcher.join(timeout=1.0)

    if timeout_s is None or not use_process_timeout:
        return _run_inline_with_heartbeat()

    started = datetime.now(UTC)
    ctx = mp.get_context("fork")
    result_queue = ctx.Queue(maxsize=1)
    process = ctx.Process(target=_timeout_worker, args=(result_queue, fn, kwargs))
    try:
        process.start()
    except OSError as exc:
        if exc.errno != 5:
            raise
        logging.getLogger(__name__).warning(
            "Worker process startup failed with EIO; falling back to inline execution without hard timeout"
        )
        result_queue.close()
        result_queue.join_thread()
        return _run_inline_with_heartbeat()
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
                return cast(TTimeout, payload)
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
    start_open_ms_bound: int | None = None,
) -> list[SpotCandle]:
    """Fetch candles for one symbol with auto bootstrap/gap-fill behavior."""

    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=timeframe)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

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
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": timeframe,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "market": market,
                    "interval": timeframe,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
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
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": timeframe,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "market": market,
                "interval": timeframe,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
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
    start_open_ms_bound: int | None = None,
) -> list[OpenInterestPoint]:
    """Fetch open-interest for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

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
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": normalized_interval,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
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
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "interval": normalized_interval,
                "market": market,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
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
    start_open_ms_bound: int | None = None,
) -> list[FundingPoint]:
    """Fetch funding for one symbol with auto bootstrap/gap-fill behavior."""

    if market != "perp":
        return []

    normalized_interval = timeframe_normalizer(exchange=exchange, value=timeframe)
    storage_symbol = symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    interval_ms = interval_ms_resolver(exchange=exchange, interval=normalized_interval)
    end_open_ms = now_open_resolver(interval_ms=interval_ms)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

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
            if start_open_ms_bound is not None:
                return _fetch_bounded_daily_rows(
                    start_open_ms_bound=start_open_ms_bound,
                    end_open_ms=end_open_ms,
                    range_fetcher=range_fetcher,
                    fetch_kwargs={
                        "exchange": exchange,
                        "symbol": symbol,
                        "interval": normalized_interval,
                        "market": market,
                    },
                    on_history_chunk=on_history_chunk,
                )
            return _fetch_bootstrap_history_rows(
                history_fetcher=history_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
                start_open_ms_bound=start_open_ms_bound,
            )
        start_open_ms = int(latest_open_time.timestamp() * 1000) + interval_ms
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
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
        if start_open_ms_bound is not None:
            return _fetch_bounded_daily_rows(
                start_open_ms_bound=start_open_ms_bound,
                end_open_ms=end_open_ms,
                range_fetcher=range_fetcher,
                fetch_kwargs={
                    "exchange": exchange,
                    "symbol": symbol,
                    "interval": normalized_interval,
                    "market": market,
                },
                on_history_chunk=on_history_chunk,
            )
        return _fetch_bootstrap_history_rows(
            history_fetcher=history_fetcher,
            fetch_kwargs={
                "exchange": exchange,
                "symbol": symbol,
                "interval": normalized_interval,
                "market": market,
            },
            on_history_chunk=on_history_chunk,
            start_open_ms_bound=start_open_ms_bound,
        )
    if end_open_ms < int(min(stored_open_times).timestamp() * 1000):
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=interval_ms,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
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


def fetch_symbol_trades(
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    open_times_reader: Callable[..., list[datetime]] = open_times_in_lake_by_dataset,
    symbol_normalizer: Callable[..., str] = normalize_storage_symbol,
    now_open_resolver: Callable[..., int] = _last_closed_open_ms,
    history_fetcher: Callable[..., list[TradeTick | OptionTradeTick]] = fetch_trades_all_history,
    range_fetcher: Callable[..., list[TradeTick] | list[OptionTradeTick] | list[TradeTick | OptionTradeTick]] = (
        fetch_trades_range
    ),
    ranges_builder: Callable[..., list[tuple[int, int]]] = _missing_ranges_ms,
    latest_open_time_reader: Callable[..., datetime | None] | None = None,
    tail_delta_only: bool = False,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
    start_open_ms_bound: int | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch trades for one symbol with auto bootstrap/tail behavior."""

    storage_symbol = (
        symbol.upper().strip()
        if market == "option"
        else symbol_normalizer(exchange=exchange, symbol=symbol, market=market)
    )
    trades_dataset_type = "option_trades" if market == "option" else "perp_trades"
    end_open_ms = now_open_resolver(interval_ms=60_000)
    if start_open_ms_bound is not None and end_open_ms < start_open_ms_bound:
        return []

    if tail_delta_only:
        latest_reader = latest_open_time_reader
        if latest_reader is None:
            raise ValueError("latest_open_time_reader is required when tail_delta_only is enabled")
        latest_open_time = latest_reader(
            lake_root=lake_root,
            dataset_type=trades_dataset_type,
            market=market,
            exchange=exchange,
            symbol=storage_symbol,
            timeframe="tick",
        )
        if latest_open_time is None:
            rows = history_fetcher(
                exchange=exchange,
                symbol=symbol,
                market=market,
                on_history_chunk=_filter_chunk_callback(on_history_chunk, start_open_ms_bound),
            )
            filtered_rows = _filter_rows_by_start_bound(rows, start_open_ms_bound)
            return _dedupe_sort_trade_rows(filtered_rows)
        start_open_ms = int(latest_open_time.timestamp() * 1000) + 1
        if start_open_ms_bound is not None:
            start_open_ms = max(start_open_ms, start_open_ms_bound)
        if start_open_ms > end_open_ms:
            return []
        fetched_rows = range_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
        return _dedupe_sort_trade_rows(cast(list[TradeTick | OptionTradeTick], fetched_rows))

    stored_open_times = open_times_reader(
        lake_root=lake_root,
        dataset_type=trades_dataset_type,
        market=market,
        exchange=exchange,
        symbol=storage_symbol,
        timeframe="tick",
    )
    if not stored_open_times:
        if start_open_ms_bound is not None:
            bootstrap_rows: list[TradeTick | OptionTradeTick] = []
            for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms_bound, end_open_ms):
                day_rows = range_fetcher(
                    exchange=exchange,
                    symbol=symbol,
                    market=market,
                    start_open_ms=day_start_ms,
                    end_open_ms=day_end_ms,
                )
                if day_rows and on_history_chunk is not None:
                    on_history_chunk(cast(list[TradeTick | OptionTradeTick], day_rows))
                bootstrap_rows.extend(day_rows)
            return _dedupe_sort_trade_rows(bootstrap_rows)
        rows = history_fetcher(
            exchange=exchange,
            symbol=symbol,
            market=market,
            on_history_chunk=_filter_chunk_callback(on_history_chunk, start_open_ms_bound),
        )
        filtered_rows = _filter_rows_by_start_bound(rows, start_open_ms_bound)
        return _dedupe_sort_trade_rows(filtered_rows)

    earliest_existing_ms = int(min(stored_open_times).timestamp() * 1000)
    if end_open_ms < earliest_existing_ms:
        return []

    missing_ranges = _build_missing_ranges_with_optional_head_gap(
        existing_open_times=stored_open_times,
        interval_ms=60_000,
        end_open_ms=end_open_ms,
        start_open_ms_bound=start_open_ms_bound,
        ranges_builder=ranges_builder,
    )
    if not missing_ranges:
        return []

    gap_rows: list[TradeTick | OptionTradeTick] = []
    for start_open_ms, gap_end_ms in _ranges_in_random_order(missing_ranges):
        for day_start_ms, day_end_ms in _day_windows_in_random_order(start_open_ms, gap_end_ms):
            day_rows = range_fetcher(
                exchange=exchange,
                symbol=symbol,
                market=market,
                start_open_ms=day_start_ms,
                end_open_ms=day_end_ms,
            )
            if day_rows and on_history_chunk is not None:
                on_history_chunk(cast(list[TradeTick | OptionTradeTick], day_rows))
            gap_rows.extend(day_rows)
    filtered = _filter_rows_by_start_bound(gap_rows, start_open_ms_bound)
    return _dedupe_sort_trade_rows(filtered)


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
        context_parts = [
            ("exchange", task.exchange),
            ("market", task.market),
            ("symbol", task.symbol),
            ("timeframe", task.timeframe),
        ]
        _log_fetch_start(
            logger=logger,
            idx=idx,
            total_tasks=total_tasks,
            fetch_type="ohlcv",
            mode="auto-bootstrap-or-gap-fill",
            context_parts=context_parts,
        )
        key = (task.exchange, task.market, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        heartbeat = _build_fetch_heartbeat(
            logger=logger,
            fetch_type="ohlcv",
            context_parts=context_parts,
        )

        try:
            candles = cast(
                list[SpotCandle],
                run_with_optional_history_chunk(
                runner=_run_with_optional_timeout,
                fn=symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=heartbeat,
                use_process_timeout=False,
                kwargs={
                    "exchange": task.exchange,
                    "market": task.market,
                    "symbol": task.symbol,
                    "timeframe": task.timeframe,
                    "lake_root": lake_root,
                    "on_history_chunk": (lambda rows, _task=task: on_task_chunk(_task, rows))
                    if on_task_chunk is not None
                    else None,
                },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                fetch_type="ohlcv",
                rows=len(candles),
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_results[key] = candles
            if on_task_complete is not None:
                on_task_complete(task, candles)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_error(
                logger=logger,
                fetch_type="ohlcv",
                elapsed_s=elapsed_s,
                context_parts=context_parts,
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
        context_parts = [
            ("exchange", task.exchange),
            ("market", "perp"),
            ("symbol", task.symbol),
            ("timeframe", task.timeframe),
        ]
        _log_fetch_start(
            logger=logger,
            idx=idx,
            total_tasks=total_tasks,
            fetch_type="oi",
            mode="auto-bootstrap-or-gap-fill",
            context_parts=context_parts,
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        heartbeat = _build_fetch_heartbeat(logger=logger, fetch_type="oi", context_parts=context_parts)
        history_chunk_cb: Callable[[list[OpenInterestPoint]], None] | None = None
        if on_task_chunk is not None:
            task_for_chunk = task

            def _history_chunk_oi(
                values: list[OpenInterestPoint],
                _task: OpenInterestFetchTaskDTO = task_for_chunk,
            ) -> None:
                on_task_chunk(_task, values)

            history_chunk_cb = _history_chunk_oi

        try:
            rows = cast(
                list[OpenInterestPoint],
                run_with_optional_history_chunk(
                runner=_run_with_optional_timeout,
                fn=symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=heartbeat,
                use_process_timeout=True,
                kwargs={
                    "exchange": task.exchange,
                    "market": "perp",
                    "symbol": task.symbol,
                    "timeframe": task.timeframe,
                    "lake_root": lake_root,
                    "on_history_chunk": history_chunk_cb,
                },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                fetch_type="oi",
                rows=len(rows),
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_error(
                logger=logger,
                fetch_type="oi",
                elapsed_s=elapsed_s,
                context_parts=context_parts,
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
        context_parts = [
            ("exchange", task.exchange),
            ("market", "perp"),
            ("symbol", task.symbol),
            ("timeframe", task.timeframe),
        ]
        _log_fetch_start(
            logger=logger,
            idx=idx,
            total_tasks=total_tasks,
            fetch_type="funding",
            mode="auto-bootstrap-or-gap-fill",
            context_parts=context_parts,
        )
        key = (task.exchange, task.symbol, task.timeframe)
        task_started_at = datetime.now(UTC)
        heartbeat = _build_fetch_heartbeat(logger=logger, fetch_type="funding", context_parts=context_parts)
        history_chunk_cb: Callable[[list[FundingPoint]], None] | None = None
        if on_task_chunk is not None:
            task_for_chunk = task

            def _history_chunk_funding(
                values: list[FundingPoint],
                _task: FundingFetchTaskDTO = task_for_chunk,
            ) -> None:
                on_task_chunk(_task, values)

            history_chunk_cb = _history_chunk_funding

        try:
            rows = cast(
                list[FundingPoint],
                run_with_optional_history_chunk(
                runner=_run_with_optional_timeout,
                fn=symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=heartbeat,
                use_process_timeout=False,
                kwargs={
                    "exchange": task.exchange,
                    "market": "perp",
                    "symbol": task.symbol,
                    "timeframe": task.timeframe,
                    "lake_root": lake_root,
                    "on_history_chunk": history_chunk_cb,
                },
                ),
            )
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                fetch_type="funding",
                rows=len(rows),
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(task_started_at)
            _log_fetch_error(
                logger=logger,
                fetch_type="funding",
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_errors[key] = str(exc)
    return FundingFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_trade_tasks_parallel(
    tasks: list[TradeFetchTaskDTO],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[TradeTick | OptionTradeTick]] = fetch_symbol_trades,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> TradeFetchResultDTO:
    """Fetch trade tasks sequentially."""

    del concurrency, shared_semaphore
    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]] = {}
    task_errors: dict[tuple[Exchange, TradeMarket, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    for idx, task in enumerate(tasks, start=1):
        context_parts = [
            ("exchange", task.exchange),
            ("market", task.market),
            ("symbol", task.symbol),
        ]
        _log_fetch_start(
            logger=logger,
            idx=idx,
            total_tasks=total_tasks,
            fetch_type="trades",
            mode="auto-bootstrap-or-tail",
            context_parts=context_parts,
        )
        key = (task.exchange, task.market, task.symbol)
        started_at = datetime.now(UTC)

        heartbeat = _build_fetch_heartbeat(
            logger=logger,
            fetch_type="trades",
            context_parts=context_parts,
        )

        try:
            rows = _run_with_optional_timeout(
                symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=heartbeat,
                exchange=task.exchange,
                market=task.market,
                symbol=task.symbol,
                lake_root=lake_root,
                on_history_chunk=(lambda chunk, _task=task: on_task_chunk(_task, chunk)) if on_task_chunk else None,
            )
            elapsed_s = elapsed_seconds(started_at)
            _log_fetch_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                fetch_type="trades",
                rows=len(rows),
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_results[key] = rows
            if on_task_complete is not None:
                on_task_complete(task, rows)
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(started_at)
            error_class = _classify_trade_fetch_error(exc)
            if error_class == "NET_UNREACHABLE":
                _log_fetch_error(
                    logger=logger,
                    fetch_type="trades",
                    elapsed_s=elapsed_s,
                    context_parts=context_parts,
                    error_class=error_class,
                    use_exception=False,
                )
            else:
                _log_fetch_error(
                    logger=logger,
                    fetch_type="trades",
                    elapsed_s=elapsed_s,
                    context_parts=context_parts,
                    error_class=error_class,
                )
            task_errors[key] = f"[{error_class}] {exc}"
            if error_class == "NET_UNREACHABLE":
                remaining_tasks = tasks[idx:]
                if remaining_tasks:
                    logger.error(
                        "Aborting remaining trade fetch tasks due to network unreachable error "
                        "remaining_tasks=%s exchange=%s",
                        len(remaining_tasks),
                        task.exchange,
                    )
                for pending in remaining_tasks:
                    pending_key = (pending.exchange, pending.market, pending.symbol)
                    task_errors[pending_key] = (
                        "[NET_UNREACHABLE] skipped due to prior network unreachable error in this run"
                    )
                break
    return TradeFetchResultDTO(rows=task_results, errors=task_errors)


def fetch_option_instrument_tasks_parallel(
    tasks: list[OptionInstrumentFetchTaskDTO],
    logger: logging.Logger,
    symbol_fetcher: Callable[..., list[OptionInstrumentMetadata]] = fetch_option_instruments,
) -> OptionInstrumentFetchResultDTO:
    """Fetch option instrument metadata tasks sequentially."""

    total_tasks = len(tasks)
    task_results: dict[tuple[Exchange, str], list[OptionInstrumentMetadata]] = {}
    task_errors: dict[tuple[Exchange, str], str] = {}
    task_timeout_s = _task_timeout_seconds()
    heartbeat_s = _heartbeat_seconds()
    for idx, task in enumerate(tasks, start=1):
        context_parts = [
            ("exchange", task.exchange),
            ("market", "option"),
            ("symbol", task.symbol),
            ("timeframe", "snapshot"),
        ]
        _log_fetch_start(
            logger=logger,
            idx=idx,
            total_tasks=total_tasks,
            fetch_type="option_instruments",
            mode="snapshot",
            context_parts=context_parts,
        )
        key = (task.exchange, task.symbol)
        started_at = datetime.now(UTC)
        heartbeat = _build_fetch_heartbeat(
            logger=logger,
            fetch_type="option_instruments",
            context_parts=context_parts,
        )

        try:
            rows = _run_with_optional_timeout(
                symbol_fetcher,
                timeout_s=task_timeout_s,
                heartbeat_s=heartbeat_s,
                heartbeat=heartbeat,
                exchange=task.exchange,
                symbol=task.symbol,
            )
            elapsed_s = elapsed_seconds(started_at)
            _log_fetch_done(
                logger=logger,
                idx=idx,
                total_tasks=total_tasks,
                fetch_type="option_instruments",
                rows=len(rows),
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_results[key] = rows
        except Exception as exc:  # noqa: BLE001
            elapsed_s = elapsed_seconds(started_at)
            _log_fetch_error(
                logger=logger,
                fetch_type="option_instruments",
                elapsed_s=elapsed_s,
                context_parts=context_parts,
            )
            task_errors[key] = str(exc)
    return OptionInstrumentFetchResultDTO(rows=task_results, errors=task_errors)
