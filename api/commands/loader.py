"""Bronze build command implementation."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast

from api.commands.loader_dataset_handlers import (
    build_trade_tasks,
    populate_funding_output,
    populate_ohlcv_output,
    populate_oi_output,
    populate_trades_output,
)
from application.dto import (
    BronzeFetchPlanDTO,
    BronzeExecutionPolicyDTO,
    CandleFetchTaskDTO,
    FundingFetchTaskDTO,
    LoaderStorageDTO,
    OpenInterestFetchTaskDTO,
    PersistOptionsDTO,
    TradeFetchTaskDTO,
)
from application.schema import dataset_contract
from application.services.fetch_service import (
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_funding,
    fetch_symbol_open_interest,
    fetch_symbol_trades,
    fetch_trade_tasks_parallel,
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
    ensure_bronze_sidecars,
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
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick, fetch_trades_all_history, fetch_trades_range

DataType = Literal["spot", "perp", "oi", "funding", "perp_trades", "option_trades"]
_TAIL_DELTA_ONLY = True
_BRONZE_START_OPEN_MS: int | None = None
_BRONZE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
_BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS: dict[str, int] = {}
OI_DATASET_TYPE = dataset_contract("oi").dataset_type
BRONZE_FIXED_TIMEFRAME = "1m"


def _sanitize_symbols(raw_symbols: object, logger: logging.Logger) -> list[str]:
    """Return validated symbol list, dropping null/blank/non-string entries."""

    if not isinstance(raw_symbols, list):
        raise ValueError("Symbols must be provided as a list")
    cleaned: list[str] = []
    dropped = 0
    for raw in raw_symbols:
        if not isinstance(raw, str):
            dropped += 1
            continue
        symbol = raw.strip()
        if not symbol:
            dropped += 1
            continue
        cleaned.append(symbol)
    if dropped > 0:
        logger.warning("Dropped %s invalid symbol entries from configured symbol list", dropped)
    if not cleaned:
        raise ValueError("No valid symbols configured. Provide at least one non-empty symbol.")
    return cleaned


def _resolved_symbol_groups(args: argparse.Namespace, logger: logging.Logger) -> tuple[list[str], list[str], list[str]]:
    """Return deterministically ordered symbol groups for Bronze task planning."""

    validated_symbols = sorted(_sanitize_symbols(cast(object, args.symbols), logger=logger))
    validated_perp_trade_symbols = sorted(_sanitize_symbols(cast(object, args.perp_trade_symbols), logger=logger))
    validated_option_trade_symbols = sorted(_sanitize_symbols(cast(object, args.option_trade_symbols), logger=logger))
    return (
        validated_symbols,
        validated_perp_trade_symbols,
        validated_option_trade_symbols,
    )


def _build_bronze_fetch_plan(args: argparse.Namespace, logger: logging.Logger) -> BronzeFetchPlanDTO:
    """Build deterministic Bronze task plan shared across all dataset fetchers."""

    exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
    data_types = sorted(cast(list[DataType], cast(list[str], args.market)))
    ohlcv_markets = cast(list[Market], [item for item in data_types if item in {"spot", "perp"}])
    symbols, perp_trade_symbols, option_trade_symbols = _resolved_symbol_groups(args=args, logger=logger)

    candle_tasks: list[tuple[Exchange, Market, str, str]] = []
    oi_tasks: list[tuple[Exchange, str, str]] = []
    funding_tasks: list[tuple[Exchange, str, str]] = []
    for exchange in sorted(exchanges):
        normalized_timeframe = normalize_timeframe(exchange=exchange, value=BRONZE_FIXED_TIMEFRAME)
        for symbol in symbols:
            for market in ohlcv_markets:
                candle_tasks.append((exchange, market, symbol, normalized_timeframe))
        if "oi" in data_types:
            for symbol in symbols:
                oi_tasks.append((exchange, symbol, normalized_timeframe))
        if "funding" in data_types:
            for symbol in symbols:
                funding_tasks.append((exchange, symbol, normalized_timeframe))

    trade_tasks = build_trade_tasks(
        exchanges=sorted(exchanges),
        perp_trade_symbols=perp_trade_symbols,
        option_trade_symbols=option_trade_symbols,
        perp_trades_requested="perp_trades" in data_types,
        option_trades_requested="option_trades" in data_types,
    )

    return BronzeFetchPlanDTO(
        exchanges=sorted(exchanges),
        data_types=data_types,
        ohlcv_markets=ohlcv_markets,
        symbols=symbols,
        perp_trade_symbols=perp_trade_symbols,
        option_trade_symbols=option_trade_symbols,
        candle_tasks=candle_tasks,
        oi_tasks=oi_tasks,
        funding_tasks=funding_tasks,
        trade_tasks=trade_tasks,
    )


def _build_bronze_execution_policy() -> BronzeExecutionPolicyDTO:
    """Build standardized Bronze execution policy."""

    configured_concurrency = fetch_concurrency()
    effective_concurrency = 1
    return BronzeExecutionPolicyDTO(
        configured_concurrency=configured_concurrency,
        effective_concurrency=effective_concurrency,
        candle_concurrency=effective_concurrency,
        oi_concurrency=effective_concurrency,
        funding_concurrency=effective_concurrency,
        trade_concurrency=effective_concurrency,
    )


def _task_key_tuple_to_string(parts: tuple[object, ...]) -> str:
    """Serialize tuple task key to stable checkpoint string."""

    return "|".join(str(part) for part in parts)


def _bronze_checkpoint_fingerprint(args: argparse.Namespace, plan: BronzeFetchPlanDTO) -> str:
    """Build stable fingerprint for one Bronze invocation plan."""

    payload = {
        "exchange": args.exchange,
        "exchanges": plan.exchanges,
        "market": plan.data_types,
        "symbols": plan.symbols,
        "perp_trade_symbols": plan.perp_trade_symbols,
        "option_trade_symbols": plan.option_trade_symbols,
        "lake_root": cast(str, args.lake_root),
        "tail_delta_only": bool(args.tail_delta_only),
        "start_date": cast(str | None, getattr(args, "start_date", None)),
        "symbol_start_dates": cast(list[str] | None, getattr(args, "symbol_start_dates", None)),
        "exchange_symbol_start_dates": cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None)),
    }
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _bronze_checkpoint_path() -> Path:
    """Return Bronze restart-checkpoint path."""

    return Path(".run") / "checkpoints" / "bronze-build.json"


def _load_bronze_checkpoint(path: Path, fingerprint: str, logger: logging.Logger) -> dict[str, set[str]]:
    """Load matching Bronze checkpoint completed-task sets."""

    if not path.exists():
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Ignoring unreadable Bronze checkpoint '%s': %s", path, exc)
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    if not isinstance(payload, dict) or payload.get("fingerprint") != fingerprint:
        logger.info("Ignoring stale Bronze checkpoint '%s' (fingerprint mismatch)", path)
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    completed = payload.get("completed")
    if not isinstance(completed, dict):
        return {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    return {
        "candle": set(str(value) for value in cast(list[object], completed.get("candle", []))),
        "oi": set(str(value) for value in cast(list[object], completed.get("oi", []))),
        "funding": set(str(value) for value in cast(list[object], completed.get("funding", []))),
        "trade": set(str(value) for value in cast(list[object], completed.get("trade", []))),
    }


def _write_bronze_checkpoint(
    path: Path,
    *,
    fingerprint: str,
    completed: dict[str, set[str]],
) -> None:
    """Persist Bronze checkpoint atomically."""

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "fingerprint": fingerprint,
        "completed": {name: sorted(values) for name, values in completed.items()},
    }
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _sidecar_path_list(parquet_files: list[str], suffix: str) -> list[str]:
    """Build sorted unique sidecar paths for provided parquet files."""

    return sorted({str(Path(path).with_suffix(suffix).resolve()) for path in parquet_files})


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
        choices=["spot", "perp", "oi", "funding", "perp_trades", "option_trades"],
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
        "--perp-trade-symbols",
        nargs="+",
        default=["BTC", "ETH", "SOL"],
        help="Symbols for perp_trades ingestion (independent from --symbols).",
    )
    parser.add_argument(
        "--option-trade-symbols",
        nargs="+",
        default=["BTC", "ETH", "SOL"],
        help="Symbols for option_trades ingestion (independent from --symbols).",
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
        help="Suppress JSON output from bronze-build command",
    )
    parser.add_argument(
        "--tail-delta-only",
        dest="tail_delta_only",
        action="store_true",
        help="Fetch only new tail data after latest stored point (overrides config).",
    )
    parser.add_argument(
        "--full-gap-fill",
        dest="tail_delta_only",
        action="store_false",
        help="Run full historical internal gap checks instead of default tail-only delta mode.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive UTC date boundary (YYYY-MM-DD) for Bronze ingestion history.",
    )
    parser.add_argument(
        "--symbol-start-dates",
        nargs="+",
        default=None,
        help="Per-symbol inclusive UTC start dates (SYMBOL=YYYY-MM-DD), e.g. BTC=2023-04-24",
    )
    parser.add_argument(
        "--exchange-symbol-start-dates",
        nargs="+",
        default=None,
        help=(
            "Per exchange-symbol inclusive UTC start dates (EXCHANGE:SYMBOL=YYYY-MM-DD), e.g. deribit:BTC=2023-04-24"
        ),
    )


def add_bronze_build_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register canonical ``bronze-build`` parser."""

    _add_ingest_parser(
        subparsers,
        command_name="bronze-build",
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
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
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
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
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
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _fetch_symbol_trades(
    exchange: Exchange,
    market: TradeMarket,
    symbol: str,
    lake_root: str,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    return fetch_symbol_trades(
        exchange=exchange,
        market=market,
        symbol=symbol,
        lake_root=lake_root,
        symbol_normalizer=normalize_storage_symbol,
        history_fetcher=fetch_trades_all_history,
        range_fetcher=fetch_trades_range,
        latest_open_time_reader=latest_open_time_in_lake_by_dataset,
        tail_delta_only=_TAIL_DELTA_ONLY,
        on_history_chunk=on_history_chunk,
        start_open_ms_bound=_symbol_start_open_ms_bound(exchange=exchange, symbol=symbol),
    )


def _parse_start_date_to_open_ms(start_date: str | None) -> int | None:
    """Parse inclusive UTC start date ``YYYY-MM-DD`` to epoch milliseconds."""

    if start_date is None:
        return None
    value = start_date.strip()
    if not value:
        return None
    start_dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return int(start_dt.timestamp() * 1000)


def _canonical_symbol_key(symbol: str) -> str:
    """Return canonical base symbol key for per-symbol start-date matching."""

    upper = symbol.upper().strip()
    if not upper:
        return upper
    if upper.endswith("-PERPETUAL"):
        return upper.split("-", 1)[0]
    if "_" in upper:
        return upper.split("_", 1)[0]
    if upper.endswith("USDC"):
        return upper[:-4]
    if upper.endswith("USDT"):
        return upper[:-4]
    if upper.endswith("USD"):
        return upper[:-3]
    return upper


def _parse_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``SYMBOL=YYYY-MM-DD`` entries into canonical symbol->epoch-ms map."""

    if not entries:
        return {}
    parsed: dict[str, int] = {}
    for raw in entries:
        item = raw.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid symbol start date '{item}'. Expected SYMBOL=YYYY-MM-DD")
        symbol_part, date_part = item.split("=", 1)
        symbol_key = _canonical_symbol_key(symbol_part)
        if not symbol_key:
            raise ValueError(f"Invalid symbol in symbol start date '{item}'")
        start_ms = _parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in symbol start date '{item}'")
        parsed[symbol_key] = start_ms
    return parsed


def _parse_exchange_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
    """Parse ``EXCHANGE:SYMBOL=YYYY-MM-DD`` entries into canonical exchange:symbol->epoch-ms map."""

    if not entries:
        return {}
    parsed: dict[str, int] = {}
    for raw in entries:
        item = raw.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid exchange-symbol start date '{item}'. Expected EXCHANGE:SYMBOL=YYYY-MM-DD")
        pair_part, date_part = item.split("=", 1)
        if ":" not in pair_part:
            raise ValueError(f"Invalid exchange-symbol pair '{pair_part}'. Expected EXCHANGE:SYMBOL")
        exchange_part, symbol_part = pair_part.split(":", 1)
        exchange_key = exchange_part.strip().lower()
        symbol_key = _canonical_symbol_key(symbol_part)
        if not exchange_key or not symbol_key:
            raise ValueError(f"Invalid exchange-symbol in '{item}'")
        start_ms = _parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in exchange-symbol start date '{item}'")
        parsed[f"{exchange_key}:{symbol_key}"] = start_ms
    return parsed


def _symbol_start_open_ms_bound(exchange: Exchange, symbol: str) -> int | None:
    """Resolve exchange-symbol boundary, then symbol boundary, then global boundary."""

    exchange_key = exchange.lower()
    symbol_key = _canonical_symbol_key(symbol)
    exchange_symbol_key = f"{exchange_key}:{symbol_key}"
    if exchange_symbol_key in _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS:
        return _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS[exchange_symbol_key]
    return _BRONZE_SYMBOL_START_OPEN_MS.get(symbol_key, _BRONZE_START_OPEN_MS)


def _configure_bronze_start_bounds(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Initialize Bronze start-bound globals from CLI/config args and emit boundary logs."""

    global _BRONZE_START_OPEN_MS, _BRONZE_SYMBOL_START_OPEN_MS, _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS
    _BRONZE_START_OPEN_MS = _parse_start_date_to_open_ms(cast(str | None, getattr(args, "start_date", None)))
    _BRONZE_SYMBOL_START_OPEN_MS = _parse_symbol_start_dates(
        cast(list[str] | None, getattr(args, "symbol_start_dates", None))
    )
    _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS = _parse_exchange_symbol_start_dates(
        cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None))
    )
    if _BRONZE_START_OPEN_MS is not None:
        logger.info(
            "Bronze start-date boundary enabled start_date=%s start_open_ms=%s",
            cast(str, args.start_date),
            _BRONZE_START_OPEN_MS,
        )
    if _BRONZE_SYMBOL_START_OPEN_MS:
        logger.info("Bronze symbol start-date boundaries enabled symbol_bounds=%s", _BRONZE_SYMBOL_START_OPEN_MS)
    if _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS:
        logger.info(
            "Bronze exchange-symbol start-date boundaries enabled exchange_symbol_bounds=%s",
            _BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS,
        )


def _fetch_candle_tasks_parallel(
    tasks: list[tuple[Exchange, Market, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
) -> tuple[dict[tuple[Exchange, Market, str, str], list[SpotCandle]], dict[tuple[Exchange, Market, str, str], str]]:
    service_tasks = [
        CandleFetchTaskDTO(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
        for exchange, market, symbol, timeframe in tasks
    ]
    result = fetch_candle_tasks_parallel(
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


def _fetch_open_interest_tasks_parallel(
    oi_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[OpenInterestPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        OpenInterestFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in oi_tasks
    ]
    result = fetch_open_interest_tasks_parallel(
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


def _fetch_funding_tasks_parallel(
    funding_tasks: list[tuple[Exchange, str, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
) -> tuple[dict[tuple[Exchange, str, str], list[FundingPoint]], dict[tuple[Exchange, str, str], str]]:
    service_tasks = [
        FundingFetchTaskDTO(exchange=exchange, symbol=symbol, timeframe=timeframe)
        for exchange, symbol, timeframe in funding_tasks
    ]
    result = fetch_funding_tasks_parallel(
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


def _fetch_trade_tasks_parallel(
    trade_tasks: list[tuple[Exchange, TradeMarket, str]],
    lake_root: str,
    concurrency: int,
    logger: logging.Logger,
    shared_semaphore: object | None = None,
    on_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> tuple[
    dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    service_tasks = [
        TradeFetchTaskDTO(exchange=exchange, market=market, symbol=symbol) for exchange, market, symbol in trade_tasks
    ]
    result = fetch_trade_tasks_parallel(
        tasks=service_tasks,
        lake_root=lake_root,
        concurrency=concurrency,
        logger=logger,
        symbol_fetcher=_fetch_symbol_trades,
        shared_semaphore=shared_semaphore,
        on_task_complete=on_task_complete,
        on_task_chunk=on_task_chunk,
    )
    return result.rows, result.errors


def _fetch_all_task_groups(
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
    trade_tasks: list[tuple[Exchange, TradeMarket, str]] | None = None,
    trade_concurrency: int = 1,
    on_trade_task_complete: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
    on_candle_task_chunk: Callable[[CandleFetchTaskDTO, list[SpotCandle]], None] | None = None,
    on_oi_task_chunk: Callable[[OpenInterestFetchTaskDTO, list[OpenInterestPoint]], None] | None = None,
    on_funding_task_chunk: Callable[[FundingFetchTaskDTO, list[FundingPoint]], None] | None = None,
    on_trade_task_chunk: Callable[[TradeFetchTaskDTO, list[TradeTick | OptionTradeTick]], None] | None = None,
) -> tuple[
    dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    dict[tuple[Exchange, Market, str, str], str],
    dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, str, str], list[FundingPoint]],
    dict[tuple[Exchange, str, str], str],
    dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    dict[tuple[Exchange, TradeMarket, str], str],
]:
    """Fetch task groups sequentially across dataset types."""

    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]] = {}
    task_errors: dict[tuple[Exchange, Market, str, str], str] = {}
    oi_results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]] = {}
    oi_errors: dict[tuple[Exchange, str, str], str] = {}
    funding_results: dict[tuple[Exchange, str, str], list[FundingPoint]] = {}
    funding_errors: dict[tuple[Exchange, str, str], str] = {}
    trade_results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]] = {}
    trade_errors: dict[tuple[Exchange, TradeMarket, str], str] = {}

    if candle_tasks:
        candle_rows, candle_errors = _fetch_candle_tasks_parallel(
            tasks=candle_tasks,
            lake_root=lake_root,
            concurrency=candle_concurrency,
            logger=logger,
            on_task_complete=on_candle_task_complete,
            on_task_chunk=on_candle_task_chunk,
        )
        task_results.update(candle_rows)
        task_errors.update(candle_errors)

    if oi_tasks:
        oi_rows, oi_task_errors = _fetch_open_interest_tasks_parallel(
            oi_tasks=oi_tasks,
            lake_root=lake_root,
            concurrency=oi_concurrency,
            logger=logger,
            on_task_complete=on_oi_task_complete,
            on_task_chunk=on_oi_task_chunk,
        )
        oi_results.update(oi_rows)
        oi_errors.update(oi_task_errors)

    if funding_tasks:
        funding_rows, funding_task_errors = _fetch_funding_tasks_parallel(
            funding_tasks=funding_tasks,
            lake_root=lake_root,
            concurrency=funding_concurrency,
            logger=logger,
            on_task_complete=on_funding_task_complete,
            on_task_chunk=on_funding_task_chunk,
        )
        funding_results.update(funding_rows)
        funding_errors.update(funding_task_errors)

    if trade_tasks:
        trade_rows, trade_task_errors = _fetch_trade_tasks_parallel(
            trade_tasks=trade_tasks,
            lake_root=lake_root,
            concurrency=trade_concurrency,
            logger=logger,
            on_task_complete=on_trade_task_complete,
            on_task_chunk=on_trade_task_chunk,
        )
        trade_results.update(trade_rows)
        trade_errors.update(trade_task_errors)

    return (
        task_results,
        task_errors,
        oi_results,
        oi_errors,
        funding_results,
        funding_errors,
        trade_results,
        trade_errors,
    )


def run_bronze_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run bronze-build command."""

    global _TAIL_DELTA_ONLY
    _TAIL_DELTA_ONLY = bool(args.tail_delta_only)
    _configure_bronze_start_bounds(args=args, logger=logger)

    try:
        with SingleInstanceLock(".run/crypto-market-loader.lock"):
            plan = _build_bronze_fetch_plan(args=args, logger=logger)
            exchanges = plan.exchanges
            ohlcv_markets = plan.ohlcv_markets
            data_types = plan.data_types
            oi_requested = "oi" in data_types
            funding_requested = "funding" in data_types
            perp_trades_requested = "perp_trades" in data_types
            option_trades_requested = "option_trades" in data_types
            multi_market = len(data_types) > 1
            output: dict[str, object] = {}
            candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]] = {}
            open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]] = {}
            funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] = {}
            trades_for_storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]] = {}
            tasks: list[tuple[Exchange, Market, str, str]] = []
            oi_tasks: list[tuple[Exchange, str, str]] = []
            funding_tasks: list[tuple[Exchange, str, str]] = []
            trade_tasks: list[tuple[Exchange, TradeMarket, str]] = []
            logger.info(
                "Deterministic schedule markets=%s symbols=%s perp_trade_symbols=%s option_trade_symbols=%s",
                data_types,
                plan.symbols,
                plan.perp_trade_symbols,
                plan.option_trade_symbols,
            )
            for exchange in exchanges:
                exchange_output: dict[str, object] = {}
                output[exchange] = exchange_output
            tasks.extend(plan.candle_tasks)
            oi_tasks.extend(plan.oi_tasks)
            funding_tasks.extend(plan.funding_tasks)
            trade_tasks.extend(plan.trade_tasks)
            checkpoint_path = _bronze_checkpoint_path()
            checkpoint_fingerprint = _bronze_checkpoint_fingerprint(args=args, plan=plan)
            checkpoint_completed = _load_bronze_checkpoint(
                path=checkpoint_path,
                fingerprint=checkpoint_fingerprint,
                logger=logger,
            )

            tasks = [
                task
                for task in tasks
                if _task_key_tuple_to_string((task[0], task[1], task[2], task[3])) not in checkpoint_completed["candle"]
            ]
            oi_tasks = [
                task
                for task in oi_tasks
                if _task_key_tuple_to_string((task[0], task[1], task[2])) not in checkpoint_completed["oi"]
            ]
            funding_tasks = [
                task
                for task in funding_tasks
                if _task_key_tuple_to_string((task[0], task[1], task[2])) not in checkpoint_completed["funding"]
            ]
            trade_tasks = [
                task
                for task in trade_tasks
                if _task_key_tuple_to_string((task[0], task[1], task[2])) not in checkpoint_completed["trade"]
            ]
            if any(checkpoint_completed[name] for name in ("candle", "oi", "funding", "trade")):
                logger.info(
                    "Resuming from Bronze checkpoint '%s' pending_tasks candle=%s oi=%s funding=%s trade=%s",
                    checkpoint_path,
                    len(tasks),
                    len(oi_tasks),
                    len(funding_tasks),
                    len(trade_tasks),
                )
            _write_bronze_checkpoint(
                checkpoint_path,
                fingerprint=checkpoint_fingerprint,
                completed=checkpoint_completed,
            )

            policy = _build_bronze_execution_policy()
            candle_concurrency = policy.candle_concurrency
            oi_concurrency = policy.oi_concurrency
            funding_concurrency = policy.funding_concurrency
            trade_concurrency = policy.trade_concurrency
            incremental_parquet_on_fetch = bool(args.save_parquet_lake)
            incremental_parquet_files: list[str] = []
            logged_daily_partitions: set[tuple[str, str, str, str, str, str]] = set()
            streamed_candle_tasks: set[tuple[Exchange, Market, str, str]] = set()
            streamed_oi_tasks: set[tuple[Exchange, str, str]] = set()
            streamed_funding_tasks: set[tuple[Exchange, str, str]] = set()
            streamed_trade_tasks: set[tuple[Exchange, TradeMarket, str]] = set()
            logger.info(
                (
                    "Fetch mode enabled for spot/perp, oi, funding, and perp_trades with "
                    "concurrency=%s (configured=%s; parallelization disabled)"
                ),
                policy.effective_concurrency,
                policy.configured_concurrency,
            )
            if incremental_parquet_on_fetch:
                logger.info("Incremental parquet flush enabled during fetch execution")

            def _mark_checkpoint_complete(dataset: str, key: tuple[object, ...]) -> None:
                checkpoint_completed[dataset].add(_task_key_tuple_to_string(key))
                _write_bronze_checkpoint(
                    checkpoint_path,
                    fingerprint=checkpoint_fingerprint,
                    completed=checkpoint_completed,
                )

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
                    {day for day in (_extract_date_partition(path) for path in parquet_files) if day is not None}
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
                        trades_requested=False,
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
                        trades_requested=False,
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
                        trades_requested=False,
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

            def _persist_trade_task(task: TradeFetchTaskDTO, rows: list[TradeTick | OptionTradeTick]) -> None:
                if not rows:
                    return
                storage_result = persist_loader_outputs_dto(
                    storage=LoaderStorageDTO(trades={task.market: {task.exchange: {task.symbol.upper(): rows}}}),
                    options=PersistOptionsDTO(
                        save_parquet_lake=True,
                        lake_root=cast(str, args.lake_root),
                        oi_requested=False,
                        funding_requested=False,
                        trades_requested=True,
                    ),
                )
                incremental_parquet_files.extend(storage_result.parquet_files)
                _log_new_daily_partitions(
                    data_type="option_trades" if task.market == "option" else "perp_trades",
                    exchange=task.exchange,
                    market=task.market,
                    symbol=task.symbol,
                    timeframe="tick",
                    parquet_files=storage_result.parquet_files,
                )

            def _persist_trade_chunk(task: TradeFetchTaskDTO, rows: list[TradeTick | OptionTradeTick]) -> None:
                if not rows:
                    return
                streamed_trade_tasks.add((task.exchange, task.market, task.symbol))
                _persist_trade_task(task, rows)
                _mark_checkpoint_complete("trade", (task.exchange, task.market, task.symbol))

            def _persist_candle_chunk(task: CandleFetchTaskDTO, rows: list[SpotCandle]) -> None:
                if not rows:
                    return
                streamed_candle_tasks.add((task.exchange, task.market, task.symbol, task.timeframe))
                _persist_candle_task(task, rows)
                _mark_checkpoint_complete("candle", (task.exchange, task.market, task.symbol, task.timeframe))

            def _persist_oi_chunk(task: OpenInterestFetchTaskDTO, rows: list[OpenInterestPoint]) -> None:
                if not rows:
                    return
                streamed_oi_tasks.add((task.exchange, task.symbol, task.timeframe))
                _persist_oi_task(task, rows)
                _mark_checkpoint_complete("oi", (task.exchange, task.symbol, task.timeframe))

            def _persist_funding_chunk(task: FundingFetchTaskDTO, rows: list[FundingPoint]) -> None:
                if not rows:
                    return
                streamed_funding_tasks.add((task.exchange, task.symbol, task.timeframe))
                _persist_funding_task(task, rows)
                _mark_checkpoint_complete("funding", (task.exchange, task.symbol, task.timeframe))

            (
                task_results,
                task_errors,
                oi_results,
                oi_errors,
                funding_results,
                funding_errors,
                trade_results,
                trade_errors,
            ) = _fetch_all_task_groups(
                candle_tasks=tasks,
                oi_tasks=oi_tasks,
                funding_tasks=funding_tasks,
                trade_tasks=trade_tasks,
                lake_root=cast(str, args.lake_root),
                candle_concurrency=candle_concurrency,
                oi_concurrency=oi_concurrency,
                funding_concurrency=funding_concurrency,
                trade_concurrency=trade_concurrency,
                logger=logger,
                on_candle_task_complete=(
                    lambda task, rows: (
                        _persist_candle_task(task, rows)
                        if (task.exchange, task.market, task.symbol, task.timeframe) not in streamed_candle_tasks
                        else None
                    )
                )
                if incremental_parquet_on_fetch
                else None,
                on_oi_task_complete=(
                    lambda task, rows: (
                        _persist_oi_task(task, rows)
                        if (task.exchange, task.symbol, task.timeframe) not in streamed_oi_tasks
                        else None
                    )
                )
                if incremental_parquet_on_fetch
                else None,
                on_funding_task_complete=(
                    lambda task, rows: (
                        _persist_funding_task(task, rows)
                        if (task.exchange, task.symbol, task.timeframe) not in streamed_funding_tasks
                        else None
                    )
                )
                if incremental_parquet_on_fetch
                else None,
                on_candle_task_chunk=_persist_candle_chunk if incremental_parquet_on_fetch else None,
                on_oi_task_chunk=_persist_oi_chunk if incremental_parquet_on_fetch else None,
                on_funding_task_chunk=_persist_funding_chunk if incremental_parquet_on_fetch else None,
                on_trade_task_complete=(
                    lambda task, rows: (
                        _persist_trade_task(task, rows)
                        if (task.exchange, task.market, task.symbol) not in streamed_trade_tasks
                        else None
                    )
                )
                if incremental_parquet_on_fetch
                else None,
                on_trade_task_chunk=_persist_trade_chunk if incremental_parquet_on_fetch else None,
            )
            ohlcv_success_count = len(task_results)
            ohlcv_error_count = len(task_errors)
            oi_success_count = len(oi_results)
            oi_error_count = len(oi_errors)
            funding_success_count = len(funding_results)
            funding_error_count = len(funding_errors)
            trade_success_count = len(trade_results)
            trade_error_count = len(trade_errors)
            for key in task_results:
                _mark_checkpoint_complete("candle", key)
            for key in oi_results:
                _mark_checkpoint_complete("oi", key)
            for key in funding_results:
                _mark_checkpoint_complete("funding", key)
            for key in trade_results:
                _mark_checkpoint_complete("trade", key)
            logger.info(
                "Fetch summary spot/perp: success=%s failed=%s | "
                "oi: success=%s failed=%s | funding: success=%s failed=%s | trades: success=%s failed=%s",
                ohlcv_success_count,
                ohlcv_error_count,
                oi_success_count,
                oi_error_count,
                funding_success_count,
                funding_error_count,
                trade_success_count,
                trade_error_count,
            )
            symbol_totals: dict[str, int] = {}
            symbol_success: dict[str, int] = {}
            for _exchange, _market, symbol, _timeframe in tasks:
                symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
            for _exchange, symbol, _timeframe in oi_tasks:
                symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
            for _exchange, symbol, _timeframe in funding_tasks:
                symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
            for _exchange, _market, symbol in trade_tasks:
                symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
            for _exchange, _market, symbol, _timeframe in task_results:
                symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
            for _exchange, symbol, _timeframe in oi_results:
                symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
            for _exchange, symbol, _timeframe in funding_results:
                symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
            for _exchange, _market, symbol in trade_results:
                symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
            if symbol_totals:
                fairness = [
                    {
                        "symbol": symbol,
                        "success": symbol_success.get(symbol, 0),
                        "total": total,
                        "ratio": round(symbol_success.get(symbol, 0) / total, 4) if total > 0 else 0.0,
                    }
                    for symbol, total in sorted(symbol_totals.items())
                ]
                logger.info("Bronze per-symbol progress: %s", fairness)

            populate_ohlcv_output(
                output=output,
                tasks=tasks,
                task_results=task_results,
                task_errors=task_errors,
                multi_market=multi_market,
                candle_serializer=_serialize_candle,
                candles_for_storage=candles_for_storage,
            )

            if oi_requested:
                populate_oi_output(
                    output=output,
                    tasks=oi_tasks,
                    results=oi_results,
                    errors=oi_errors,
                    multi_market=multi_market,
                    storage=open_interest_for_storage,
                )

            if funding_requested:
                populate_funding_output(
                    output=output,
                    tasks=funding_tasks,
                    results=funding_results,
                    errors=funding_errors,
                    multi_market=multi_market,
                    storage=funding_for_storage,
                )

            if perp_trades_requested or option_trades_requested:
                populate_trades_output(
                    output=output,
                    tasks=trade_tasks,
                    results=trade_results,
                    errors=trade_errors,
                    multi_market=multi_market,
                    storage=trades_for_storage,
                )

            if args.save_parquet_lake and not incremental_parquet_on_fetch:
                try:
                    storage_result = persist_loader_outputs_dto(
                        storage=LoaderStorageDTO(
                            candles=candles_for_storage,
                            open_interest=open_interest_for_storage,
                            funding=funding_for_storage,
                            trades=trades_for_storage,
                        ),
                        options=PersistOptionsDTO(
                            save_parquet_lake=True,
                            lake_root=cast(str, args.lake_root),
                            oi_requested=oi_requested,
                            funding_requested=funding_requested,
                            trades_requested=(perp_trades_requested or option_trades_requested),
                        ),
                    )
                    output.update(storage_result.to_output_dict())
                except Exception as exc:  # noqa: BLE001
                    output["_parquet_error"] = str(exc)
                    logger.exception("Parquet lake write failed")
            elif incremental_parquet_on_fetch:
                output["_parquet_files"] = incremental_parquet_files

            if args.save_parquet_lake:
                parquet_files = cast(list[str], output.get("_parquet_files", []))
                selected_dataset_types: set[str] = set()
                if any(market == "spot" for market in ohlcv_markets):
                    selected_dataset_types.add("spot")
                if any(market == "perp" for market in ohlcv_markets):
                    selected_dataset_types.add("perp")
                if oi_requested:
                    selected_dataset_types.add(OI_DATASET_TYPE)
                if funding_requested:
                    selected_dataset_types.add("funding")
                if perp_trades_requested:
                    selected_dataset_types.add("perp_trades")
                if option_trades_requested:
                    selected_dataset_types.add("option_trades")
                repaired_parquet_files = ensure_bronze_sidecars(
                    lake_root=cast(str, args.lake_root),
                    dataset_types=sorted(selected_dataset_types),
                    log_fn=logger.info,
                )
                if repaired_parquet_files:
                    parquet_files = sorted(set(parquet_files).union(repaired_parquet_files))
                    output["_parquet_files"] = parquet_files
                output["_manifest_files"] = _sidecar_path_list(parquet_files, ".json")
                output["_plot_files"] = _sidecar_path_list(parquet_files, ".png")

            if perp_trades_requested or option_trades_requested:
                trade_task_total = len(trade_tasks)
                trade_error_total = len(trade_errors)
                trade_success_total = trade_task_total - trade_error_total
                trade_rows_total = sum(len(rows) for rows in trade_results.values())
                trade_parquet_files = sorted(
                    {
                        str(Path(path).resolve())
                        for path in cast(list[str], output.get("_parquet_files", []))
                        if ("dataset_type=perp_trades" in path or "dataset_type=option_trades" in path)
                        and path.endswith(".parquet")
                    }
                )
                logger.info(
                    (
                        "Trades bronze summary tasks_total=%s tasks_success=%s tasks_failed=%s "
                        "rows_total=%s parquet_files_written=%s lake_root=%s"
                    ),
                    trade_task_total,
                    trade_success_total,
                    trade_error_total,
                    trade_rows_total,
                    len(trade_parquet_files),
                    cast(str, args.lake_root),
                )
                if trade_parquet_files:
                    logger.info("Trades bronze parquet files: %s", trade_parquet_files)
                if trade_errors:
                    logger.error("Trades bronze task errors: %s", trade_errors)

            if not args.no_json_output:
                print(json.dumps(output, indent=2))
            if not (task_errors or oi_errors or funding_errors or trade_errors):
                checkpoint_path.unlink(missing_ok=True)
                logger.info("Cleared Bronze checkpoint '%s' after successful run", checkpoint_path)
            else:
                logger.warning(
                    "Retaining Bronze checkpoint '%s' for resume; failures remain",
                    checkpoint_path,
                )
            logger.info("Command complete: bronze-build")
    except SingleInstanceError as exc:
        logger.warning("Single-instance lock active")
        raise SystemExit(str(exc)) from exc
