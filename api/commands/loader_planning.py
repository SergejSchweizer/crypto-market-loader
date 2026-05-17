"""Planning helpers for bronze loader command orchestration."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime
from typing import Literal, cast

from api.commands.loader_dataset_handlers import build_option_instrument_tasks, build_trade_tasks
from application.dto import BronzeFetchPlanDTO
from ingestion.spot import Exchange, Market, normalize_timeframe

DataType = Literal["spot", "perp", "oi", "funding", "perp_trades", "option_trades", "option_instruments"]
BRONZE_FIXED_TIMEFRAME = "1m"


def sanitize_symbols(raw_symbols: object, logger: logging.Logger) -> list[str]:
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


def resolved_symbol_groups(
    args: argparse.Namespace, logger: logging.Logger
) -> tuple[list[str], list[str], list[str], list[str]]:
    """Return deterministically ordered symbol groups for Bronze task planning."""

    validated_symbols = sorted(sanitize_symbols(cast(object, args.symbols), logger=logger))
    validated_perp_trade_symbols = sorted(sanitize_symbols(cast(object, args.perp_trade_symbols), logger=logger))
    validated_option_trade_symbols = sorted(sanitize_symbols(cast(object, args.option_trade_symbols), logger=logger))
    raw_option_instrument_symbols = cast(object, getattr(args, "option_instrument_symbols", args.option_trade_symbols))
    validated_option_instrument_symbols = sorted(sanitize_symbols(raw_option_instrument_symbols, logger=logger))
    return (
        validated_symbols,
        validated_perp_trade_symbols,
        validated_option_trade_symbols,
        validated_option_instrument_symbols,
    )


def build_bronze_fetch_plan(args: argparse.Namespace, logger: logging.Logger) -> BronzeFetchPlanDTO:
    """Build deterministic Bronze task plan shared across all dataset fetchers."""

    exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
    data_types: list[str] = sorted(cast(list[str], args.market))
    ohlcv_markets = cast(list[Market], [item for item in data_types if item in {"spot", "perp"}])
    symbols, perp_trade_symbols, option_trade_symbols, option_instrument_symbols = resolved_symbol_groups(
        args=args, logger=logger
    )

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
    option_instrument_tasks = build_option_instrument_tasks(
        exchanges=sorted(exchanges),
        option_instrument_symbols=option_instrument_symbols,
        option_instruments_requested="option_instruments" in data_types,
    )

    return BronzeFetchPlanDTO(
        exchanges=sorted(exchanges),
        data_types=data_types,
        ohlcv_markets=ohlcv_markets,
        symbols=symbols,
        perp_trade_symbols=perp_trade_symbols,
        option_trade_symbols=option_trade_symbols,
        option_instrument_symbols=option_instrument_symbols,
        candle_tasks=candle_tasks,
        oi_tasks=oi_tasks,
        funding_tasks=funding_tasks,
        trade_tasks=trade_tasks,
        option_instrument_tasks=option_instrument_tasks,
    )


def parse_start_date_to_open_ms(start_date: str | None) -> int | None:
    """Parse inclusive UTC start date ``YYYY-MM-DD`` to epoch milliseconds."""

    if start_date is None:
        return None
    value = start_date.strip()
    if not value:
        return None
    start_dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=UTC)
    return int(start_dt.timestamp() * 1000)


def canonical_symbol_key(symbol: str) -> str:
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


def parse_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
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
        symbol_key = canonical_symbol_key(symbol_part)
        if not symbol_key:
            raise ValueError(f"Invalid symbol in symbol start date '{item}'")
        start_ms = parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in symbol start date '{item}'")
        parsed[symbol_key] = start_ms
    return parsed


def parse_exchange_symbol_start_dates(entries: list[str] | None) -> dict[str, int]:
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
        symbol_key = canonical_symbol_key(symbol_part)
        if not exchange_key or not symbol_key:
            raise ValueError(f"Invalid exchange-symbol in '{item}'")
        start_ms = parse_start_date_to_open_ms(date_part)
        if start_ms is None:
            raise ValueError(f"Invalid start date in exchange-symbol start date '{item}'")
        parsed[f"{exchange_key}:{symbol_key}"] = start_ms
    return parsed
