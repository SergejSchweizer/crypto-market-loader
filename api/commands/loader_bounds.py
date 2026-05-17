"""Bronze start-bound helpers for loader orchestration."""

from __future__ import annotations

import argparse
import logging
from typing import cast

from api.commands.loader_planning import (
    canonical_symbol_key,
    parse_exchange_symbol_start_dates,
    parse_start_date_to_open_ms,
    parse_symbol_start_dates,
)
from ingestion.spot import Exchange


def symbol_start_open_ms_bound(
    *,
    exchange: Exchange,
    symbol: str,
    global_start_open_ms: int | None,
    symbol_start_open_ms: dict[str, int],
    exchange_symbol_start_open_ms: dict[str, int],
) -> int | None:
    """Resolve exchange-symbol boundary, then symbol boundary, then global boundary."""

    exchange_key = exchange.lower()
    symbol_key = canonical_symbol_key(symbol)
    exchange_symbol_key = f"{exchange_key}:{symbol_key}"
    if exchange_symbol_key in exchange_symbol_start_open_ms:
        return exchange_symbol_start_open_ms[exchange_symbol_key]
    return symbol_start_open_ms.get(symbol_key, global_start_open_ms)


def configure_bronze_start_bounds(
    *,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> tuple[int | None, dict[str, int], dict[str, int]]:
    """Compute Bronze start-bound maps from CLI/config args and emit boundary logs."""

    global_start_open_ms = parse_start_date_to_open_ms(cast(str | None, getattr(args, "start_date", None)))
    symbol_start_open_ms = parse_symbol_start_dates(cast(list[str] | None, getattr(args, "symbol_start_dates", None)))
    exchange_symbol_start_open_ms = parse_exchange_symbol_start_dates(
        cast(list[str] | None, getattr(args, "exchange_symbol_start_dates", None))
    )
    if global_start_open_ms is not None:
        logger.info(
            "Bronze start-date boundary enabled start_date=%s start_open_ms=%s",
            cast(str, args.start_date),
            global_start_open_ms,
        )
    if symbol_start_open_ms:
        logger.info("Bronze symbol start-date boundaries enabled symbol_bounds=%s", symbol_start_open_ms)
    if exchange_symbol_start_open_ms:
        logger.info(
            "Bronze exchange-symbol start-date boundaries enabled exchange_symbol_bounds=%s",
            exchange_symbol_start_open_ms,
        )
    return global_start_open_ms, symbol_start_open_ms, exchange_symbol_start_open_ms
