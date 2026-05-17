"""Tests for loader bounds helper module."""

from __future__ import annotations

import argparse
import logging
from datetime import UTC, datetime

from api.commands.loader_bounds import configure_bronze_start_bounds, symbol_start_open_ms_bound


def test_symbol_start_open_ms_bound_precedence() -> None:
    resolved = symbol_start_open_ms_bound(
        exchange="deribit",
        symbol="BTCUSDT",
        global_start_open_ms=1000,
        symbol_start_open_ms={"BTC": 2000},
        exchange_symbol_start_open_ms={"deribit:BTC": 3000},
    )
    assert resolved == 3000


def test_configure_bronze_start_bounds_returns_expected_maps() -> None:
    args = argparse.Namespace(
        start_date="2023-04-24",
        symbol_start_dates=["BTC=2023-04-24"],
        exchange_symbol_start_dates=["deribit:SOL=2024-02-27"],
    )
    logger = logging.getLogger("test_loader_bounds")
    global_start_ms, symbol_map, exchange_symbol_map = configure_bronze_start_bounds(args=args, logger=logger)
    assert global_start_ms == int(datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert symbol_map["BTC"] == int(datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert exchange_symbol_map["deribit:SOL"] == int(datetime(2024, 2, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
