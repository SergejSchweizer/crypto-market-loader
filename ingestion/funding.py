"""Funding-rate ingestion interface (Deribit-only)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from ingestion.exchanges import deribit_funding
from ingestion.http_client import HttpClientError, HttpClientHttpError
from ingestion.spot import Exchange, Market, interval_to_milliseconds, normalize_storage_symbol

DERIBIT_FUNDING_NATIVE_INTERVAL = "8h"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FundingPoint:
    """Funding-rate datapoint for one instrument interval.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.funding import FundingPoint

        point = FundingPoint(
            exchange="deribit",
            symbol="ETH-PERPETUAL",
            interval="8h",
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 7, 59, 59, 999000, tzinfo=UTC),
            funding_rate=0.0001,
            index_price=3200.0,
            mark_price=3205.0,
        )
        ```
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    funding_rate: float
    index_price: float
    mark_price: float


def normalize_funding_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize funding timeframe by exchange.

    Deribit funding retrieval uses native 8h endpoint windows while bronze storage
    preserves source event timestamps without additional bucket aggregation.
    """

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized = value.strip().lower()
    if normalized not in {"1m", "m1", DERIBIT_FUNDING_NATIVE_INTERVAL}:
        supported = f"1m, M1, {DERIBIT_FUNDING_NATIVE_INTERVAL}"
        raise ValueError(
            f"Unsupported funding timeframe '{value}' for {exchange}. Supported values: {supported}"
        )
    return DERIBIT_FUNDING_NATIVE_INTERVAL


def funding_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert funding interval to milliseconds."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return interval_to_milliseconds(exchange=exchange, interval=interval)


def fetch_funding_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
    on_history_chunk: Callable[[list[FundingPoint]], None] | None = None,
) -> list[FundingPoint]:
    """Fetch all available funding-rate history."""

    if market != "perp":
        return []
    if exchange != "deribit":
        return []
    normalized_interval = normalize_funding_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    try:
        def _on_page(page: list[dict[str, object]]) -> None:
            if on_history_chunk is None:
                return
            parsed_page = [
                deribit_funding.parse_funding_row(normalized_symbol, normalized_interval, row) for row in page
            ]
            on_history_chunk(
                [
                    FundingPoint(
                        exchange=exchange,
                        symbol=normalized_symbol,
                        interval=normalized_interval,
                        open_time=cast(datetime, cast(Any, item["open_time"])),
                        close_time=cast(datetime, cast(Any, item["close_time"])),
                        funding_rate=float(cast(Any, item["funding_rate"])),
                        index_price=float(cast(Any, item["index_price"])),
                        mark_price=float(cast(Any, item["mark_price"])),
                    )
                    for item in parsed_page
                ]
            )

        try:
            rows = deribit_funding.fetch_funding_all(
                symbol=normalized_symbol,
                period=normalized_interval,
                on_page=_on_page if on_history_chunk is not None else None,
                collect=on_history_chunk is None,
            )
        except TypeError as exc:
            if "collect" not in str(exc):
                raise
            rows = deribit_funding.fetch_funding_all(
                symbol=normalized_symbol,
                period=normalized_interval,
                on_page=_on_page if on_history_chunk is not None else None,
            )
    except HttpClientHttpError as exc:
        # Deribit may return HTTP 400 for unsupported/per-symbol funding history windows.
        # Treat that as "no rows" so one symbol does not fail the full loader run.
        if exc.status_code == 400:
            return []
        raise
    except HttpClientError:
        return []
    if on_history_chunk is not None:
        return []
    parsed = [deribit_funding.parse_funding_row(normalized_symbol, normalized_interval, row) for row in rows]
    return [
        FundingPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            funding_rate=float(cast(Any, item["funding_rate"])),
            index_price=float(cast(Any, item["index_price"])),
            mark_price=float(cast(Any, item["mark_price"])),
        )
        for item in parsed
    ]


def fetch_funding_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[FundingPoint]:
    """Fetch funding-rate rows by inclusive open-time range."""

    if market != "perp":
        return []
    if exchange != "deribit":
        return []
    normalized_interval = normalize_funding_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    started = time.monotonic()
    try:
        rows = deribit_funding.fetch_funding_range(
            symbol=normalized_symbol,
            period=normalized_interval,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
        )
    except HttpClientHttpError as exc:
        if exc.status_code == 400:
            return []
        raise
    except HttpClientError:
        logger.warning(
            "Funding day fetch failed exchange=%s symbol=%s interval=%s start_ms=%s end_ms=%s elapsed_s=%.2f",
            exchange,
            normalized_symbol,
            normalized_interval,
            start_open_ms,
            end_open_ms,
            time.monotonic() - started,
        )
        return []
    parsed = [deribit_funding.parse_funding_row(normalized_symbol, normalized_interval, row) for row in rows]
    points = [
        FundingPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            funding_rate=float(cast(Any, item["funding_rate"])),
            index_price=float(cast(Any, item["index_price"])),
            mark_price=float(cast(Any, item["mark_price"])),
        )
        for item in parsed
    ]
    logger.info(
        "Funding day fetch done exchange=%s symbol=%s interval=%s start_ms=%s end_ms=%s rows=%s elapsed_s=%.2f",
        exchange,
        normalized_symbol,
        normalized_interval,
        start_open_ms,
        end_open_ms,
        len(points),
        time.monotonic() - started,
    )
    return points
