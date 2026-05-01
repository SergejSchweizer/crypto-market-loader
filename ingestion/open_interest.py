"""Open-interest ingestion interface (Deribit-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

from ingestion.exchanges import deribit_open_interest
from ingestion.spot import Exchange, Market, interval_to_milliseconds, normalize_storage_symbol, normalize_timeframe


@dataclass(frozen=True)
class OpenInterestPoint:
    """Open-interest datapoint for one instrument interval.

    Example:
        ```python
        from datetime import UTC, datetime
        from ingestion.open_interest import OpenInterestPoint

        point = OpenInterestPoint(
            exchange="deribit",
            symbol="BTC-PERPETUAL",
            interval="1m",
            open_time=datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
            open_interest=12345.0,
            open_interest_value=0.0,
        )
        ```
    """

    exchange: str
    symbol: str
    interval: str
    open_time: datetime
    close_time: datetime
    open_interest: float
    open_interest_value: float
    oi_ffill: float | None = None
    oi_is_observed: bool = True
    minutes_since_oi_observation: int = 0


def expand_open_interest_to_interval_grid(items: list[OpenInterestPoint]) -> list[OpenInterestPoint]:
    """Expand observed OI points to interval grid with forward-filled values.

    For each interval between two observed points, emits synthetic rows using the
    most recent observed OI value and marks those rows as non-observed.
    """

    if not items:
        return []

    unique_by_open: dict[datetime, OpenInterestPoint] = {item.open_time: item for item in items}
    observed_points = [unique_by_open[key] for key in sorted(unique_by_open)]
    interval_ms = open_interest_interval_to_milliseconds(
        exchange=cast(Exchange, observed_points[0].exchange),
        interval=observed_points[0].interval,
    )
    step = timedelta(milliseconds=interval_ms)
    close_delta = timedelta(milliseconds=interval_ms - 1)

    expanded: list[OpenInterestPoint] = []
    last_observed = observed_points[0]
    expanded.append(
        OpenInterestPoint(
            exchange=last_observed.exchange,
            symbol=last_observed.symbol,
            interval=last_observed.interval,
            open_time=last_observed.open_time,
            close_time=last_observed.close_time,
            open_interest=last_observed.open_interest,
            open_interest_value=last_observed.open_interest_value,
            oi_ffill=last_observed.open_interest,
            oi_is_observed=True,
            minutes_since_oi_observation=0,
        )
    )

    for observed in observed_points[1:]:
        cursor = last_observed.open_time + step
        minutes_since = 1
        while cursor < observed.open_time:
            expanded.append(
                OpenInterestPoint(
                    exchange=last_observed.exchange,
                    symbol=last_observed.symbol,
                    interval=last_observed.interval,
                    open_time=cursor,
                    close_time=cursor + close_delta,
                    open_interest=last_observed.open_interest,
                    open_interest_value=last_observed.open_interest_value,
                    oi_ffill=last_observed.open_interest,
                    oi_is_observed=False,
                    minutes_since_oi_observation=minutes_since,
                )
            )
            minutes_since += 1
            cursor += step

        expanded.append(
            OpenInterestPoint(
                exchange=observed.exchange,
                symbol=observed.symbol,
                interval=observed.interval,
                open_time=observed.open_time,
                close_time=observed.close_time,
                open_interest=observed.open_interest,
                open_interest_value=observed.open_interest_value,
                oi_ffill=observed.open_interest,
                oi_is_observed=True,
                minutes_since_oi_observation=0,
            )
        )
        last_observed = observed

    return expanded


def normalize_open_interest_timeframe(exchange: Exchange, value: str) -> str:
    """Normalize open-interest timeframe by exchange."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    # Deribit adapter currently supports snapshot collection bucketed by requested timeframe.
    return normalize_timeframe(exchange=exchange, value=value)


def open_interest_interval_to_milliseconds(exchange: Exchange, interval: str) -> int:
    """Convert open-interest interval to milliseconds."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    return interval_to_milliseconds(exchange=exchange, interval=interval)


def fetch_open_interest_all_history(
    exchange: Exchange,
    symbol: str,
    interval: str,
    market: Market,
    on_history_chunk: Callable[[list[OpenInterestPoint]], None] | None = None,
) -> list[OpenInterestPoint]:
    """Fetch all available open-interest history."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange != "deribit":
        return []
    def _on_page(page: list[dict[str, object]]) -> None:
        if on_history_chunk is None:
            return
        parsed_page = [
            deribit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
            for row in page
        ]
        on_history_chunk(
            [
                OpenInterestPoint(
                    exchange=exchange,
                    symbol=normalized_symbol,
                    interval=normalized_interval,
                    open_time=cast(datetime, cast(Any, item["open_time"])),
                    close_time=cast(datetime, cast(Any, item["close_time"])),
                    open_interest=float(cast(Any, item["open_interest"])),
                    open_interest_value=float(cast(Any, item["open_interest_value"])),
                )
                for item in parsed_page
            ]
        )

    rows = deribit_open_interest.fetch_open_interest_all(
        symbol=normalized_symbol,
        period=normalized_interval,
        on_page=_on_page if on_history_chunk is not None else None,
    )
    parsed = [
        deribit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
        for row in rows
    ]
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]


def fetch_open_interest_range(
    exchange: Exchange,
    symbol: str,
    interval: str,
    start_open_ms: int,
    end_open_ms: int,
    market: Market,
) -> list[OpenInterestPoint]:
    """Fetch open-interest by inclusive open-time range."""

    if market != "perp":
        return []
    normalized_interval = normalize_open_interest_timeframe(exchange=exchange, value=interval)
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    parsed: list[dict[str, object]] = []
    if exchange != "deribit":
        return []
    rows = deribit_open_interest.fetch_open_interest_range(
        symbol=normalized_symbol,
        period=normalized_interval,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )
    parsed = [
        deribit_open_interest.parse_open_interest_row(normalized_symbol, normalized_interval, row)
        for row in rows
    ]
    return [
        OpenInterestPoint(
            exchange=exchange,
            symbol=normalized_symbol,
            interval=normalized_interval,
            open_time=cast(datetime, cast(Any, item["open_time"])),
            close_time=cast(datetime, cast(Any, item["close_time"])),
            open_interest=float(cast(Any, item["open_interest"])),
            open_interest_value=float(cast(Any, item["open_interest_value"])),
        )
        for item in parsed
    ]
