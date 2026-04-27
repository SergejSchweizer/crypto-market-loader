"""Binance candle download adapter."""

from __future__ import annotations

from typing import Any

from ingestion.http_client import get_json

BINANCE_SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)
BINANCE_MAX_KLINES_PER_REQUEST = 1000



def list_supported_intervals() -> tuple[str, ...]:
    """Return Binance-supported kline intervals."""

    return BINANCE_SUPPORTED_INTERVALS



def normalize_timeframe(value: str) -> str:
    """Normalize user-provided timeframe aliases into Binance interval format."""

    raw = value.strip()
    if not raw:
        raise ValueError("timeframe cannot be empty")

    lowered = raw.lower()
    if lowered.startswith("mn") and raw[2:].isdigit():
        candidate = f"{raw[2:]}M"
    elif raw[0].isalpha() and raw[1:].isdigit():
        candidate = f"{raw[1:]}{raw[0].lower()}"
    elif raw[:-1].isdigit() and raw[-1].isalpha():
        unit = raw[-1]
        if unit == "M":
            candidate = f"{raw[:-1]}M"
        else:
            candidate = f"{raw[:-1]}{unit.lower()}"
    else:
        candidate = lowered

    if candidate in BINANCE_SUPPORTED_INTERVALS:
        return candidate

    raise ValueError(
        f"Unsupported timeframe '{value}' for binance. "
        f"Supported values: {', '.join(BINANCE_SUPPORTED_INTERVALS)}"
    )



def fetch_klines(symbol: str, interval: str, limit: int) -> list[list[object]]:
    """Fetch Binance klines with pagination for large limits."""

    if limit <= 0:
        raise ValueError("limit must be positive")

    remaining = limit
    end_time_ms: int | None = None
    pages: list[list[list[object]]] = []

    while remaining > 0:
        page_limit = min(remaining, BINANCE_MAX_KLINES_PER_REQUEST)
        page = _fetch_klines_page(
            symbol=symbol,
            interval=interval,
            limit=page_limit,
            end_time_ms=end_time_ms,
        )
        if not page:
            break

        pages.append(page)
        remaining -= len(page)

        if len(page) < page_limit:
            break

        earliest_open_time_ms = _extract_open_time_ms(page[0])
        end_time_ms = earliest_open_time_ms - 1

    return [row for page in reversed(pages) for row in page]



def _extract_open_time_ms(row: list[object]) -> int:
    """Return kline open time in milliseconds."""

    return int(row[0])



def _fetch_klines_page(symbol: str, interval: str, limit: int, end_time_ms: int | None) -> list[list[object]]:
    """Fetch one page of klines from Binance."""

    params: dict[str, Any] = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    payload = get_json("https://api.binance.com/api/v3/klines", params=params)
    if not isinstance(payload, list):
        raise ValueError("Unexpected Binance response format")
    return payload
