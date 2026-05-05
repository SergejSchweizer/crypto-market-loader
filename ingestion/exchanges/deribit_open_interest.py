"""Deribit perpetual open-interest adapter."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import HttpClientHttpError, get_json

DERIBIT_LAST_SETTLEMENTS_URL = "https://www.deribit.com/api/v2/public/get_last_settlements_by_instrument"
DERIBIT_MAX_POINTS_PER_REQUEST = 1000
_OPEN_INTEREST_HISTORY_CACHE: dict[str, list[dict[str, object]]] = {}


def _normalize_continuation_token(value: object) -> str | None:
    """Normalize Deribit continuation token, including string sentinels."""

    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized or normalized.lower() == "none":
        return None
    return normalized


def _normalize_open_interest_instrument(symbol: str) -> str:
    """Map normalized perp symbols to Deribit OI endpoint instrument names."""

    if symbol == "SOL-PERPETUAL":
        return "SOL_USDC-PERPETUAL"
    return symbol


def fetch_open_interest_all(
    symbol: str,
    period: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
    collect: bool = True,
) -> list[dict[str, object]]:
    """Fetch all available Deribit historical open-interest points."""

    del period
    instrument_name = _normalize_open_interest_instrument(symbol)
    continuation: str | None = None
    pages: list[list[dict[str, object]]] = []
    seen_continuations: set[str] = set()
    previous_page_edge: tuple[int, int] | None = None

    while True:
        page, next_continuation = _fetch_open_interest_page(symbol=instrument_name, continuation=continuation)
        if not page:
            break
        page_first_ts = int(cast(Any, page[0]["timestamp"]))
        page_last_ts = int(cast(Any, page[-1]["timestamp"]))
        page_edge = (page_first_ts, page_last_ts)
        if previous_page_edge == page_edge:
            break
        previous_page_edge = page_edge
        if collect:
            pages.append(page)
        if on_page is not None:
            on_page(page)
        if next_continuation is None:
            break
        if next_continuation in seen_continuations:
            break
        seen_continuations.add(next_continuation)
        continuation = next_continuation

    if not collect:
        return []

    rows = [row for page in reversed(pages) for row in page]
    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_open_interest_range(
    symbol: str,
    period: str,
    start_open_ms: int,
    end_open_ms: int,
) -> list[dict[str, object]]:
    """Fetch Deribit historical open-interest records by inclusive range."""

    if end_open_ms < start_open_ms:
        return []

    history = _OPEN_INTEREST_HISTORY_CACHE.get(symbol)
    if history is None:
        history = fetch_open_interest_all(symbol=symbol, period=period)
        _OPEN_INTEREST_HISTORY_CACHE[symbol] = history

    return [
        item
        for item in history
        if start_open_ms <= int(cast(Any, item["timestamp"])) <= end_open_ms
    ]


def parse_open_interest_row(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
    """Convert Deribit settlement payload to normalized raw record fields."""

    open_time_ms = int(cast(Any, row["timestamp"]))
    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
    close_time = open_time

    return {
        "symbol": symbol,
        "timeframe": period,
        "open_time": open_time,
        "close_time": close_time,
        "open_interest": float(cast(Any, row["open_interest"])),
        "open_interest_value": 0.0,
    }


def _fetch_open_interest_page(symbol: str, continuation: str | None) -> tuple[list[dict[str, object]], str | None]:
    """Fetch one historical-open-interest page from Deribit settlements."""

    params: dict[str, Any] = {
        "instrument_name": symbol,
        "count": DERIBIT_MAX_POINTS_PER_REQUEST,
    }
    if continuation:
        params["continuation"] = continuation

    try:
        payload = get_json(DERIBIT_LAST_SETTLEMENTS_URL, params=params)
    except HttpClientHttpError as exc:
        if exc.status_code == 400:
            # Some instruments are not supported by this endpoint. Treat as no data.
            return [], None
        raise
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit open-interest response format")

    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit open-interest result payload")
    settlements = result.get("settlements")
    if not isinstance(settlements, list):
        return [], None
    next_continuation = result.get("continuation")
    continuation_token = _normalize_continuation_token(next_continuation)

    rows: list[dict[str, object]] = []
    for item in settlements:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        position = item.get("position")
        if ts is None or position is None:
            continue
        rows.append({"timestamp": int(cast(Any, ts)), "open_interest": float(cast(Any, position))})

    rows.sort(key=lambda x: int(cast(Any, x["timestamp"])))
    return rows, continuation_token
