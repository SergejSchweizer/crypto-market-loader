"""Deribit perpetual funding-rate adapter."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_FUNDING_URL = "https://www.deribit.com/api/v2/public/get_funding_rate_history"
DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST = 500


def _normalize_funding_instrument(symbol: str) -> str:
    """Map normalized perp symbols to Deribit funding endpoint instrument names."""

    if symbol == "SOL-PERPETUAL":
        return "SOL_USDC-PERPETUAL"
    return symbol


def fetch_funding_range(
    symbol: str,
    period: str,
    start_open_ms: int,
    end_open_ms: int,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
) -> list[dict[str, object]]:
    """Fetch Deribit funding-rate records by inclusive range."""

    if end_open_ms < start_open_ms:
        return []

    instrument_name = _normalize_funding_instrument(symbol)
    period_ms = _period_to_milliseconds(period)
    max_window_ms = DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST * period_ms

    # Fast path: small ranges (e.g. one-day delta windows) should be served by one API call.
    if start_open_ms > 0 and (end_open_ms - start_open_ms + 1) <= max_window_ms:
        page = _fetch_funding_page(
            symbol=instrument_name,
            start_time_ms=start_open_ms,
            end_time_ms=end_open_ms,
        )
        if on_page is not None and page:
            on_page(page)
        dedup = {
            int(cast(Any, row["timestamp"])): row
            for row in page
            if start_open_ms <= int(cast(Any, row["timestamp"])) <= end_open_ms
        }
        return [dedup[key] for key in sorted(dedup)]

    cursor = start_open_ms
    rows: list[dict[str, object]] = []
    last_cursor: int | None = None

    while cursor <= end_open_ms:
        if last_cursor is not None and cursor <= last_cursor:
            cursor = last_cursor + period_ms
            if cursor > end_open_ms:
                break
        last_cursor = cursor
        window_end_ms = min(
            end_open_ms,
            cursor + (DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST * period_ms) - 1,
        )
        page = _fetch_funding_page(
            symbol=instrument_name,
            start_time_ms=cursor,
            end_time_ms=window_end_ms,
        )
        if page:
            if on_page is not None:
                on_page(page)
            rows.extend(
                [item for item in page if start_open_ms <= int(cast(Any, item["timestamp"])) <= end_open_ms]
            )
            page_ts = [int(cast(Any, item["timestamp"])) for item in page]
            min_ts = min(page_ts)
            max_ts = max(page_ts)
            if min_ts > cursor:
                cursor = min_ts
            else:
                cursor = max(cursor + period_ms, max_ts + period_ms)
        else:
            cursor = window_end_ms + 1

    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_funding_all(
    symbol: str,
    period: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
    collect: bool = True,
) -> list[dict[str, object]]:
    """Fetch all available funding-rate history via backward pagination windows."""

    instrument_name = _normalize_funding_instrument(symbol)
    period_ms = _period_to_milliseconds(period)
    window_span_ms = DERIBIT_FUNDING_MAX_POINTS_PER_REQUEST * period_ms
    cursor_end_ms = int(datetime.now(UTC).timestamp() * 1000)
    previous_cursor_end_ms: int | None = None
    rows: list[dict[str, object]] = []

    while cursor_end_ms >= 0:
        if previous_cursor_end_ms is not None and cursor_end_ms >= previous_cursor_end_ms:
            break
        previous_cursor_end_ms = cursor_end_ms

        window_start_ms = max(0, cursor_end_ms - window_span_ms + 1)
        page = _fetch_funding_page(
            symbol=instrument_name,
            start_time_ms=window_start_ms,
            end_time_ms=cursor_end_ms,
        )
        if not page:
            break
        if on_page is not None:
            on_page(page)
        if collect:
            rows.extend(page)

        page_min_ts = min(int(cast(Any, item["timestamp"])) for item in page)
        next_cursor_end_ms = page_min_ts - period_ms
        if next_cursor_end_ms >= cursor_end_ms:
            next_cursor_end_ms = window_start_ms - 1
        cursor_end_ms = next_cursor_end_ms

    if not collect:
        return []

    dedup: dict[int, dict[str, object]] = {}
    for row in rows:
        dedup[int(cast(Any, row["timestamp"]))] = row
    return [dedup[key] for key in sorted(dedup)]


def parse_funding_row(symbol: str, period: str, row: dict[str, object]) -> dict[str, object]:
    """Convert Deribit funding payload to normalized record fields."""

    open_time_ms = int(cast(Any, row["timestamp"]))
    open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=UTC)
    close_time = open_time

    funding_rate = row.get("interest_8h", row.get("interest_1h", 0.0))

    return {
        "symbol": symbol,
        "timeframe": period,
        "open_time": open_time,
        "close_time": close_time,
        "funding_rate": float(cast(Any, funding_rate)),
        "index_price": float(cast(Any, row.get("index_price", 0.0))),
        "mark_price": float(cast(Any, row.get("prev_index_price", 0.0))),
    }


def _period_to_milliseconds(period: str) -> int:
    if period.endswith("m"):
        return int(period[:-1]) * 60_000
    if period.endswith("h"):
        return int(period[:-1]) * 3_600_000
    if period.endswith("d"):
        return int(period[:-1]) * 86_400_000
    raise ValueError(f"Unsupported period '{period}'")


def _fetch_funding_page(symbol: str, start_time_ms: int, end_time_ms: int) -> list[dict[str, object]]:
    """Fetch one funding page from Deribit."""

    params: dict[str, Any] = {
        "instrument_name": symbol,
        "start_timestamp": start_time_ms,
        "end_timestamp": end_time_ms,
    }
    payload = get_json(DERIBIT_FUNDING_URL, params=params)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit funding response format")
    result = payload.get("result")
    if not isinstance(result, list):
        return []

    rows: list[dict[str, object]] = []
    for item in result:
        if not isinstance(item, dict):
            continue
        ts = item.get("timestamp")
        if ts is None:
            continue
        rows.append(item)
    rows.sort(key=lambda x: int(cast(Any, x["timestamp"])))
    return rows
