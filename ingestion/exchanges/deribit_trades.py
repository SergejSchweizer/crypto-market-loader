"""Deribit historical trades adapter."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.exchanges.deribit import normalize_symbol
from ingestion.http_client import get_json

DERIBIT_TRADES_MAX_PAGE_SIZE = 1000
DERIBIT_TRADES_DEFAULT_PAGE_SIZE = 200
DERIBIT_TRADES_BASE_URL_DEFAULT = "https://history.deribit.com"
logger = logging.getLogger(__name__)


def _utc_now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def _trades_base_url() -> str:
    """Return Deribit trades API base URL.

    Historical backfill requires the archive host. Can be overridden via
    ``DEPTH_DERIBIT_TRADES_BASE_URL`` for debugging or custom routing.
    """

    value = os.getenv("DEPTH_DERIBIT_TRADES_BASE_URL", DERIBIT_TRADES_BASE_URL_DEFAULT).strip()
    return value.rstrip("/")


def _extract_result_rows(payload: dict[str, Any]) -> list[dict[str, object]]:
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit trades response payload")
    rows = result.get("trades")
    if not isinstance(rows, list):
        return []
    return [cast(dict[str, object], row) for row in rows if isinstance(row, dict)]


def _has_more(payload: dict[str, Any]) -> bool:
    result = payload.get("result")
    if not isinstance(result, dict):
        return False
    return bool(result.get("has_more", False))


def fetch_trades_range(
    *,
    symbol: str,
    market: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int = DERIBIT_TRADES_MAX_PAGE_SIZE,
) -> list[dict[str, object]]:
    """Fetch Deribit trades in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    if count <= 0:
        raise ValueError("count must be positive")

    instrument_name = normalize_symbol(symbol=symbol, market=market)
    cursor = start_open_ms
    collected: list[dict[str, object]] = []
    page_size = min(count, DERIBIT_TRADES_MAX_PAGE_SIZE)
    max_pages = int(os.getenv("DEPTH_DERIBIT_TRADES_MAX_PAGES_PER_RANGE", "5000"))
    pages = 0

    while cursor <= end_open_ms:
        pages += 1
        if max_pages > 0 and pages > max_pages:
            logger.warning(
                "Deribit trades range page cap reached instrument=%s start_ms=%s end_ms=%s max_pages=%s",
                instrument_name,
                start_open_ms,
                end_open_ms,
                max_pages,
            )
            break
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": cursor,
            "end_timestamp": end_open_ms,
            "count": page_size,
            "sorting": "asc",
        }
        payload = get_json(
            f"{_trades_base_url()}/api/v2/public/get_last_trades_by_instrument_and_time",
            params=params,
        )
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Deribit trades response format")
        rows = _extract_result_rows(payload)
        if not rows:
            break
        collected.extend(rows)
        last_ts = int(cast(Any, rows[-1]).get("timestamp", 0))
        if last_ts < cursor:
            break
        cursor = last_ts + 1
        if len(rows) < page_size:
            break
        if not _has_more(payload):
            break
        if pages % 100 == 0:
            logger.info(
                (
                    "Deribit trades range progress instrument=%s start_ms=%s end_ms=%s "
                    "pages=%s cursor_ms=%s rows_collected=%s"
                ),
                instrument_name,
                start_open_ms,
                end_open_ms,
                pages,
                cursor,
                len(collected),
            )

    dedup: dict[tuple[int, str], dict[str, object]] = {}
    for row in collected:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        if start_open_ms <= ts <= end_open_ms:
            dedup[(ts, trade_id)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_trades_all(
    *,
    symbol: str,
    market: str,
    on_page: Callable[[list[dict[str, object]]], None] | None = None,
) -> list[dict[str, object]]:
    """Fetch available Deribit trade history by paging backwards in fixed windows."""

    window_ms = 24 * 60 * 60 * 1000
    end_ms = _utc_now_ms()
    all_rows: list[dict[str, object]] = []

    while end_ms > 0:
        start_ms = max(0, end_ms - window_ms + 1)
        page_rows = fetch_trades_range(
            symbol=symbol,
            market=market,
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        if not page_rows:
            break
        all_rows.extend(page_rows)
        if on_page is not None:
            on_page(page_rows)
        first_ts = int(cast(Any, page_rows[0]).get("timestamp", 0))
        if first_ts <= 0:
            break
        end_ms = first_ts - 1

    dedup: dict[tuple[int, str], dict[str, object]] = {}
    for row in all_rows:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        dedup[(ts, trade_id)] = row
    return [dedup[key] for key in sorted(dedup)]
