"""Deribit historical option trades adapter."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.http_client import HttpClientError, get_json

DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE = 1000
DERIBIT_OPTION_TRADES_DEFAULT_PAGE_SIZE = 200
DERIBIT_OPTION_TRADES_BASE_URL_DEFAULT = "https://history.deribit.com"
DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL = "https://www.deribit.com"
logger = logging.getLogger(__name__)


def _trades_base_url() -> str:
    """Return Deribit trades API base URL."""

    value = os.getenv("DEPTH_DERIBIT_OPTION_TRADES_BASE_URL", DERIBIT_OPTION_TRADES_BASE_URL_DEFAULT).strip()
    return value.rstrip("/")


def _trades_base_urls() -> list[str]:
    """Return ordered base URL candidates for option trades API."""

    primary = _trades_base_url()
    if primary != DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL:
        return [primary, DERIBIT_OPTION_TRADES_FALLBACK_BASE_URL]
    return [primary]


def _is_route_failure(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no route to host" in message or "network is unreachable" in message


def _extract_result_rows(payload: dict[str, Any]) -> list[dict[str, object]]:
    result = payload.get("result")
    if not isinstance(result, dict):
        raise ValueError("Unexpected Deribit option trades response payload")
    rows = result.get("trades")
    if not isinstance(rows, list):
        return []
    return [cast(dict[str, object], row) for row in rows if isinstance(row, dict)]


def _has_more(payload: dict[str, Any]) -> bool:
    result = payload.get("result")
    if not isinstance(result, dict):
        return False
    return bool(result.get("has_more", False))


def _utc_now_ms() -> int:
    return int(datetime.now(UTC).timestamp() * 1000)


def fetch_option_trades_range(
    *,
    currency: str,
    start_open_ms: int,
    end_open_ms: int,
    count: int = DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE,
) -> list[dict[str, object]]:
    """Fetch Deribit option trades in inclusive millisecond range."""

    if end_open_ms < start_open_ms:
        return []
    if count <= 0:
        raise ValueError("count must be positive")

    normalized_currency = currency.upper().strip()
    if not normalized_currency:
        raise ValueError("currency cannot be empty")
    cursor = start_open_ms
    collected: list[dict[str, object]] = []
    page_size = min(count, DERIBIT_OPTION_TRADES_MAX_PAGE_SIZE)
    max_pages = int(os.getenv("DEPTH_DERIBIT_OPTION_TRADES_MAX_PAGES_PER_RANGE", "5000"))
    pages = 0

    while cursor <= end_open_ms:
        pages += 1
        if max_pages > 0 and pages > max_pages:
            logger.warning(
                "Deribit option trades range page cap reached currency=%s start_ms=%s end_ms=%s max_pages=%s",
                normalized_currency,
                start_open_ms,
                end_open_ms,
                max_pages,
            )
            break
        params = {
            "currency": normalized_currency,
            "kind": "option",
            "start_timestamp": cursor,
            "end_timestamp": end_open_ms,
            "count": page_size,
            "sorting": "asc",
        }
        payload: Any | None = None
        last_error: Exception | None = None
        for base_url in _trades_base_urls():
            try:
                payload = get_json(
                    f"{base_url}/api/v2/public/get_last_trades_by_currency_and_time",
                    params=params,
                )
                break
            except HttpClientError as exc:
                last_error = exc
                if not _is_route_failure(exc):
                    raise
                logger.warning(
                    "Deribit option trades route failure via base_url=%s currency=%s cursor=%s; trying fallback",
                    base_url,
                    normalized_currency,
                    cursor,
                )
        if payload is None:
            assert last_error is not None
            raise last_error
        if not isinstance(payload, dict):
            raise ValueError("Unexpected Deribit option trades response format")
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

    dedup: dict[tuple[int, str, str], dict[str, object]] = {}
    for row in collected:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        instrument_name = str(cast(Any, row).get("instrument_name", ""))
        if start_open_ms <= ts <= end_open_ms:
            dedup[(ts, trade_id, instrument_name)] = row
    return [dedup[key] for key in sorted(dedup)]


def fetch_option_trades_all(
    *,
    currency: str,
) -> list[dict[str, object]]:
    """Fetch available Deribit option trade history by paging backwards in fixed windows."""

    window_ms = 24 * 60 * 60 * 1000
    end_ms = _utc_now_ms()
    all_rows: list[dict[str, object]] = []

    while end_ms > 0:
        start_ms = max(0, end_ms - window_ms + 1)
        page_rows = fetch_option_trades_range(
            currency=currency,
            start_open_ms=start_ms,
            end_open_ms=end_ms,
        )
        if not page_rows:
            break
        all_rows.extend(page_rows)
        first_ts = int(cast(Any, page_rows[0]).get("timestamp", 0))
        if first_ts <= 0:
            break
        end_ms = first_ts - 1

    dedup: dict[tuple[int, str, str], dict[str, object]] = {}
    for row in all_rows:
        ts = int(cast(Any, row).get("timestamp", 0))
        trade_id = str(cast(Any, row).get("trade_id", ""))
        instrument_name = str(cast(Any, row).get("instrument_name", ""))
        dedup[(ts, trade_id, instrument_name)] = row
    return [dedup[key] for key in sorted(dedup)]
