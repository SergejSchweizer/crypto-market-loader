"""Deribit option instruments metadata adapter."""

from __future__ import annotations

import os
from typing import Any, cast

from ingestion.http_client import get_json

DERIBIT_OPTION_INSTRUMENTS_BASE_URL_DEFAULT = "https://www.deribit.com"


def _base_url() -> str:
    value = os.getenv("DEPTH_DERIBIT_OPTION_INSTRUMENTS_BASE_URL", DERIBIT_OPTION_INSTRUMENTS_BASE_URL_DEFAULT).strip()
    return value.rstrip("/")


def fetch_option_instruments(currency: str) -> list[dict[str, object]]:
    """Fetch option instruments metadata for one underlying currency."""

    normalized_currency = currency.upper().strip()
    if not normalized_currency:
        raise ValueError("currency cannot be empty")
    payload = get_json(
        f"{_base_url()}/api/v2/public/get_instruments",
        params={"currency": normalized_currency, "kind": "option", "expired": "true"},
    )
    if not isinstance(payload, dict):
        raise ValueError("Unexpected Deribit option instruments response format")
    result = payload.get("result")
    if not isinstance(result, list):
        return []
    rows = [cast(dict[str, object], row) for row in result if isinstance(row, dict)]
    rows.sort(key=lambda row: str(cast(Any, row).get("instrument_name", "")))
    return rows
