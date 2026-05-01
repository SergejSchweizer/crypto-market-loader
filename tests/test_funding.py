"""Tests for funding timeframe normalization behavior."""

from __future__ import annotations

import pytest

from ingestion.funding import fetch_funding_all_history, fetch_funding_range, normalize_funding_timeframe
from ingestion.http_client import HttpClientHttpError


def test_normalize_funding_timeframe_uses_native_deribit_interval() -> None:
    assert normalize_funding_timeframe("deribit", "1m") == "8h"
    assert normalize_funding_timeframe("deribit", "M1") == "8h"
    assert normalize_funding_timeframe("deribit", "8h") == "8h"


def test_normalize_funding_timeframe_rejects_unsupported_values() -> None:
    with pytest.raises(ValueError, match="Unsupported funding timeframe"):
        normalize_funding_timeframe("deribit", "5m")


def test_fetch_funding_all_history_returns_empty_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_funding_all(symbol: str, period: str) -> list[dict[str, object]]:
        del symbol, period
        raise HttpClientHttpError("bad request", status_code=400, retryable=False)

    monkeypatch.setattr("ingestion.funding.deribit_funding.fetch_funding_all", fake_fetch_funding_all)

    rows = fetch_funding_all_history(exchange="deribit", symbol="SOL", interval="1m", market="perp")
    assert rows == []


def test_fetch_funding_range_returns_empty_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_fetch_funding_range(
        symbol: str,
        period: str,
        start_open_ms: int,
        end_open_ms: int,
    ) -> list[dict[str, object]]:
        del symbol, period, start_open_ms, end_open_ms
        raise HttpClientHttpError("bad request", status_code=400, retryable=False)

    monkeypatch.setattr("ingestion.funding.deribit_funding.fetch_funding_range", fake_fetch_funding_range)

    rows = fetch_funding_range(
        exchange="deribit",
        symbol="SOL",
        interval="1m",
        start_open_ms=0,
        end_open_ms=60_000,
        market="perp",
    )
    assert rows == []
