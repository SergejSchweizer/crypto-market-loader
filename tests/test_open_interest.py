"""Tests for Deribit-only open-interest ingestion interface."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion import open_interest as oi
from ingestion.exchanges import deribit_open_interest
from ingestion.http_client import HttpClientHttpError


def test_normalize_open_interest_timeframe_deribit() -> None:
    assert oi.normalize_open_interest_timeframe("deribit", "M1") == "1m"


def test_fetch_open_interest_all_history_returns_empty_for_spot() -> None:
    rows = oi.fetch_open_interest_all_history(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        market="spot",
    )
    assert rows == []


def test_fetch_open_interest_range_deribit_historical(monkeypatch: pytest.MonkeyPatch) -> None:
    point_ms = int(datetime(2026, 4, 28, 9, 2, tzinfo=UTC).timestamp() * 1000)
    monkeypatch.setattr(
        deribit_open_interest,
        "fetch_open_interest_range",
        lambda **kwargs: [{"timestamp": point_ms, "open_interest": 1000.0}],
    )
    monkeypatch.setattr(
        deribit_open_interest,
        "parse_open_interest_row",
        lambda symbol, period, row: {
            "open_time": datetime(2026, 4, 28, 9, 2, tzinfo=UTC),
            "close_time": datetime(2026, 4, 28, 9, 2, 59, 999000, tzinfo=UTC),
            "open_interest": float(row["open_interest"]),
            "open_interest_value": 0.0,
        },
    )

    start = int(datetime(2026, 4, 28, 9, 0, tzinfo=UTC).timestamp() * 1000)
    end = int(datetime(2026, 4, 28, 9, 5, tzinfo=UTC).timestamp() * 1000)

    rows = oi.fetch_open_interest_range(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        start_open_ms=start,
        end_open_ms=end,
        market="perp",
    )
    assert len(rows) == 1
    assert rows[0].exchange == "deribit"
    assert rows[0].open_interest == 1000.0


def test_fetch_open_interest_all_returns_empty_on_http_400(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_http_400(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, params, kwargs
        raise HttpClientHttpError("HTTP error 400 for test", status_code=400, retryable=False)

    monkeypatch.setattr(deribit_open_interest, "get_json", _raise_http_400)

    rows = oi.fetch_open_interest_all_history(
        exchange="deribit",
        symbol="SOL",
        interval="1m",
        market="perp",
    )
    assert rows == []


def test_fetch_open_interest_all_maps_sol_to_usdc_perpetual(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[str] = []

    def _fake_get_json(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, kwargs
        assert params is not None
        captured.append(str(params["instrument_name"]))
        return {"result": {"settlements": []}}

    monkeypatch.setattr(deribit_open_interest, "get_json", _fake_get_json)

    rows = oi.fetch_open_interest_all_history(
        exchange="deribit",
        symbol="SOL",
        interval="1m",
        market="perp",
    )

    assert rows == []
    assert captured == ["SOL_USDC-PERPETUAL"]


def test_expand_open_interest_to_interval_grid_forward_fills() -> None:
    first = oi.OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 4, 28, 12, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 28, 12, 0, 59, 999000, tzinfo=UTC),
        open_interest=100000.0,
        open_interest_value=0.0,
    )
    second = oi.OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 4, 28, 12, 8, tzinfo=UTC),
        close_time=datetime(2026, 4, 28, 12, 8, 59, 999000, tzinfo=UTC),
        open_interest=101500.0,
        open_interest_value=0.0,
    )

    rows = oi.expand_open_interest_to_interval_grid([first, second])
    assert len(rows) == 9
    assert rows[0].open_time == datetime(2026, 4, 28, 12, 0, tzinfo=UTC)
    assert rows[0].oi_is_observed is True
    assert rows[0].minutes_since_oi_observation == 0
    assert rows[0].oi_ffill == 100000.0

    assert rows[1].open_time == datetime(2026, 4, 28, 12, 1, tzinfo=UTC)
    assert rows[1].oi_is_observed is False
    assert rows[1].minutes_since_oi_observation == 1
    assert rows[1].oi_ffill == 100000.0
    assert rows[7].open_time == datetime(2026, 4, 28, 12, 7, tzinfo=UTC)
    assert rows[7].minutes_since_oi_observation == 7
    assert rows[7].oi_ffill == 100000.0

    assert rows[8].open_time == datetime(2026, 4, 28, 12, 8, tzinfo=UTC)
    assert rows[8].oi_is_observed is True
    assert rows[8].minutes_since_oi_observation == 0
    assert rows[8].oi_ffill == 101500.0
