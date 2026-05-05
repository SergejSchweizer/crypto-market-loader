"""Tests for Deribit-only open-interest ingestion interface."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ingestion import open_interest as oi
from ingestion.exchanges import deribit_open_interest
from ingestion.http_client import HttpClientError, HttpClientHttpError


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


def test_fetch_open_interest_range_returns_empty_on_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_connection_error(**kwargs: object) -> list[dict[str, object]]:
        del kwargs
        raise HttpClientError("Connection error")

    monkeypatch.setattr(deribit_open_interest, "fetch_open_interest_range", _raise_connection_error)
    rows = oi.fetch_open_interest_range(
        exchange="deribit",
        symbol="BTC",
        interval="1m",
        start_open_ms=0,
        end_open_ms=60_000,
        market="perp",
    )
    assert rows == []


@pytest.mark.parametrize(
    ("symbol_input", "expected_instrument"),
    [
        ("BTC", "BTC-PERPETUAL"),
        ("ETH", "ETH-PERPETUAL"),
        ("SOL", "SOL_USDC-PERPETUAL"),
    ],
)
def test_fetch_open_interest_all_maps_symbols_to_expected_deribit_instrument(
    monkeypatch: pytest.MonkeyPatch,
    symbol_input: str,
    expected_instrument: str,
) -> None:
    captured: list[str] = []

    def _fake_get_json(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, kwargs
        assert params is not None
        captured.append(str(params["instrument_name"]))
        return {"result": {"settlements": []}}

    monkeypatch.setattr(deribit_open_interest, "get_json", _fake_get_json)

    rows = oi.fetch_open_interest_all_history(
        exchange="deribit",
        symbol=symbol_input,
        interval="1m",
        market="perp",
    )

    assert rows == []
    assert captured == [expected_instrument]


def test_fetch_open_interest_all_stops_when_continuation_is_none_string(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str | None] = []

    def _fake_get_json(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, kwargs
        assert params is not None
        continuation = params.get("continuation")
        calls.append(str(continuation) if continuation is not None else None)
        if continuation is None:
            return {
                "result": {
                    "settlements": [
                        {"timestamp": 2, "position": 200.0},
                        {"timestamp": 1, "position": 100.0},
                    ],
                    "continuation": "none",
                }
            }
        raise AssertionError("Expected loop to stop before requesting continuation='none'")

    monkeypatch.setattr(deribit_open_interest, "get_json", _fake_get_json)

    rows = deribit_open_interest.fetch_open_interest_all(symbol="BTC-PERPETUAL", period="1m")
    assert calls == [None]
    assert [row["timestamp"] for row in rows] == [1, 2]


def test_fetch_open_interest_all_breaks_on_repeated_continuation(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str | None] = []

    def _fake_get_json(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, kwargs
        assert params is not None
        continuation = params.get("continuation")
        calls.append(str(continuation) if continuation is not None else None)
        if continuation is None:
            return {
                "result": {
                    "settlements": [{"timestamp": 3, "position": 300.0}],
                    "continuation": "repeat-token",
                }
            }
        if continuation == "repeat-token":
            return {
                "result": {
                    "settlements": [{"timestamp": 2, "position": 200.0}],
                    "continuation": "repeat-token",
                }
            }
        raise AssertionError(f"Unexpected continuation: {continuation}")

    monkeypatch.setattr(deribit_open_interest, "get_json", _fake_get_json)

    rows = deribit_open_interest.fetch_open_interest_all(symbol="BTC-PERPETUAL", period="1m")
    assert calls == [None, "repeat-token"]
    assert [row["timestamp"] for row in rows] == [2, 3]


def test_fetch_open_interest_all_breaks_on_repeated_page_window(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str | None] = []

    def _fake_get_json(url: str, params: dict[str, object] | None = None, **kwargs: object) -> object:
        del url, kwargs
        assert params is not None
        continuation = params.get("continuation")
        calls.append(str(continuation) if continuation is not None else None)
        if continuation is None:
            return {
                "result": {
                    "settlements": [{"timestamp": 3, "position": 300.0}, {"timestamp": 2, "position": 200.0}],
                    "continuation": "token-a",
                }
            }
        return {
            "result": {
                "settlements": [{"timestamp": 3, "position": 301.0}, {"timestamp": 2, "position": 201.0}],
                "continuation": "token-b",
            }
        }

    monkeypatch.setattr(deribit_open_interest, "get_json", _fake_get_json)

    rows = deribit_open_interest.fetch_open_interest_all(symbol="BTC-PERPETUAL", period="1m")
    assert calls == [None, "token-a"]
    assert [row["timestamp"] for row in rows] == [2, 3]


def test_parse_open_interest_row_preserves_raw_timestamp() -> None:
    ts_ms = int(datetime(2026, 4, 28, 12, 8, 34, tzinfo=UTC).timestamp() * 1000)
    parsed = deribit_open_interest.parse_open_interest_row(
        symbol="BTC-PERPETUAL",
        period="1m",
        row={"timestamp": ts_ms, "open_interest": 101500.0},
    )
    assert parsed["open_time"] == datetime(2026, 4, 28, 12, 8, 34, tzinfo=UTC)
    assert parsed["close_time"] == datetime(2026, 4, 28, 12, 8, 34, tzinfo=UTC)
