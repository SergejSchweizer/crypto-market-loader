"""Tests for Deribit-only spot/perpetual ingestion parsing and validation."""

from __future__ import annotations

from datetime import UTC
from typing import Literal

import pytest

from ingestion.exchanges import deribit
from ingestion.http_client import HttpClientError
from ingestion.spot import (
    fetch_candles,
    fetch_candles_all_history,
    fetch_candles_range,
    normalize_timeframe,
    parse_kline,
)


def test_parse_deribit_kline_maps_fields() -> None:
    row = [1714478400000, "64000.0", "64200.0", "63850.0", "64100.0", "120.5", 1714481999999, "7720000.0", 2300]

    candle = parse_kline("deribit", "BTC-PERPETUAL", "1m", row)

    assert candle.symbol == "BTC-PERPETUAL"
    assert candle.interval == "1m"
    assert candle.open_time.tzinfo == UTC
    assert candle.close_price == pytest.approx(64100.0)
    assert candle.volume == pytest.approx(120.5)
    assert candle.quote_volume == pytest.approx(7720000.0)
    assert candle.trade_count == 2300


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("M1", "1m"),
        ("1m", "1m"),
    ],
)
def test_normalize_deribit_timeframe_aliases(value: str, expected: str) -> None:
    assert normalize_timeframe("deribit", value) == expected


@pytest.mark.parametrize("value", ["M5", "H1", "H6", "D1", "5m", "1h", "1d"])
def test_normalize_deribit_timeframe_rejects_non_1m(value: str) -> None:
    with pytest.raises(ValueError, match="Unsupported timeframe"):
        normalize_timeframe("deribit", value)


def test_deribit_symbol_normalization_perp_aliases() -> None:
    assert deribit.normalize_symbol("BTC", "perp") == "BTC-PERPETUAL"
    assert deribit.normalize_symbol("ETHUSDT", "perp") == "ETH-PERPETUAL"
    assert deribit.normalize_symbol("SOL", "perp") == "SOL-PERPETUAL"


def test_fetch_deribit_candles_respects_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_utc_now_ms() -> int:
        return 10_000

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s, params
        return {
            "result": {
                "status": "ok",
                "ticks": [1000, 2000, 3000, 4000],
                "open": [1, 2, 3, 4],
                "high": [1, 2, 3, 4],
                "low": [1, 2, 3, 4],
                "close": [1, 2, 3, 4],
                "volume": [1, 2, 3, 4],
            }
        }

    monkeypatch.setattr(deribit_exchange, "_utc_now_ms", fake_utc_now_ms)
    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    candles = fetch_candles(exchange="deribit", market="perp", symbol="BTC", interval="1m", limit=3)
    assert len(candles) == 3
    assert [int(item.open_time.timestamp()) for item in candles] == [2, 3, 4]
    assert all(item.quote_volume is None for item in candles)


@pytest.mark.parametrize(
    ("market", "symbol", "expected_instrument", "expected_symbol"),
    [
        ("spot", "BTCUSDT", "BTC_USDC", "BTC_USDC"),
        ("perp", "BTC", "BTC-PERPETUAL", "BTC-PERPETUAL"),
        ("spot", "SOL", "SOL_USDC", "SOL_USDC"),
        ("perp", "SOL", "SOL-PERPETUAL", "SOL-PERPETUAL"),
    ],
)
def test_fetch_deribit_routes_spot_and_perp_symbols(
    monkeypatch: pytest.MonkeyPatch,
    market: Literal["spot", "perp"],
    symbol: str,
    expected_instrument: str,
    expected_symbol: str,
) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    captured_instruments: list[str] = []

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, timeout_s
        assert params is not None
        captured_instruments.append(str(params["instrument_name"]))
        return {
            "result": {
                "status": "ok",
                "ticks": [1000, 2000],
                "open": [1, 2],
                "high": [1, 2],
                "low": [1, 2],
                "close": [1, 2],
                "volume": [1, 2],
            }
        }

    monkeypatch.setattr(deribit_exchange, "_utc_now_ms", lambda: 10_000)
    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    candles = fetch_candles(exchange="deribit", market=market, symbol=symbol, interval="1m", limit=2)
    assert len(candles) == 2
    assert captured_instruments[0] == expected_instrument
    assert candles[0].symbol == expected_symbol


def test_fetch_all_history_deribit(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_fetch_klines_all_deribit(symbol: str, market: str, interval: str) -> list[list[object]]:
        assert symbol == "BTC"
        assert market == "perp"
        assert interval == "1m"
        return [[1000, "1", "1", "1", "1", "1", 1999, "1", 1]]

    monkeypatch.setattr(deribit_exchange, "fetch_klines_all", fake_fetch_klines_all_deribit)

    candles = fetch_candles_all_history(exchange="deribit", market="perp", symbol="BTC", interval="1m")

    assert len(candles) == 1
    assert candles[0].symbol == "BTC-PERPETUAL"


def test_fetch_candles_range_returns_empty_on_http_client_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_fetch_klines_range(**kwargs: object) -> list[list[object]]:
        del kwargs
        raise HttpClientError("Connection error")

    monkeypatch.setattr(deribit_exchange, "fetch_klines_range", fake_fetch_klines_range)

    rows = fetch_candles_range(
        exchange="deribit",
        market="perp",
        symbol="ETH",
        interval="1m",
        start_open_ms=1_000,
        end_open_ms=2_000,
    )

    assert rows == []


def test_fetch_candles_range_returns_empty_on_deribit_no_data_status(monkeypatch: pytest.MonkeyPatch) -> None:
    from ingestion.exchanges import deribit as deribit_exchange

    def fake_get_json(url: str, params: dict[str, object] | None = None, timeout_s: float = 15.0) -> object:
        del url, params, timeout_s
        return {
            "result": {
                "status": "no_data",
                "ticks": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
            }
        }

    monkeypatch.setattr(deribit_exchange, "get_json", fake_get_json)

    rows = fetch_candles_range(
        exchange="deribit",
        market="spot",
        symbol="SOL",
        interval="1m",
        start_open_ms=1_000,
        end_open_ms=120_000,
    )

    assert rows == []


def test_deribit_resolution_interval_and_symbol_guards() -> None:
    assert deribit.to_deribit_resolution("1m") == "1"
    assert deribit.to_deribit_resolution("1h") == "60"
    assert deribit.to_deribit_resolution("1d") == "1D"
    with pytest.raises(ValueError, match="Cannot map interval"):
        deribit.to_deribit_resolution("1x")

    assert deribit.interval_to_milliseconds("1m") == 60_000
    assert deribit.interval_to_milliseconds("1h") == 3_600_000
    assert deribit.interval_to_milliseconds("1d") == 86_400_000
    with pytest.raises(ValueError, match="Unsupported interval"):
        deribit.interval_to_milliseconds("1x")

    with pytest.raises(ValueError, match="market must be either"):
        deribit.normalize_symbol("BTC", "x")


def test_deribit_fetch_chart_page_validation_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deribit, "get_json", lambda *args, **kwargs: [])
    with pytest.raises(ValueError, match="Unexpected Deribit response format"):
        deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 1)

    monkeypatch.setattr(deribit, "get_json", lambda *args, **kwargs: {"result": []})
    with pytest.raises(ValueError, match="Unexpected Deribit response payload"):
        deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 1)

    monkeypatch.setattr(
        deribit,
        "get_json",
        lambda *args, **kwargs: {"result": {"status": "bad", "ticks": [], "open": [], "high": [], "low": [], "close": [], "volume": []}},
    )
    with pytest.raises(ValueError, match="Deribit chart status"):
        deribit._fetch_chart_page("BTC-PERPETUAL", "1", 60_000, 0, 1)


def test_deribit_fetch_klines_all_and_range_edge_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(deribit, "_utc_now_ms", lambda: 1_000)
    calls = {"n": 0}

    def _page(**kwargs: object) -> list[list[object]]:
        calls["n"] += 1
        if calls["n"] == 1:
            return [[0, "1", "1", "1", "1", "1", 999, None, 0, "0", "0", "0"]]
        return []

    monkeypatch.setattr(deribit, "_fetch_chart_page", lambda **kwargs: _page(**kwargs))
    seen_pages: list[int] = []
    rows_all = deribit.fetch_klines_all("BTC", "perp", "1m", on_page=lambda page: seen_pages.append(len(page)))
    assert rows_all
    assert seen_pages == [1]

    monkeypatch.setattr(deribit, "_fetch_chart_page", lambda **kwargs: [])
    assert deribit.fetch_klines_range("BTC", "perp", "1m", 10, 5) == []
