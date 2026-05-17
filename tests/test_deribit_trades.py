"""Tests for Deribit trade adapter pagination safeguards."""

from __future__ import annotations

from typing import Any, cast

import pytest

from ingestion.exchanges import deribit_trades
from ingestion.http_client import HttpClientError


def test_fetch_trades_range_stops_when_has_more_false(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url
        calls.append(int(cast(Any, params["start_timestamp"])))
        return {
            "result": {
                "trades": [
                    {"timestamp": int(cast(Any, params["start_timestamp"])), "trade_id": f"id-{len(calls)}"},
                ],
                "has_more": False,
            }
        }

    monkeypatch.setattr(deribit_trades, "get_json", _fake_get_json)
    rows = deribit_trades.fetch_trades_range(
        symbol="BTC-PERPETUAL",
        market="perp",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_100_000,
        count=1,
    )
    assert len(rows) == 1
    assert len(calls) == 1


def test_fetch_trades_range_respects_max_pages_env(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url
        cursor = int(cast(Any, params["start_timestamp"]))
        calls.append(cursor)
        return {
            "result": {
                "trades": [{"timestamp": cursor, "trade_id": f"id-{len(calls)}"}],
                "has_more": True,
            }
        }

    monkeypatch.setenv("DEPTH_DERIBIT_TRADES_MAX_PAGES_PER_RANGE", "3")
    monkeypatch.setattr(deribit_trades, "get_json", _fake_get_json)
    rows = deribit_trades.fetch_trades_range(
        symbol="BTC-PERPETUAL",
        market="perp",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_100_000,
        count=1,
    )
    assert len(calls) == 3
    assert len(rows) == 3


def test_trades_base_url_env_override(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_DERIBIT_TRADES_BASE_URL", "https://example.org///")
    assert deribit_trades._trades_base_url() == "https://example.org"


def test_fetch_trades_range_falls_back_on_route_failure(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[str] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        calls.append(url)
        if "history.deribit.com" in url:
            raise HttpClientError("Connection error for x: [Errno 113] No route to host")
        return {
            "result": {
                "trades": [
                    {
                        "timestamp": int(cast(Any, params["start_timestamp"])),
                        "trade_id": "id-1",
                    }
                ],
                "has_more": False,
            }
        }

    monkeypatch.setattr(deribit_trades, "get_json", _fake_get_json)
    rows = deribit_trades.fetch_trades_range(
        symbol="BTC-PERPETUAL",
        market="perp",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_100_000,
        count=1,
    )
    assert len(rows) == 1
    assert any("history.deribit.com" in url for url in calls)
    assert any("www.deribit.com" in url for url in calls)


def test_extract_rows_and_has_more_helpers() -> None:
    payload = {"result": {"trades": [{"timestamp": 1, "trade_id": "a"}, "x"], "has_more": True}}
    assert deribit_trades._extract_result_rows(payload) == [{"timestamp": 1, "trade_id": "a"}]
    assert deribit_trades._has_more(payload) is True
    assert deribit_trades._has_more({"result": {}}) is False
    assert deribit_trades._extract_result_rows({"result": {"trades": "bad"}}) == []
    with pytest.raises(ValueError, match="Unexpected Deribit trades response payload"):
        deribit_trades._extract_result_rows({})


def test_fetch_trades_range_validates_inputs() -> None:
    assert (
        deribit_trades.fetch_trades_range(
            symbol="BTC-PERPETUAL",
            market="perp",
            start_open_ms=2,
            end_open_ms=1,
        )
        == []
    )
    with pytest.raises(ValueError, match="count must be positive"):
        deribit_trades.fetch_trades_range(
            symbol="BTC-PERPETUAL",
            market="perp",
            start_open_ms=1,
            end_open_ms=2,
            count=0,
        )


def test_fetch_trades_range_rejects_non_dict_payload(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(deribit_trades, "get_json", lambda *args, **kwargs: [])  # type: ignore[return-value]
    with pytest.raises(ValueError, match="Unexpected Deribit trades response format"):
        deribit_trades.fetch_trades_range(
            symbol="BTC-PERPETUAL",
            market="perp",
            start_open_ms=1,
            end_open_ms=2,
            count=1,
        )


def test_fetch_trades_range_deduplicates_and_filters_range(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url, params
        return {
            "result": {
                "trades": [
                    {"timestamp": 100, "trade_id": "a"},
                    {"timestamp": 100, "trade_id": "a"},
                    {"timestamp": 101, "trade_id": "b"},
                    {"timestamp": 9999, "trade_id": "out"},
                ],
                "has_more": False,
            }
        }

    monkeypatch.setattr(deribit_trades, "get_json", _fake_get_json)
    rows = deribit_trades.fetch_trades_range(
        symbol="BTC-PERPETUAL",
        market="perp",
        start_open_ms=100,
        end_open_ms=101,
        count=10,
    )
    assert rows == [{"timestamp": 100, "trade_id": "a"}, {"timestamp": 101, "trade_id": "b"}]


def test_fetch_trades_all_with_on_page_and_dedup(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(deribit_trades, "_utc_now_ms", lambda: 2_000)
    calls = {"n": 0}

    def _fake_range(**kwargs: object) -> list[dict[str, object]]:
        calls["n"] += 1
        if calls["n"] == 1:
            return [{"timestamp": 1_500, "trade_id": "x"}, {"timestamp": 1_600, "trade_id": "y"}]
        if calls["n"] == 2:
            return [{"timestamp": 1_500, "trade_id": "x"}]
        return []

    pages: list[list[dict[str, object]]] = []
    monkeypatch.setattr(deribit_trades, "fetch_trades_range", _fake_range)
    rows = deribit_trades.fetch_trades_all(symbol="BTC-PERPETUAL", market="perp", on_page=pages.append)
    assert pages
    assert rows == [{"timestamp": 1_500, "trade_id": "x"}, {"timestamp": 1_600, "trade_id": "y"}]
