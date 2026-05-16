"""Tests for Deribit option trade adapter pagination safeguards."""

from __future__ import annotations

import pytest

from ingestion.exchanges import deribit_option_trades


def test_fetch_option_trades_range_stops_when_has_more_false(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url
        calls.append(int(params["start_timestamp"]))
        return {
            "result": {
                "trades": [
                    {
                        "timestamp": int(params["start_timestamp"]),
                        "trade_id": f"id-{len(calls)}",
                        "instrument_name": "BTC-31DEC26-100000-C",
                    },
                ],
                "has_more": False,
            }
        }

    monkeypatch.setattr(deribit_option_trades, "get_json", _fake_get_json)
    rows = deribit_option_trades.fetch_option_trades_range(
        currency="BTC",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_100_000,
        count=1,
    )
    assert len(rows) == 1
    assert len(calls) == 1


def test_fetch_option_trades_range_respects_max_pages_env(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url
        cursor = int(params["start_timestamp"])
        calls.append(cursor)
        return {
            "result": {
                "trades": [
                    {
                        "timestamp": cursor,
                        "trade_id": f"id-{len(calls)}",
                        "instrument_name": "BTC-31DEC26-100000-P",
                    }
                ],
                "has_more": True,
            }
        }

    monkeypatch.setenv("DEPTH_DERIBIT_OPTION_TRADES_MAX_PAGES_PER_RANGE", "3")
    monkeypatch.setattr(deribit_option_trades, "get_json", _fake_get_json)
    rows = deribit_option_trades.fetch_option_trades_range(
        currency="BTC",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_100_000,
        count=1,
    )
    assert len(calls) == 3
    assert len(rows) == 3


def test_option_trades_base_url_env_override(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DEPTH_DERIBIT_OPTION_TRADES_BASE_URL", "https://example.org///")
    assert deribit_option_trades._trades_base_url() == "https://example.org"


def test_option_extract_rows_and_has_more_helpers() -> None:
    payload = {"result": {"trades": [{"timestamp": 1, "trade_id": "a", "instrument_name": "X"}, 1], "has_more": True}}
    assert deribit_option_trades._extract_result_rows(payload) == [{"timestamp": 1, "trade_id": "a", "instrument_name": "X"}]
    assert deribit_option_trades._has_more(payload) is True
    assert deribit_option_trades._has_more({"result": {}}) is False
    assert deribit_option_trades._extract_result_rows({"result": {"trades": "bad"}}) == []
    with pytest.raises(ValueError, match="Unexpected Deribit option trades response payload"):
        deribit_option_trades._extract_result_rows({})


def test_fetch_option_trades_range_validations() -> None:
    assert (
        deribit_option_trades.fetch_option_trades_range(
            currency="BTC",
            start_open_ms=2,
            end_open_ms=1,
        )
        == []
    )
    with pytest.raises(ValueError, match="count must be positive"):
        deribit_option_trades.fetch_option_trades_range(currency="BTC", start_open_ms=1, end_open_ms=2, count=0)
    with pytest.raises(ValueError, match="currency cannot be empty"):
        deribit_option_trades.fetch_option_trades_range(currency=" ", start_open_ms=1, end_open_ms=2, count=1)


def test_fetch_option_trades_range_rejects_non_dict_payload(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(deribit_option_trades, "get_json", lambda *args, **kwargs: [])  # type: ignore[return-value]
    with pytest.raises(ValueError, match="Unexpected Deribit option trades response format"):
        deribit_option_trades.fetch_option_trades_range(currency="BTC", start_open_ms=1, end_open_ms=2, count=1)


def test_fetch_option_trades_range_deduplicates_and_filters_range(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url, params
        return {
            "result": {
                "trades": [
                    {"timestamp": 100, "trade_id": "a", "instrument_name": "I1"},
                    {"timestamp": 100, "trade_id": "a", "instrument_name": "I1"},
                    {"timestamp": 100, "trade_id": "a", "instrument_name": "I2"},
                    {"timestamp": 101, "trade_id": "b", "instrument_name": "I1"},
                    {"timestamp": 9999, "trade_id": "out", "instrument_name": "I1"},
                ],
                "has_more": False,
            }
        }

    monkeypatch.setattr(deribit_option_trades, "get_json", _fake_get_json)
    rows = deribit_option_trades.fetch_option_trades_range(currency="BTC", start_open_ms=100, end_open_ms=101, count=10)
    assert rows == [
        {"timestamp": 100, "trade_id": "a", "instrument_name": "I1"},
        {"timestamp": 100, "trade_id": "a", "instrument_name": "I2"},
        {"timestamp": 101, "trade_id": "b", "instrument_name": "I1"},
    ]


def test_fetch_option_trades_all_deduplicates(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(deribit_option_trades, "_utc_now_ms", lambda: 2_000)
    calls = {"n": 0}

    def _fake_range(**kwargs: object) -> list[dict[str, object]]:
        calls["n"] += 1
        if calls["n"] == 1:
            return [{"timestamp": 1_500, "trade_id": "x", "instrument_name": "I1"}]
        if calls["n"] == 2:
            return [{"timestamp": 1_500, "trade_id": "x", "instrument_name": "I1"}]
        return []

    monkeypatch.setattr(deribit_option_trades, "fetch_option_trades_range", _fake_range)
    rows = deribit_option_trades.fetch_option_trades_all(currency="BTC")
    assert rows == [{"timestamp": 1_500, "trade_id": "x", "instrument_name": "I1"}]
