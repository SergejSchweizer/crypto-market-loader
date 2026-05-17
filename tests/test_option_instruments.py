"""Tests for option instruments ingestion adapters."""

from __future__ import annotations

import logging

import pytest

from application.dto import OptionInstrumentFetchTaskDTO
from application.services.fetch_service import fetch_option_instrument_tasks_parallel
from ingestion.exchanges import deribit_option_instruments
from ingestion.option_instruments import fetch_option_instruments


def test_deribit_option_instruments_fetch_sorts_and_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_get_json(url: str, params: dict[str, str]) -> dict[str, object]:
        assert url.endswith("/api/v2/public/get_instruments")
        assert params == {"currency": "BTC", "kind": "option", "expired": "true"}
        return {
            "result": [
                {"instrument_name": "BTC-29MAR24-70000-C", "strike": 70000},
                "ignore-me",
                {"instrument_name": "BTC-29MAR24-60000-C", "strike": 60000},
            ]
        }

    monkeypatch.setattr(deribit_option_instruments, "get_json", _fake_get_json)
    rows = deribit_option_instruments.fetch_option_instruments("btc")
    assert [row["instrument_name"] for row in rows] == ["BTC-29MAR24-60000-C", "BTC-29MAR24-70000-C"]


def test_deribit_option_instruments_fetch_rejects_empty_currency() -> None:
    with pytest.raises(ValueError, match="currency cannot be empty"):
        deribit_option_instruments.fetch_option_instruments("   ")


def test_fetch_option_instruments_normalizes_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        deribit_option_instruments,
        "fetch_option_instruments",
        lambda currency: [
            {
                "instrument_name": "BTC-31MAY24-60000-C",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "settlement_currency": "BTC",
                "strike": 60000,
                "option_type": "CALL",
                "creation_timestamp": 1716500000000,
                "expiration_timestamp": 1717200000000,
                "contract_size": 1.0,
                "tick_size": 0.5,
            }
        ],
    )
    rows = fetch_option_instruments(exchange="deribit", symbol="btcusdt")
    assert len(rows) == 1
    assert rows[0].symbol == "BTC"
    assert rows[0].option_type == "call"
    assert rows[0].interval == "snapshot"


def test_fetch_option_instruments_rejects_unsupported_exchange() -> None:
    with pytest.raises(ValueError, match="Unsupported exchange"):
        fetch_option_instruments(exchange="binance", symbol="BTC")


def test_fetch_option_instruments_applies_start_date_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        deribit_option_instruments,
        "fetch_option_instruments",
        lambda currency: [
            {
                "instrument_name": "BTC-OLD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "settlement_currency": "BTC",
                "strike": 50000,
                "option_type": "call",
                "creation_timestamp": 1716400000000,
                "expiration_timestamp": 1716500000000,
                "contract_size": 1.0,
                "tick_size": 0.5,
            },
            {
                "instrument_name": "BTC-NEW",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "settlement_currency": "BTC",
                "strike": 60000,
                "option_type": "call",
                "creation_timestamp": 1716600000000,
                "expiration_timestamp": 1716700000000,
                "contract_size": 1.0,
                "tick_size": 0.5,
            },
        ],
    )
    rows = fetch_option_instruments(exchange="deribit", symbol="BTC", start_open_ms_bound=1716500000000)
    assert [row.instrument_name for row in rows] == ["BTC-NEW"]


def test_fetch_option_instrument_tasks_parallel_collects_success_and_error() -> None:
    tasks = [
        OptionInstrumentFetchTaskDTO(exchange="deribit", symbol="BTC"),
        OptionInstrumentFetchTaskDTO(exchange="deribit", symbol="ETH"),
    ]

    def _fake_symbol_fetcher(exchange: str, symbol: str):  # type: ignore[no-untyped-def]
        if symbol == "ETH":
            raise RuntimeError("boom")
        return []

    result = fetch_option_instrument_tasks_parallel(
        tasks=tasks,
        logger=logging.getLogger("test_option_instruments"),
        symbol_fetcher=_fake_symbol_fetcher,
    )
    assert ("deribit", "BTC") in result.rows
    assert result.rows[("deribit", "BTC")] == []
    assert result.errors[("deribit", "ETH")] == "boom"
