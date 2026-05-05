"""Tests for Deribit funding adapter."""

from __future__ import annotations

import pytest

from ingestion.exchanges import deribit_funding


@pytest.mark.parametrize(
    ("symbol_input", "expected_instrument"),
    [
        ("BTC-PERPETUAL", "BTC-PERPETUAL"),
        ("ETH-PERPETUAL", "ETH-PERPETUAL"),
        ("SOL-PERPETUAL", "SOL_USDC-PERPETUAL"),
    ],
)
def test_fetch_funding_range_normalizes_instrument_for_deribit(
    monkeypatch,  # type: ignore[no-untyped-def]
    symbol_input: str,
    expected_instrument: str,
) -> None:
    captured: dict[str, object] = {}

    def fake_get_json(url: str, params: dict[str, object] | None = None):  # type: ignore[no-untyped-def]
        del url
        assert params is not None
        captured["instrument_name"] = params["instrument_name"]
        return {
            "result": [
                {
                    "timestamp": 1_700_000_000_000,
                    "interest_8h": 0.0001,
                    "index_price": 100.0,
                    "prev_index_price": 99.0,
                }
            ]
        }

    monkeypatch.setattr(deribit_funding, "get_json", fake_get_json)

    rows = deribit_funding.fetch_funding_range(
        symbol=symbol_input,
        period="8h",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_000_000,
    )

    assert rows
    assert captured["instrument_name"] == expected_instrument


def test_fetch_funding_range_does_not_skip_when_page_starts_after_cursor(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    period = "8h"
    period_ms = 8 * 3_600_000
    start_open_ms = 0
    end_open_ms = (500 * period_ms) - 1
    first_ts = 100 * period_ms
    second_ts = 200 * period_ms
    calls: list[tuple[int, int]] = []

    def fake_fetch_funding_page(
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict[str, object]]:
        del symbol
        calls.append((start_time_ms, end_time_ms))
        if start_time_ms == start_open_ms:
            return [
                {
                    "timestamp": first_ts,
                    "interest_8h": 0.0001,
                    "index_price": 100.0,
                    "prev_index_price": 99.0,
                }
            ]
        if start_time_ms == first_ts:
            return [
                {
                    "timestamp": second_ts,
                    "interest_8h": 0.0002,
                    "index_price": 101.0,
                    "prev_index_price": 100.0,
                }
            ]
        return []

    monkeypatch.setattr(deribit_funding, "_fetch_funding_page", fake_fetch_funding_page)

    rows = deribit_funding.fetch_funding_range(
        symbol="BTC-PERPETUAL",
        period=period,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )

    assert [int(row["timestamp"]) for row in rows] == [first_ts, second_ts]
    assert calls[0][0] == start_open_ms
    assert calls[1][0] == first_ts


def test_fetch_funding_range_forces_cursor_progress_on_non_advancing_pages(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    period = "8h"
    period_ms = 8 * 3_600_000
    start_open_ms = 0
    end_open_ms = period_ms * 2
    calls: list[int] = []

    def fake_fetch_funding_page(
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict[str, object]]:
        del symbol, end_time_ms
        calls.append(start_time_ms)
        if len(calls) == 1:
            return [{"timestamp": 0, "interest_8h": 0.0001, "index_price": 100.0, "prev_index_price": 99.0}]
        if len(calls) == 2:
            return [{"timestamp": 0, "interest_8h": 0.0002, "index_price": 100.0, "prev_index_price": 99.0}]
        return []

    monkeypatch.setattr(deribit_funding, "_fetch_funding_page", fake_fetch_funding_page)

    rows = deribit_funding.fetch_funding_range(
        symbol="BTC-PERPETUAL",
        period=period,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
    )

    assert calls[:2] == [0, period_ms]
    assert [int(row["timestamp"]) for row in rows] == [0]


def test_fetch_funding_all_paginates_backward_until_empty(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    period = "8h"
    period_ms = 8 * 3_600_000
    now_ms = period_ms * 10
    calls: list[tuple[int, int]] = []

    class _FixedDatetime:
        @staticmethod
        def now(tz=None):  # type: ignore[no-untyped-def]
            del tz
            from datetime import UTC, datetime

            return datetime.fromtimestamp(now_ms / 1000, tz=UTC)

    def fake_fetch_funding_page(
        symbol: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict[str, object]]:
        del symbol
        calls.append((start_time_ms, end_time_ms))
        if len(calls) == 1:
            return [
                {"timestamp": period_ms * 10, "interest_8h": 0.0001, "index_price": 100.0, "prev_index_price": 99.0},
                {"timestamp": period_ms * 9, "interest_8h": 0.0001, "index_price": 100.0, "prev_index_price": 99.0},
            ]
        if len(calls) == 2:
            return [
                {"timestamp": period_ms * 8, "interest_8h": 0.0001, "index_price": 100.0, "prev_index_price": 99.0},
            ]
        return []

    monkeypatch.setattr(deribit_funding, "datetime", _FixedDatetime)
    monkeypatch.setattr(deribit_funding, "_fetch_funding_page", fake_fetch_funding_page)

    rows = deribit_funding.fetch_funding_all(symbol="BTC-PERPETUAL", period=period)
    assert [int(row["timestamp"]) for row in rows] == [period_ms * 8, period_ms * 9, period_ms * 10]
    assert len(calls) == 3
