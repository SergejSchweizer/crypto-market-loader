"""Tests for Deribit funding adapter."""

from __future__ import annotations

from ingestion.exchanges import deribit_funding


def test_fetch_funding_range_normalizes_sol_instrument_for_deribit(monkeypatch) -> None:  # type: ignore[no-untyped-def]
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
        symbol="SOL-PERPETUAL",
        period="8h",
        start_open_ms=1_700_000_000_000,
        end_open_ms=1_700_000_000_000,
    )

    assert rows
    assert captured["instrument_name"] == "SOL_USDC-PERPETUAL"


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
