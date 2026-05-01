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
