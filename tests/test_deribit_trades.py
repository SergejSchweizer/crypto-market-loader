"""Tests for Deribit trade adapter pagination safeguards."""

from __future__ import annotations

from ingestion.exchanges import deribit_trades


def test_fetch_trades_range_stops_when_has_more_false(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[int] = []

    def _fake_get_json(url: str, params: dict[str, object]) -> dict[str, object]:
        del url
        calls.append(int(params["start_timestamp"]))
        return {
            "result": {
                "trades": [
                    {"timestamp": int(params["start_timestamp"]), "trade_id": f"id-{len(calls)}"},
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
        cursor = int(params["start_timestamp"])
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
