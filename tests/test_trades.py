"""Tests for trade ingestion adapters and parquet persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake import save_trades_parquet_lake
from ingestion.trades import TradeTick, fetch_trades_range


def test_fetch_trades_range_parses_deribit_rows(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    def _fake_fetch(**kwargs: object) -> list[dict[str, object]]:
        del kwargs
        return [
            {
                "timestamp": int(datetime(2026, 5, 1, 0, 0, tzinfo=UTC).timestamp() * 1000),
                "trade_id": "abc",
                "price": 100.5,
                "amount": 1.25,
                "direction": "buy",
                "liquidation": "m",
            }
        ]

    monkeypatch.setattr("ingestion.exchanges.deribit_trades.fetch_trades_range", _fake_fetch)
    rows = fetch_trades_range(
        exchange="deribit",
        symbol="BTC",
        market="perp",
        start_open_ms=int(datetime(2026, 5, 1, 0, 0, tzinfo=UTC).timestamp() * 1000),
        end_open_ms=int(datetime(2026, 5, 1, 0, 1, tzinfo=UTC).timestamp() * 1000),
    )
    assert len(rows) == 1
    assert rows[0].trade_id == "abc"
    assert rows[0].side == "buy"
    assert rows[0].is_maker is True


def test_save_trades_parquet_lake_writes_dataset(tmp_path: Path) -> None:
    tick = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="t-1",
        trade_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        price=100.0,
        quantity=2.0,
        side="sell",
        is_maker=False,
        source_endpoint="public_trades",
    )
    files = save_trades_parquet_lake(
        trades_by_exchange={"deribit": {"BTC": [tick]}},
        market="perp",
        lake_root=str(tmp_path),
    )
    assert len(files) == 1
    path = Path(files[0])
    assert "dataset_type=trades" in files[0]
    table = pq.ParquetFile(path).read()
    row = table.to_pylist()[0]
    assert row["trade_id"] == "t-1"
    assert row["timeframe"] == "tick"


def test_save_trades_parquet_lake_preserves_same_timestamp_distinct_trade_ids(tmp_path: Path) -> None:
    ts = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    tick_a = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="t-a",
        trade_time=ts,
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=True,
        source_endpoint="public_trades",
    )
    tick_b = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="t-b",
        trade_time=ts,
        price=101.0,
        quantity=2.0,
        side="sell",
        is_maker=False,
        source_endpoint="public_trades",
    )
    files = save_trades_parquet_lake(
        trades_by_exchange={"deribit": {"BTC": [tick_a, tick_b]}},
        market="perp",
        lake_root=str(tmp_path),
    )
    table = pq.ParquetFile(files[0]).read()
    rows = table.to_pylist()
    assert len(rows) == 2
    assert sorted(str(row["trade_id"]) for row in rows) == ["t-a", "t-b"]
