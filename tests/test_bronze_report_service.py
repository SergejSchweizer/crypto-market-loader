"""Tests for bronze report generation service."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from application.services.bronze_report_service import build_bronze_symbol_reports
from ingestion.lake import save_spot_candles_parquet_lake
from ingestion.spot import SpotCandle


def test_build_bronze_symbol_reports_writes_full_period_report(tmp_path: Path) -> None:
    candle_one = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_two = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 2, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 2, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=101.0,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )
    save_spot_candles_parquet_lake(
        {"deribit": {"BTCUSDT": [candle_one, candle_two]}},
        market="spot",
        lake_root=str(tmp_path),
    )

    report_files = build_bronze_symbol_reports(
        lake_root=str(tmp_path),
        spot_symbols={("deribit", "BTCUSDT", "1m")},
        perp_symbols=set(),
        oi_symbols=set(),
        funding_symbols=set(),
    )
    assert len(report_files) == 1
    payload = json.loads(Path(report_files[0]).read_text(encoding="utf-8"))
    assert payload["dataset"] == "spot_1m"
    assert payload["rows_out"] == 2
    assert payload["min_timestamp"] == "2026-05-01T00:00:00Z"
    assert payload["max_timestamp"] == "2026-05-02T00:00:00Z"
    assert payload["symbols"] == ["BTCUSDT"]
