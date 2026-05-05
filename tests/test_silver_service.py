"""Tests for silver transformation service."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.silver_service import (
    build_silver_for_symbol,
    discover_months,
    discover_symbols,
    write_symbol_report,
)

pl = pytest.importorskip("polars")


def _write_bronze_day_file(
    root: Path,
    *,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
    day: str,
    rows: list[dict[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"month={month}"
        / f"date={day}"
        / "data.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def test_build_silver_for_symbol_writes_monthly_parquet_and_aggregated_report(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    base = {
        "schema_version": "v1",
        "dataset_type": "perp",
        "exchange": "deribit",
        "symbol": symbol,
        "instrument_type": "perp",
        "timeframe": "1m",
        "run_id": "r1",
        "source_endpoint": "public_market_data",
        "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        "volume": 1.0,
        "quote_volume": 1.0,
        "trade_count": 1,
    }
    rows_day1 = [
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "open_price": 100.0,
            "high_price": 101.0,
            "low_price": 99.0,
            "close_price": 100.5,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "open_price": 101.0,
            "high_price": 102.0,
            "low_price": 100.0,
            "close_price": 101.5,
        },
    ]
    rows_day2 = [
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 1, tzinfo=UTC),  # duplicate key
            "close_time": datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "open_price": 101.1,
            "high_price": 102.1,
            "low_price": 100.1,
            "close_price": 101.6,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 2, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "open_price": 100.0,
            "high_price": 99.0,  # invalid high
            "low_price": 98.0,
            "close_price": 100.5,
        },
        {
            **base,
            "open_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 3, 59, 999000, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "open_price": None,  # null price
            "high_price": 103.0,
            "low_price": 99.0,
            "close_price": 101.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="perp",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows_day1,
    )
    _write_bronze_day_file(
        bronze,
        market="perp",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-02",
        rows=rows_day2,
    )

    assert discover_symbols(str(bronze), "perp", "deribit") == [symbol]
    assert discover_months(str(bronze), "perp", "deribit", symbol) == ["2026-05"]

    report = build_silver_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        market="perp",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
    )
    assert report.rows_in == 5
    assert report.rows_out == 2
    assert report.duplicates_removed == 1
    assert report.invalid_ohlc_rows == 1
    assert report.null_price_rows == 1
    assert report.period_start == "2026-05"
    assert report.period_end == "2026-05"
    assert report.symbols == [symbol]

    silver_file = (
        silver
        / "dataset_type=perp"
        / "exchange=deribit"
        / "instrument_type=perp"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "month=2026-05"
        / "data.parquet"
    )
    assert silver_file.exists()
    written = pl.read_parquet(silver_file)
    assert written.height == 2

    report_path = write_symbol_report(
        silver_root=str(silver),
        market="perp",
        exchange="deribit",
        symbol=symbol,
        report=report,
    )
    payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    assert payload["rows_out"] == 2
    assert payload["dataset"] == "perp_1m"
