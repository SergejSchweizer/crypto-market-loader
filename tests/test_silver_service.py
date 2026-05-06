"""Tests for silver transformation service."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.silver_service import (
    SilverBuildReport,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_oi_1m_feature_for_symbol,
    build_oi_observed_for_symbol,
    build_silver_for_symbol,
    discover_months,
    discover_symbols,
    write_monthly_sidecars,
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
    dataset_type: str | None = None,
    instrument_type: str | None = None,
) -> None:
    ds = dataset_type or market
    instrument = instrument_type or market
    target = (
        root
        / f"dataset_type={ds}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
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
    assert "close_price" in report.columns

    silver_file = (
        silver
        / "dataset_type=perp"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
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
    assert "columns" in payload

    manifest_paths, plot_paths = write_monthly_sidecars(
        silver_root=str(silver),
        market="perp",
        exchange="deribit",
        symbol=symbol,
        report=report,
        write_manifest=True,
        plot=False,
    )
    assert len(manifest_paths) == 1
    assert plot_paths == []
    monthly_manifest_path = Path(manifest_paths[0])
    assert monthly_manifest_path.exists()
    assert monthly_manifest_path.name == f"{symbol}-2026-05.json"
    monthly_payload = json.loads(monthly_manifest_path.read_text(encoding="utf-8"))
    assert monthly_payload["dataset"] == "perp_1m"
    assert "column_hash" in monthly_payload
    assert "source_silver_datasets" in monthly_payload
    assert "feature_metadata" in monthly_payload
    assert "plot_generated" in monthly_payload


def test_build_funding_observed_and_1m_feature(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "BTC-PERPETUAL"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 10, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.001,
            "index_price": 100.0,
            "mark_price": 99.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 20, tzinfo=UTC),
            "run_id": "r2",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.002,
            "index_price": 101.0,
            "mark_price": 100.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "funding",
            "exchange": "deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 8, 10, tzinfo=UTC),
            "run_id": "r3",
            "source_endpoint": "public_funding",
            "open_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 8, 0, tzinfo=UTC),
            "timeframe": "1m",
            "funding_rate": 0.003,
            "index_price": 102.0,
            "mark_price": 101.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="funding",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="funding",
        instrument_type="perp",
    )

    observed_report = build_funding_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
    )
    assert observed_report.rows_in == 3
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert "funding_time" in observed_report.columns

    feature_report = build_funding_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol=symbol,
        observed_timeframe="1m",
    )
    assert feature_report.rows_out > 0
    assert "funding_rate_last_known" in feature_report.columns

    feature_file = (
        silver
        / "dataset_type=funding_1m_feature"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / f"{symbol}-2026-05.parquet"
    )
    assert feature_file.exists()
    feature = pl.read_parquet(feature_file)
    assert "funding_rate_last_known" in feature.columns
    assert "funding_observed_at" in feature.columns
    assert "minutes_since_funding" in feature.columns
    assert "is_funding_observation_minute" in feature.columns


def test_build_oi_observed_and_1m_feature(tmp_path: Path) -> None:
    bronze = tmp_path / "bronze"
    silver = tmp_path / "silver"
    symbol = "btc_perpetual"
    rows = [
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 30, tzinfo=UTC),
            "run_id": "r1",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 0, 35, tzinfo=UTC),
            "run_id": "r2",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1000.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 2, 30, tzinfo=UTC),
            "run_id": "r3",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1025.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": None,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 3, 30, tzinfo=UTC),
            "run_id": "r4",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1030.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 4, 30, tzinfo=UTC),
            "run_id": "r5",
            "source_endpoint": "public_open_interest",
            "open_time": None,
            "close_time": datetime(2026, 5, 1, 0, 4, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": 1035.0,
            "open_interest_value": 0.0,
        },
        {
            "schema_version": "v1",
            "dataset_type": "oi",
            "exchange": "Deribit",
            "symbol": symbol,
            "instrument_type": "perp",
            "event_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "ingested_at": datetime(2026, 5, 1, 0, 5, 30, tzinfo=UTC),
            "run_id": "r6",
            "source_endpoint": "public_open_interest",
            "open_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "close_time": datetime(2026, 5, 1, 0, 5, tzinfo=UTC),
            "timeframe": "1m",
            "open_interest": -1.0,
            "open_interest_value": 0.0,
        },
    ]
    _write_bronze_day_file(
        bronze,
        market="oi",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        day="2026-05-01",
        rows=rows,
        dataset_type="oi",
        instrument_type="perp",
    )

    observed_report = build_oi_observed_for_symbol(
        bronze_root=str(bronze),
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
    )
    assert observed_report.rows_in == 6
    assert observed_report.rows_out == 2
    assert observed_report.duplicates_removed == 1
    assert observed_report.invalid_ohlc_rows == 3
    assert "oi_source_timestamp" in observed_report.columns

    feature_report = build_oi_1m_feature_for_symbol(
        silver_root=str(silver),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        observed_timeframe="1m",
    )
    assert feature_report.rows_out > 0
    assert "oi_is_observed" in feature_report.columns
    assert "minutes_since_oi_observation" in feature_report.columns

    observed_file = (
        silver
        / "dataset_type=oi_observed"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    observed = pl.read_parquet(observed_file)
    assert observed["symbol"].to_list() == ["BTC-PERPETUAL", "BTC-PERPETUAL"]

    feature_file = (
        silver
        / "dataset_type=oi_1m_feature"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "year=2026"
        / "month=2026-05"
        / "BTC-PERPETUAL-2026-05.parquet"
    )
    feature = pl.read_parquet(feature_file)
    minute_0 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 0, tzinfo=UTC))
    minute_1 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 1, tzinfo=UTC))
    minute_2 = feature.filter(pl.col("timestamp_m1") == datetime(2026, 5, 1, 0, 2, tzinfo=UTC))
    assert minute_0.select("oi_is_observed").item() is True
    assert minute_0.select("oi_is_ffill").item() is False
    assert minute_1.select("oi_is_observed").item() is False
    assert minute_1.select("oi_is_ffill").item() is True
    assert minute_1.select("minutes_since_oi_observation").item() == 1
    assert minute_2.select("oi_is_observed").item() is True
