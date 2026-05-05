"""Tests for silver transformation service."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.silver_service import (
    _downsample_series,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_samples_plot_filename,
    build_silver_for_symbol,
    discover_months,
    discover_symbols,
    write_symbol_plot,
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
    assert "columns" in payload


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
        / "month=2026-05"
        / "data.parquet"
    )
    assert feature_file.exists()
    feature = pl.read_parquet(feature_file)
    assert "funding_rate_last_known" in feature.columns
    assert "funding_observed_at" in feature.columns
    assert "minutes_since_funding" in feature.columns
    assert "is_funding_observation_minute" in feature.columns


def test_build_samples_plot_filename_uses_required_schema() -> None:
    file_name = build_samples_plot_filename(
        zone="silver",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        market="perp",
    )
    assert file_name == "silver_deribit_BTC-PERPETUAL_perp.png"


def test_write_symbol_plot_writes_png_under_samples(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    samples = tmp_path / "samples"
    symbol = "BTC-PERPETUAL"
    month_file = (
        silver
        / "dataset_type=perp"
        / "exchange=deribit"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / "month=2026-05"
        / "data.parquet"
    )
    month_file.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "open_time": [datetime(2026, 5, 1, 0, 0, tzinfo=UTC), datetime(2026, 5, 1, 0, 1, tzinfo=UTC)],
            "close_price": [100.0, 101.0],
        }
    ).write_parquet(month_file)

    path = write_symbol_plot(
        silver_root=str(silver),
        zone="perp",
        exchange="deribit",
        symbol=symbol,
        timeframe="1m",
        period_start="2026-05",
        period_end="2026-05",
        samples_root=str(samples),
    )
    assert path is not None
    assert Path(path).exists()
    assert Path(path).name == "perp_deribit_BTC-PERPETUAL_perp.png"


def test_downsample_series_caps_to_max_points() -> None:
    xs = list(range(10_000))
    ys = [float(i) for i in range(10_000)]
    sampled_xs, sampled_ys = _downsample_series(xs, ys, max_points=3000)
    assert len(sampled_xs) == len(sampled_ys)
    assert len(sampled_xs) <= 3000
    assert sampled_xs[0] == 0
    assert sampled_xs[-1] == 9999
