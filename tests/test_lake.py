"""Tests for parquet lake helper functions."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake import (
    candle_partition_key,
    candle_record,
    load_open_interest_from_lake,
    load_spot_candles_from_lake,
    merge_and_deduplicate_rows,
    open_times_in_lake,
    partition_path,
    save_open_interest_parquet_lake,
    save_spot_candles_parquet_lake,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=110.0,
        low_price=90.0,
        close_price=105.0,
        volume=15.0,
        quote_volume=1500.0,
        trade_count=42,
    )


def test_partition_key_and_path() -> None:
    candle = _sample_candle()
    key = candle_partition_key(candle=candle, market="spot")
    assert key == ("deribit", "spot", "BTCUSDT", "1m", "2026-04-27")

    result = partition_path("lake/bronze", "spot", key)
    assert str(result).endswith(
        "dataset_type=spot/exchange=deribit/instrument_type=spot/symbol=BTCUSDT/timeframe=1m/month=2026-04/date=2026-04-27"
    )


def test_candle_record_contains_core_fields() -> None:
    candle = _sample_candle()
    ingested_at = datetime(2026, 4, 27, 12, 0, tzinfo=UTC)
    record = candle_record(candle=candle, market="spot", run_id="run-1", ingested_at=ingested_at)

    assert record["dataset_type"] == "spot"
    assert record["instrument_type"] == "spot"
    assert record["run_id"] == "run-1"
    assert record["open_price"] == 100.0
    assert record["close_price"] == 105.0


def test_merge_and_deduplicate_rows_keeps_latest_record() -> None:
    first_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    second_time = datetime(2026, 4, 27, 11, 0, tzinfo=UTC)
    base = {
        "exchange": "deribit",
        "instrument_type": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "open_time": first_time,
        "close": 100.0,
    }
    existing = [base, {**base, "open_time": second_time, "close": 101.0}]
    new = [{**base, "close": 102.0}]

    merged = merge_and_deduplicate_rows(existing=existing, new=new)
    assert len(merged) == 2
    assert merged[0]["open_time"] == first_time
    assert merged[0]["close"] == 102.0


def test_save_spot_candles_parquet_lake_rewrites_single_partition_file(tmp_path: Path) -> None:
    candle_1 = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_2 = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=UTC),
        open_price=100.5,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )

    first = {"deribit": {"BTCUSDT": [candle_1]}}
    second = {"deribit": {"BTCUSDT": [candle_1, candle_2]}}

    files_1 = save_spot_candles_parquet_lake(first, market="spot", lake_root=str(tmp_path))
    files_2 = save_spot_candles_parquet_lake(second, market="spot", lake_root=str(tmp_path))

    assert files_1 == files_2
    assert len(files_2) == 1
    assert "/month=2026-04/date=2026-04-27/data.parquet" in files_2[0]


def test_open_times_in_lake_returns_sorted_unique(tmp_path: Path) -> None:
    candle_1 = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_2 = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=UTC),
        open_price=101.0,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )
    save_spot_candles_parquet_lake({"deribit": {"BTCUSDT": [candle_2, candle_1, candle_1]}}, "spot", str(tmp_path))

    values = open_times_in_lake(
        lake_root=str(tmp_path),
        market="spot",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert values == [candle_1.open_time, candle_2.open_time]


def test_load_spot_candles_from_lake_reads_full_partition_history(tmp_path: Path) -> None:
    candle_apr = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    candle_may = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=110.0,
        high_price=112.0,
        low_price=109.0,
        close_price=111.0,
        volume=9.0,
        quote_volume=999.0,
        trade_count=8,
    )

    save_spot_candles_parquet_lake(
        {"deribit": {"BTCUSDT": [candle_may, candle_apr]}},
        market="spot",
        lake_root=str(tmp_path),
    )

    values = load_spot_candles_from_lake(
        lake_root=str(tmp_path),
        market="spot",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert [item.open_time for item in values] == [candle_apr.open_time, candle_may.open_time]


def test_load_open_interest_from_lake_preserves_observed_rows(tmp_path: Path) -> None:
    observed = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 12, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 12, 0, 59, 999000, tzinfo=UTC),
        open_interest=100000.0,
        open_interest_value=0.0,
    )
    later = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 12, 8, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 12, 8, 59, 999000, tzinfo=UTC),
        open_interest=101500.0,
        open_interest_value=0.0,
    )

    save_open_interest_parquet_lake(
        {"deribit": {"BTCUSDT": [observed, later]}},
        market="perp",
        lake_root=str(tmp_path),
    )

    values = load_open_interest_from_lake(
        lake_root=str(tmp_path),
        market="perp",
        exchange="deribit",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert len(values) == 2
    assert values[0].open_interest == 100000.0
    assert values[1].open_interest == 101500.0


def test_bronze_open_interest_writes_no_engineered_feature_columns(tmp_path: Path) -> None:
    observed = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 12, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 12, 0, 59, 999000, tzinfo=UTC),
        open_interest=100000.0,
        open_interest_value=0.0,
    )
    files = save_open_interest_parquet_lake(
        {"deribit": {"BTCUSDT": [observed]}},
        market="perp",
        lake_root=str(tmp_path),
    )
    parquet_file = pq.ParquetFile(files[0])  # type: ignore[no-untyped-call]
    columns = set(parquet_file.schema_arrow.names)
    assert "open_interest" in columns
    assert "open_interest_value" in columns
    assert "oi_ffill" not in columns
    assert "oi_is_observed" not in columns
    assert "minutes_since_oi_observation" not in columns


def test_bronze_all_symbols_use_same_daily_partition_format(tmp_path: Path) -> None:
    btc = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )
    eth = SpotCandle(
        exchange="deribit",
        symbol="ETHUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=200.0,
        high_price=202.0,
        low_price=199.0,
        close_price=201.0,
        volume=12.0,
        quote_volume=2400.0,
        trade_count=12,
    )
    files = save_spot_candles_parquet_lake(
        {"deribit": {"BTCUSDT": [btc], "ETHUSDT": [eth]}},
        market="spot",
        lake_root=str(tmp_path),
    )
    assert len(files) == 2
    for file_path in files:
        assert re.search(r"/month=\d{4}-\d{2}/date=\d{4}-\d{2}-\d{2}/data\.parquet$", file_path) is not None
