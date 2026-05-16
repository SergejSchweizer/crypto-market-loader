"""Tests for parquet lake helper functions."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ingestion.funding import FundingPoint
from ingestion.lake import (
    candle_partition_key,
    candle_record,
    ensure_bronze_sidecars,
    load_combined_dataframe_from_lake,
    load_funding_from_lake,
    load_open_interest_from_lake,
    load_spot_candles_from_lake,
    merge_and_deduplicate_rows,
    open_times_in_lake,
    partition_path,
    save_funding_parquet_lake,
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
        "dataset_type=spot/exchange=deribit/instrument_type=spot/symbol=BTCUSDT/timeframe=1m/year=2026/month=2026-04/date=2026-04-27"
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
    assert "/year=2026/month=2026-04/date=2026-04-27/data.parquet" in files_2[0]
    parquet_path = Path(files_2[0])
    assert parquet_path.with_suffix(".json").exists()
    assert parquet_path.with_suffix(".png").exists()


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
        assert re.search(r"/year=\d{4}/month=\d{4}-\d{2}/date=\d{4}-\d{2}-\d{2}/data\.parquet$", file_path) is not None


def test_ensure_bronze_sidecars_backfills_missing_sidecars(tmp_path: Path) -> None:
    candle = SpotCandle(
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
    files = save_spot_candles_parquet_lake(
        {"deribit": {"BTCUSDT": [candle]}},
        market="spot",
        lake_root=str(tmp_path),
    )
    parquet_path = Path(files[0])
    parquet_path.with_suffix(".json").unlink()
    parquet_path.with_suffix(".png").unlink()

    repaired = ensure_bronze_sidecars(lake_root=str(tmp_path), dataset_types=["spot"])

    assert repaired == [str(parquet_path.resolve())]
    assert parquet_path.with_suffix(".json").exists()
    assert parquet_path.with_suffix(".png").exists()
    manifest = json.loads(parquet_path.with_suffix(".json").read_text(encoding="utf-8"))
    assert "feature_metadata" in manifest
    assert "open_price" in manifest["feature_metadata"]


def test_load_funding_from_lake_reads_saved_rows(tmp_path: Path) -> None:
    row = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 7, 59, 59, tzinfo=UTC),
        funding_rate=0.001,
        index_price=100000.0,
        mark_price=100100.0,
    )
    save_funding_parquet_lake(
        funding_by_exchange={"deribit": {"BTC-PERPETUAL": [row]}},
        market="perp",
        lake_root=str(tmp_path),
    )
    values = load_funding_from_lake(
        lake_root=str(tmp_path),
        market="perp",
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="8h",
    )
    assert len(values) == 1
    assert values[0].funding_rate == 0.001


def test_load_combined_dataframe_limit_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="limit must be positive"):
        load_combined_dataframe_from_lake(lake_root=str(tmp_path), limit=0)


def test_load_combined_dataframe_applies_filters_and_open_interest(tmp_path: Path) -> None:
    btc_perp = SpotCandle(
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
    eth = SpotCandle(
        exchange="deribit",
        symbol="ETHUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
        open_price=200.0,
        high_price=201.0,
        low_price=199.0,
        close_price=200.5,
        volume=20.0,
        quote_volume=2000.0,
        trade_count=20,
    )
    save_spot_candles_parquet_lake({"deribit": {"BTCUSDT": [btc_perp]}}, "perp", str(tmp_path))
    save_spot_candles_parquet_lake({"deribit": {"ETHUSDT": [eth]}}, "spot", str(tmp_path))
    oi = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )
    save_open_interest_parquet_lake({"deribit": {"BTCUSDT": [oi]}}, "perp", str(tmp_path))

    frame = load_combined_dataframe_from_lake(
        lake_root=str(tmp_path),
        exchanges=["deribit"],
        symbols=["BTCUSDT"],
        instrument_types=["perp"],
        timeframes=["1m"],
        start_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        end_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        include_open_interest=True,
    )
    assert len(frame) == 1
    assert list(frame["symbol"]) == ["BTCUSDT"]
    assert float(frame.iloc[0]["open"]) == 100.0
    assert float(frame.iloc[0]["close"]) == 100.5
    assert float(frame.iloc[0]["open_interest"]) == 123.0


def test_load_combined_dataframe_limit_applies(tmp_path: Path) -> None:
    candle_1 = SpotCandle(
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
    candle_2 = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 1, 59, 999000, tzinfo=UTC),
        open_price=101.0,
        high_price=102.0,
        low_price=100.0,
        close_price=101.5,
        volume=11.0,
        quote_volume=1100.0,
        trade_count=11,
    )
    save_spot_candles_parquet_lake({"deribit": {"BTCUSDT": [candle_1, candle_2]}}, "spot", str(tmp_path))
    frame = load_combined_dataframe_from_lake(lake_root=str(tmp_path), limit=1)
    assert len(frame) == 1
