"""Parquet schema contract tests for bronze datasets."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.funding import FundingPoint
from ingestion.lake import (
    save_funding_parquet_lake,
    save_open_interest_parquet_lake,
    save_spot_candles_parquet_lake,
    save_trades_parquet_lake,
)
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle
from ingestion.trades import TradeTick


def test_bronze_spot_schema_contract_order(tmp_path: Path) -> None:
    candle = SpotCandle(
        exchange="deribit",
        symbol="BTC_USDC",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=12,
    )
    files = save_spot_candles_parquet_lake({"deribit": {"BTC": [candle]}}, market="spot", lake_root=str(tmp_path))
    schema = pq.ParquetFile(files[0]).schema_arrow
    assert schema.names[:6] == ["schema_version", "dataset_type", "exchange", "symbol", "instrument_type", "event_time"]
    assert schema.names[-1] == "origin_payload"


def test_bronze_oi_schema_contract_fields(tmp_path: Path) -> None:
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=0.0,
    )
    files = save_open_interest_parquet_lake({"deribit": {"BTC": [point]}}, market="perp", lake_root=str(tmp_path))
    schema = pq.ParquetFile(files[0]).schema_arrow
    assert "open_interest" in schema.names
    assert "open_interest_value" in schema.names
    assert "timeframe" in schema.names


def test_bronze_funding_schema_contract_fields(tmp_path: Path) -> None:
    point = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 5, 1, 7, 59, 59, 999000, tzinfo=UTC),
        funding_rate=0.001,
        index_price=100.0,
        mark_price=101.0,
    )
    files = save_funding_parquet_lake({"deribit": {"BTC": [point]}}, market="perp", lake_root=str(tmp_path))
    schema = pq.ParquetFile(files[0]).schema_arrow
    assert "funding_rate" in schema.names
    assert "index_price" in schema.names
    assert "mark_price" in schema.names


def test_bronze_trades_schema_contract_fields(tmp_path: Path) -> None:
    tick = TradeTick(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        instrument_type="perp",
        trade_id="x1",
        trade_time=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=True,
        source_endpoint="public_trades",
    )
    files = save_trades_parquet_lake({"deribit": {"BTC": [tick]}}, market="perp", lake_root=str(tmp_path))
    schema = pq.ParquetFile(files[0]).schema_arrow
    assert schema.names[:6] == ["schema_version", "dataset_type", "exchange", "symbol", "instrument_type", "event_time"]
    assert "trade_id" in schema.names
    assert "price" in schema.names
    assert "quantity" in schema.names
    assert "is_maker" in schema.names
