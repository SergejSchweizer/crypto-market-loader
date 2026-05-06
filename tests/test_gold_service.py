"""Tests for gold transformation service."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from application.services.gold_service import build_gold_for_symbol, discover_gold_symbols

pl = pytest.importorskip("polars")


def _write_silver_month(
    root: Path,
    *,
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
    rows: list[dict[str, object]],
) -> None:
    target = (
        root
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"{symbol}_{month.replace('-', '_')}.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target)


def test_build_gold_for_symbol_writes_hashed_parquet_and_manifest(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    symbol = "BTC"
    exchange = "deribit"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 5, 1, 0, 1, tzinfo=UTC)

    _write_silver_month(
        silver,
        dataset_type="spot",
        exchange=exchange,
        symbol="BTC_USDC",
        timeframe="1m",
        month="2026-05",
        rows=[
            {"open_time": t0, "exchange": exchange, "symbol": symbol, "open_price": 1.0, "high_price": 2.0, "low_price": 0.5, "close_price": 1.5, "volume": 10.0},
            {"open_time": t1, "exchange": exchange, "symbol": symbol, "open_price": 1.5, "high_price": 2.5, "low_price": 1.0, "close_price": 2.0, "volume": 11.0},
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="perp",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {"open_time": t0, "exchange": exchange, "symbol": symbol, "open_price": 10.0, "high_price": 11.0, "low_price": 9.5, "close_price": 10.5, "volume": 110.0},
            {"open_time": t1, "exchange": exchange, "symbol": symbol, "open_price": 10.5, "high_price": 11.5, "low_price": 10.0, "close_price": 11.0, "volume": 111.0},
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="oi_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {"timestamp_m1": t0, "exchange": exchange, "symbol": symbol, "open_interest": 1000.0, "oi_is_observed": True, "oi_is_ffill": False, "minutes_since_oi_observation": 0, "oi_observation_lag_sec": 0},
            {"timestamp_m1": t1, "exchange": exchange, "symbol": symbol, "open_interest": 1001.0, "oi_is_observed": False, "oi_is_ffill": True, "minutes_since_oi_observation": 1, "oi_observation_lag_sec": 60},
        ],
    )
    _write_silver_month(
        silver,
        dataset_type="funding_1m_feature",
        exchange=exchange,
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        month="2026-05",
        rows=[
            {"timestamp": t0, "exchange": exchange, "symbol": symbol, "funding_rate_last_known": 0.001, "minutes_since_funding": 0, "is_funding_observation_minute": True, "funding_data_available": True},
            {"timestamp": t1, "exchange": exchange, "symbol": symbol, "funding_rate_last_known": 0.001, "minutes_since_funding": 1, "is_funding_observation_minute": False, "funding_data_available": True},
        ],
    )

    assert discover_gold_symbols(str(silver), exchange) == [symbol]

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol=symbol,
        manifest=True,
    )

    parquet_name = Path(report.parquet_path).name
    assert re.fullmatch(
        rf"{symbol}_gold-market-full-m1_v1\.0\.0_[a-f0-9]{{12}}_[a-f0-9]{{12}}_[A-Za-z0-9]+\.parquet",
        parquet_name,
    ) is not None
    assert Path(report.parquet_path).parent == gold
    assert Path(report.parquet_path).exists()
    assert Path(report.manifest_path).exists()
    assert report.plot_path is None or Path(report.plot_path).exists()
    assert Path(report.manifest_path).name == f"{Path(report.parquet_path).stem}.json"

    payload = json.loads(Path(report.manifest_path).read_text(encoding="utf-8"))
    assert payload["symbol"] == symbol
    assert payload["exchange"] == exchange
    assert payload["rows_out"] == 2
    assert "plot_generated" in payload
    assert "source_silver_datasets" in payload
    assert "spot_1m" in payload["source_silver_datasets"]
    assert "columns" in payload["source_silver_datasets"]["spot_1m"]
    assert "open_time" in payload["source_silver_datasets"]["spot_1m"]["columns"]
    assert "perp_1m" in payload["source_silver_datasets"]
    assert "oi_1m_feature" in payload["source_silver_datasets"]
    assert "funding_1m_feature" in payload["source_silver_datasets"]
    assert payload["source_silver_datasets"]["spot_1m"]["source_symbols"] == ["BTC"]
    assert payload["source_silver_datasets"]["perp_1m"]["source_symbols"] == ["BTC"]
    assert "feature_metadata" in payload
    assert "spot_close_price" in payload["feature_metadata"]
    assert payload["feature_metadata"]["spot_close_price"]["source_exchange"] == exchange
    assert "time_range" in payload["feature_metadata"]["spot_close_price"]
    assert payload["feature_metadata"]["spot_close_price"]["time_range"]["min_timestamp"] is not None
    assert payload["dataset_id"] == "gold.market.full.m1"
    assert payload["dataset_version"] == "v1.0.0"
    assert "feature_set_hash" in payload
    assert "source_data_hash" in payload
    assert "git_commit_hash" in payload
    assert "build_id" in payload


def test_build_gold_for_symbol_normalizes_input_symbol(tmp_path: Path) -> None:
    silver = tmp_path / "silver"
    gold = tmp_path / "gold"
    exchange = "deribit"
    canonical = "BTC"
    t0 = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)

    for dataset_type, rows in [
        ("spot", [{"open_time": t0, "exchange": exchange, "symbol": "BTC_USDC", "open_price": 1.0, "high_price": 1.1, "low_price": 0.9, "close_price": 1.0, "volume": 1.0}]),
        ("perp", [{"open_time": t0, "exchange": exchange, "symbol": "BTC-PERPETUAL", "open_price": 1.0, "high_price": 1.1, "low_price": 0.9, "close_price": 1.0, "volume": 1.0}]),
        ("oi_1m_feature", [{"timestamp_m1": t0, "exchange": exchange, "symbol": "BTC-PERPETUAL", "open_interest": 1.0, "oi_is_observed": True, "oi_is_ffill": False, "minutes_since_oi_observation": 0, "oi_observation_lag_sec": 0}]),
        ("funding_1m_feature", [{"timestamp": t0, "exchange": exchange, "symbol": "BTC-PERPETUAL", "funding_rate_last_known": 0.0, "minutes_since_funding": 0, "is_funding_observation_minute": True, "funding_data_available": True}]),
    ]:
        _write_silver_month(
            silver,
            dataset_type=dataset_type,
            exchange=exchange,
        symbol="BTC-PERPETUAL",
            timeframe="1m",
            month="2026-05",
            rows=rows,
        )

    report = build_gold_for_symbol(
        silver_root=str(silver),
        gold_root=str(gold),
        exchange=exchange,
        symbol="btc_perpetual",
    )
    assert Path(report.parquet_path).parent == gold
    assert report.manifest_path is None
