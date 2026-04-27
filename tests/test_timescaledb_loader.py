"""Tests for parquet-to-TimescaleDB loader helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from ingestion.timescaledb_loader import (
    _ensure_tables,
    _upsert_rows,
    list_parquet_files,
    load_timescale_config_from_env,
    parquet_file_signature,
)


def test_load_timescale_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TIMESCALEDB_HOST", "10.10.10.10")
    monkeypatch.setenv("TIMESCALEDB_PORT", "54321")
    monkeypatch.setenv("TIMESCALEDB_USER", "crypto")
    monkeypatch.setenv("TIMESCALEDB_PASSWORD", "secret")
    monkeypatch.setenv("TIMESCALEDB_DB", "crypto")
    monkeypatch.setenv("PGSSLMODE", "require")
    config = load_timescale_config_from_env()

    assert config.host == "10.10.10.10"
    assert config.port == 54321
    assert config.user == "crypto"
    assert config.password == "secret"
    assert config.dbname == "crypto"
    assert config.sslmode == "require"


def test_load_timescale_config_from_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "TIMESCALEDB_HOST=127.0.0.1",
                "TIMESCALEDB_PORT=5432",
                "TIMESCALEDB_USER=postgres",
                "TIMESCALEDB_PASSWORD=change_me",
                "TIMESCALEDB_DB=postgres",
                "PGSSLMODE=require",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("TIMESCALEDB_HOST", raising=False)
    monkeypatch.delenv("TIMESCALEDB_PORT", raising=False)
    monkeypatch.delenv("TIMESCALEDB_USER", raising=False)
    monkeypatch.delenv("TIMESCALEDB_PASSWORD", raising=False)
    monkeypatch.delenv("TIMESCALEDB_DB", raising=False)
    monkeypatch.delenv("PGSSLMODE", raising=False)

    config = load_timescale_config_from_env(env_file=str(env_file))

    assert config.host == "127.0.0.1"
    assert config.port == 5432
    assert config.user == "postgres"
    assert config.password == "change_me"
    assert config.dbname == "postgres"
    assert config.sslmode == "require"


def test_load_timescale_config_env_overrides_env_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "TIMESCALEDB_HOST=127.0.0.1",
                "TIMESCALEDB_PORT=5432",
                "TIMESCALEDB_USER=postgres",
                "TIMESCALEDB_PASSWORD=change_me",
                "TIMESCALEDB_DB=postgres",
                "PGSSLMODE=require",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("TIMESCALEDB_HOST", "10.10.10.10")
    monkeypatch.setenv("TIMESCALEDB_PORT", "54321")
    monkeypatch.setenv("TIMESCALEDB_USER", "crypto")
    monkeypatch.setenv("TIMESCALEDB_PASSWORD", "secret")
    monkeypatch.setenv("TIMESCALEDB_DB", "crypto")
    monkeypatch.setenv("PGSSLMODE", "verify-full")

    config = load_timescale_config_from_env(env_file=str(env_file))

    assert config.host == "10.10.10.10"
    assert config.port == 54321
    assert config.user == "crypto"
    assert config.password == "secret"
    assert config.dbname == "crypto"
    assert config.sslmode == "verify-full"


def test_load_timescale_config_default_sslmode_disable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "TIMESCALEDB_HOST=127.0.0.1",
                "TIMESCALEDB_PORT=5432",
                "TIMESCALEDB_USER=postgres",
                "TIMESCALEDB_PASSWORD=change_me",
                "TIMESCALEDB_DB=postgres",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("TIMESCALEDB_SSLMODE", raising=False)
    monkeypatch.delenv("PGSSLMODE", raising=False)

    config = load_timescale_config_from_env(env_file=str(env_file))
    assert config.sslmode == "disable"


def test_list_parquet_files_filters_dataset_types(tmp_path: Path) -> None:
    spot_file = (
        tmp_path
        / "dataset_type=ohlcv"
        / "exchange=binance"
        / "instrument_type=spot"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "date=2026-04"
        / "data.parquet"
    )
    perp_file = (
        tmp_path
        / "dataset_type=ohlcv"
        / "exchange=deribit"
        / "instrument_type=perp"
        / "symbol=BTC-PERPETUAL"
        / "timeframe=1m"
        / "date=2026-04"
        / "data.parquet"
    )
    spot_file.parent.mkdir(parents=True, exist_ok=True)
    perp_file.parent.mkdir(parents=True, exist_ok=True)
    spot_file.write_bytes(b"spot")
    perp_file.write_bytes(b"perp")

    ohlcv_only = list_parquet_files(lake_root=str(tmp_path), dataset_types=["ohlcv"])
    all_files = list_parquet_files(lake_root=str(tmp_path), dataset_types=None)

    assert ohlcv_only == sorted([spot_file, perp_file])
    assert all_files == sorted([spot_file, perp_file])


def test_parquet_file_signature_changes_with_content(tmp_path: Path) -> None:
    file_path = tmp_path / "data.parquet"
    file_path.write_bytes(b"one")
    first = parquet_file_signature(file_path)

    file_path.write_bytes(b"two-two")
    second = parquet_file_signature(file_path)

    assert first != second


def test_ensure_tables_tolerates_hypertable_failure() -> None:
    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        def execute(self, query: str) -> None:
            if "SELECT create_hypertable(" in query:
                raise RuntimeError("timescaledb library missing")

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

    _ensure_tables(FakeConnection())


def test_upsert_rows_serializes_datetime_in_extra() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.params: list[tuple[Any, ...]] = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

        def executemany(self, query: str, params: list[tuple[Any, ...]]) -> None:
            del query
            self.params.extend(params)

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def cursor(self) -> FakeCursor:
            return self.cursor_obj

    connection = FakeConnection()
    now = datetime(2026, 4, 27, 12, 0, tzinfo=timezone.utc)
    row = {
        "schema_version": "v1",
        "dataset_type": "ohlcv",
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "instrument_type": "spot",
        "event_time": now,
        "ingested_at": now,
        "run_id": "run-1",
        "source_endpoint": "public_market_data",
        "open_time": now,
        "close_time": now,
        "timeframe": "1m",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
        "quote_volume": 15.0,
        "trade_count": 2,
        "extra": {"open_time": now, "nested": {"close_time": now}},
    }

    upserted = _upsert_rows(connection=connection, rows=[row], batch_size=1000)

    assert upserted == 1
    serialized_extra = connection.cursor_obj.params[0][-1]
    parsed_extra = json.loads(serialized_extra)
    assert parsed_extra["open_time"] == now.isoformat()
    assert parsed_extra["nested"]["close_time"] == now.isoformat()
