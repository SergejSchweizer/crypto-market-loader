"""Tests for bronze loader command output sidecar metadata."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api.commands import loader as loader_cmd
from application.dto import CandleFetchTaskDTO, PersistResultDTO
from ingestion.spot import SpotCandle


def test_run_bronze_build_emits_manifest_and_plot_file_lists(tmp_path: Path, monkeypatch, capsys) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    candle = SpotCandle(
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
    parquet_path = (
        tmp_path
        / "dataset_type=spot"
        / "exchange=deribit"
        / "instrument_type=spot"
        / "symbol=BTCUSDT"
        / "timeframe=1m"
        / "month=2026-05"
        / "date=2026-05-01"
        / "data.parquet"
    )

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        callback = kwargs.get("on_candle_task_chunk")
        if callable(callback):
            callback(
                CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m"),
                [candle],
            )
        return (
            {("deribit", "spot", "BTCUSDT", "1m"): [candle]},
            {},
            {},
            {},
            {},
            {},
        )

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(
        loader_cmd,
        "persist_loader_outputs_dto",
        lambda **kwargs: PersistResultDTO([str(parquet_path)]),
    )
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot"],
        symbols=["BTCUSDT"],
        save_parquet_lake=True,
        lake_root=str(tmp_path),
        no_json_output=False,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_bronze_build")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    payload = json.loads(capsys.readouterr().out)
    assert payload["_parquet_files"] == [str(parquet_path)]
    assert payload["_manifest_files"] == [str(parquet_path.with_suffix(".json").resolve())]
    assert payload["_plot_files"] == [str(parquet_path.with_suffix(".png").resolve())]


def test_exchange_symbol_start_dates_override_symbol_and_global_bounds() -> None:
    original_global = loader_cmd._BRONZE_START_OPEN_MS
    original_symbol = dict(loader_cmd._BRONZE_SYMBOL_START_OPEN_MS)
    original_exchange_symbol = dict(loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS)
    try:
        loader_cmd._BRONZE_START_OPEN_MS = 1000
        loader_cmd._BRONZE_SYMBOL_START_OPEN_MS = {"BTC": 2000}
        loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS = {"deribit:BTC": 3000}
        assert loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="BTCUSDT") == 3000
        assert loader_cmd._symbol_start_open_ms_bound(exchange="deribit", symbol="ETHUSDT") == 1000
    finally:
        loader_cmd._BRONZE_START_OPEN_MS = original_global
        loader_cmd._BRONZE_SYMBOL_START_OPEN_MS = original_symbol
        loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS = original_exchange_symbol


def test_parse_exchange_symbol_start_dates_parses_canonical_pairs() -> None:
    parsed = loader_cmd._parse_exchange_symbol_start_dates(["deribit:BTCUSDT=2023-04-24", "DERIBIT:SOL=2024-02-27"])
    assert parsed["deribit:BTC"] == int(datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert parsed["deribit:SOL"] == int(datetime(2024, 2, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)


def test_configure_bronze_start_bounds_sets_globals() -> None:
    args = argparse.Namespace(
        start_date=None,
        symbol_start_dates=["BTC=2023-04-24"],
        exchange_symbol_start_dates=["deribit:SOL=2024-02-27"],
    )
    logger = logging.getLogger("test_bronze_bounds")

    original_global = loader_cmd._BRONZE_START_OPEN_MS
    original_symbol = dict(loader_cmd._BRONZE_SYMBOL_START_OPEN_MS)
    original_exchange_symbol = dict(loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS)
    try:
        loader_cmd._configure_bronze_start_bounds(args=args, logger=logger)
        assert loader_cmd._BRONZE_START_OPEN_MS is None
        assert loader_cmd._BRONZE_SYMBOL_START_OPEN_MS["BTC"] == int(
            datetime(2023, 4, 24, 0, 0, tzinfo=UTC).timestamp() * 1000
        )
        assert loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS["deribit:SOL"] == int(
            datetime(2024, 2, 27, 0, 0, tzinfo=UTC).timestamp() * 1000
        )
    finally:
        loader_cmd._BRONZE_START_OPEN_MS = original_global
        loader_cmd._BRONZE_SYMBOL_START_OPEN_MS = original_symbol
        loader_cmd._BRONZE_EXCHANGE_SYMBOL_START_OPEN_MS = original_exchange_symbol


def test_run_bronze_build_drops_invalid_symbols_before_scheduling(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    scheduled_candle_tasks: list[tuple[str, str, str, str]] = []
    scheduled_oi_tasks: list[tuple[str, str, str]] = []
    scheduled_funding_tasks: list[tuple[str, str, str]] = []

    def _fake_fetch_all_task_groups(**kwargs: object):  # type: ignore[no-untyped-def]
        scheduled_candle_tasks.extend(kwargs["candle_tasks"])
        scheduled_oi_tasks.extend(kwargs["oi_tasks"])
        scheduled_funding_tasks.extend(kwargs["funding_tasks"])
        return ({}, {}, {}, {}, {}, {})

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(loader_cmd, "_fetch_all_task_groups", _fake_fetch_all_task_groups)
    monkeypatch.setattr(loader_cmd, "ensure_bronze_sidecars", lambda **kwargs: [])

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot", "perp", "oi", "funding"],
        symbols=["BTC", None, " ", "\t", "ETH"],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_symbol_sanitization")
    loader_cmd.run_bronze_build(args=args, logger=logger)

    assert all(task[2] in {"BTC", "ETH"} for task in scheduled_candle_tasks)
    assert all(task[1] in {"BTC", "ETH"} for task in scheduled_oi_tasks)
    assert all(task[1] in {"BTC", "ETH"} for task in scheduled_funding_tasks)


def test_run_bronze_build_raises_when_no_valid_symbols(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(loader_cmd, "SingleInstanceLock", _NoopLock)

    args = argparse.Namespace(
        exchange="deribit",
        exchanges=None,
        market=["spot"],
        symbols=[None, "", "  "],
        save_parquet_lake=False,
        lake_root="lake/bronze",
        no_json_output=True,
        tail_delta_only=True,
    )
    logger = logging.getLogger("test_symbol_sanitization_empty")
    with pytest.raises(ValueError, match="No valid symbols configured"):
        loader_cmd.run_bronze_build(args=args, logger=logger)
