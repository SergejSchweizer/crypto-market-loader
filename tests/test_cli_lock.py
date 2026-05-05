"""Tests for single-instance CLI locking."""

from __future__ import annotations

import fcntl
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from api import cli
from api.cli import SingleInstanceError, SingleInstanceLock
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep CLI tests from writing to the configured runtime log directory."""

    monkeypatch.setenv("DEPTH_SYNC_LOG_DIR", str(tmp_path / "logs"))


def test_single_instance_lock_creates_lock_file(tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    with SingleInstanceLock(str(lock_file)):
        assert lock_file.exists()
        content = lock_file.read_text().strip()
        assert content.isdigit()


def test_single_instance_lock_raises_on_contention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lock_file = tmp_path / "test.lock"

    def fake_flock(fd: int, operation: int) -> None:
        del fd, operation
        raise BlockingIOError("locked")

    monkeypatch.setattr(fcntl, "flock", fake_flock)

    with pytest.raises(SingleInstanceError):
        with SingleInstanceLock(str(lock_file)):
            pass


def test_auto_mode_fetches_all_history_when_no_lake_data(monkeypatch: pytest.MonkeyPatch) -> None:
    sample = SpotCandle(
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

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "latest_open_time_in_lake", lambda **kwargs: None)
    monkeypatch.setattr(cli, "_TAIL_DELTA_ONLY", True)
    calls: list[dict[str, object]] = []

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        calls.append(kwargs)
        return [sample]

    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)

    candles = cli._fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    assert len(candles) == 1
    assert len(calls) == 1
    assert calls[0]["exchange"] == "deribit"
    assert calls[0]["symbol"] == "BTCUSDT"


def test_gap_fill_fetches_internal_and_tail_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    interval_ms = 60_000
    open_times = [
        datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
        datetime(2026, 4, 27, 10, 3, tzinfo=UTC),
    ]
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: open_times)
    monkeypatch.setattr(cli, "normalize_storage_symbol", lambda **kwargs: "BTCUSDT")
    monkeypatch.setattr(cli, "interval_to_milliseconds", lambda **kwargs: interval_ms)
    monkeypatch.setattr(cli, "_last_closed_open_ms", lambda **kwargs: end_open_ms)
    monkeypatch.setattr(cli, "_TAIL_DELTA_ONLY", False)

    def fake_fetch_candles_range(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = cast(int, kwargs["start_open_ms"])
        end_open_ms = cast(int, kwargs["end_open_ms"])
        calls.append((start_open_ms, end_open_ms))
        return []

    monkeypatch.setattr(cli, "fetch_candles_range", fake_fetch_candles_range)

    candles = cli._fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
    )

    gap_one_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert candles == []
    assert set(calls) == {(gap_one_ms, gap_one_ms), (gap_tail_start_ms, end_open_ms)}


def test_main_loader_command_still_uses_single_instance_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Locked:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            raise SingleInstanceError("bronze-ingest already running")

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", Locked)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--no-json-output",
        ],
    )

    with pytest.raises(SystemExit, match="bronze-ingest already running"):
        cli.main()



def test_main_loader_randomizes_symbol_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    expected_order = ["SOLUSDT", "ETHUSDT", "BTCUSDT"]
    seen_symbols: list[str] = []

    def fake_random_order(values: list[str]) -> list[str]:
        if values == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            return expected_order
        return values

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        seen_symbols.append(cast(str, kwargs["symbol"]))
        return []

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli.loader_cmd, "_items_in_random_order", fake_random_order)
    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "--no-json-output",
        ],
    )

    cli.main()

    assert seen_symbols == expected_order


def test_main_bronze_ingest_command_uses_loader_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    called: dict[str, bool] = {"fetch": False}

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        del kwargs
        called["fetch"] = True
        return []

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--no-json-output",
        ],
    )

    cli.main()
    assert called["fetch"] is True


def test_main_loader_randomizes_market_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    seen_markets: list[str] = []

    def fake_random_order(values: list[str]) -> list[str]:
        if values == ["spot", "perp"]:
            return ["perp", "spot"]
        return values

    def fake_fetch_candles_all_history(**kwargs: object) -> list[SpotCandle]:
        seen_markets.append(cast(str, kwargs["market"]))
        return []

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli.loader_cmd, "_items_in_random_order", fake_random_order)
    monkeypatch.setattr(cli, "open_times_in_lake", lambda **kwargs: [])
    monkeypatch.setattr(cli, "fetch_candles_all_history", fake_fetch_candles_all_history)
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "perp",
            "--symbols",
            "BTCUSDT",
            "--no-json-output",
        ],
    )

    cli.main()

    assert seen_markets == ["perp", "spot"]


def test_main_loader_uses_randomized_dataset_group_order(monkeypatch: pytest.MonkeyPatch) -> None:
    class NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    scheduled_groups: list[str] = []

    def fake_fetch_candle_tasks_parallel(
        **kwargs: object,
    ) -> tuple[dict[tuple[object, ...], list[object]], dict[tuple[object, ...], str]]:
        tasks = cast(list[tuple[str, str, str, str]], kwargs["tasks"])
        for task in tasks:
            scheduled_groups.append(task[1])
        return {}, {}

    def fake_fetch_open_interest_tasks_parallel(
        **kwargs: object,
    ) -> tuple[dict[tuple[object, ...], list[object]], dict[tuple[object, ...], str]]:
        scheduled_groups.append("oi")
        return {}, {}

    def fake_fetch_funding_tasks_parallel(
        **kwargs: object,
    ) -> tuple[dict[tuple[object, ...], list[object]], dict[tuple[object, ...], str]]:
        scheduled_groups.append("funding")
        return {}, {}

    def fake_random_order(values: list[str]) -> list[str]:
        if values == ["spot", "perp", "oi", "funding"]:
            return ["funding", "oi", "perp", "spot"]
        return values

    monkeypatch.setattr(cli, "SingleInstanceLock", NoopLock)
    monkeypatch.setattr(cli.loader_cmd, "_items_in_random_order", fake_random_order)
    monkeypatch.setattr(cli.loader_cmd, "_fetch_candle_tasks_parallel", fake_fetch_candle_tasks_parallel)
    monkeypatch.setattr(cli.loader_cmd, "_fetch_open_interest_tasks_parallel", fake_fetch_open_interest_tasks_parallel)
    monkeypatch.setattr(cli.loader_cmd, "_fetch_funding_tasks_parallel", fake_fetch_funding_tasks_parallel)
    monkeypatch.setattr(cli, "_write_loader_samples", lambda **kwargs: None)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "perp",
            "oi",
            "funding",
            "--symbols",
            "BTCUSDT",
            "--no-json-output",
        ],
    )

    cli.main()

    assert {"funding", "oi", "perp", "spot"}.issubset(set(scheduled_groups))


def test_write_loader_samples_does_not_write_csv_or_parquet_without_plots(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
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
    oi_point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )

    cli._write_loader_samples(
        candles_for_storage={"spot": {"deribit": {"BTCUSDT": [candle]}}},
        open_interest_for_storage={"perp": {"deribit": {"BTCUSDT": [oi_point]}}},
        logger=logging.getLogger("test_loader_samples"),
    )

    sample_files = {path.name for path in (tmp_path / "samples").glob("*")}
    assert sample_files == set()
    assert not any(path.suffix == ".png" for path in (tmp_path / "samples").glob("*"))
    assert not any(path.suffix == ".parquet" for path in (tmp_path / "samples").glob("*"))


def test_export_descriptive_stats_writes_csv(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    rows = [
        {"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0},
        {"open": 2.0, "high": 3.0, "low": 1.5, "close": 2.5, "volume": 20.0},
    ]
    monkeypatch.setattr(cli, "load_combined_dataframe_from_lake", lambda **kwargs: pd.DataFrame(rows))
    output_csv = tmp_path / "docs" / "tables" / "descriptive_stats_baseline.csv"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "export-descriptive-stats",
            "--lake-root",
            str(tmp_path / "lake"),
            "--output-csv",
            str(output_csv),
            "--start-time",
            "2026-01-01T00:00:00+00:00",
            "--end-time",
            "2026-01-31T23:59:59+00:00",
            "--no-json-output",
        ],
    )

    cli.main()
    assert output_csv.exists()
    written = pd.read_csv(output_csv)
    assert list(written.columns) == ["Variable", "Mean", "Std", "Min", "Max"]
    assert set(written["Variable"]) == {"open", "high", "low", "close", "volume"}


def test_loader_rejects_removed_timeframe_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    class _NoopLock:
        def __init__(self, lock_path: str) -> None:
            del lock_path

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            del exc_type, exc, tb

    monkeypatch.setattr(cli, "SingleInstanceLock", _NoopLock)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "bronze-ingest",
            "--exchange",
            "deribit",
            "--market",
            "spot",
            "--symbols",
            "BTCUSDT",
            "--timeframe",
            "5m",
            "--no-json-output",
        ],
    )
    with pytest.raises(SystemExit):
        cli.main()
