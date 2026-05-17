"""Tests for descriptive stats command edge cases."""

from __future__ import annotations

import argparse
import logging
import pathlib

import polars as pl
import pytest

from api.commands import stats as stats_cmd


def test_run_export_descriptive_stats_requires_timezone() -> None:
    args = argparse.Namespace(
        lake_root="lake/bronze",
        output_csv="docs/tables/out.csv",
        start_time="2026-01-01T00:00:00",
        end_time="2026-01-31T23:59:59+00:00",
        exchanges=None,
        symbols=None,
        timeframes=None,
        instrument_types=["spot", "perp"],
        no_json_output=True,
    )
    with pytest.raises(ValueError, match="must include timezone offset"):
        stats_cmd.run_export_descriptive_stats(args=args, logger=logging.getLogger("test"))


def test_run_export_descriptive_stats_requires_start_before_end() -> None:
    args = argparse.Namespace(
        lake_root="lake/bronze",
        output_csv="docs/tables/out.csv",
        start_time="2026-02-01T00:00:00+00:00",
        end_time="2026-01-31T23:59:59+00:00",
        exchanges=None,
        symbols=None,
        timeframes=None,
        instrument_types=["spot", "perp"],
        no_json_output=True,
    )
    with pytest.raises(ValueError, match="start-time must be <= end-time"):
        stats_cmd.run_export_descriptive_stats(args=args, logger=logging.getLogger("test"))


def test_run_export_descriptive_stats_emits_none_for_missing_columns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
) -> None:
    monkeypatch.setattr(
        stats_cmd,
        "load_combined_dataframe_from_lake",
        lambda **kwargs: pl.DataFrame([{"open": 1.0, "close": 1.1}]),
    )
    output_csv = tmp_path / "stats.csv"
    args = argparse.Namespace(
        lake_root="lake/bronze",
        output_csv=str(output_csv),
        start_time="2026-01-01T00:00:00+00:00",
        end_time="2026-01-31T23:59:59+00:00",
        exchanges=None,
        symbols=None,
        timeframes=None,
        instrument_types=["spot", "perp"],
        no_json_output=True,
    )
    stats_cmd.run_export_descriptive_stats(args=args, logger=logging.getLogger("test"))
    written = pl.read_csv(output_csv)
    high_row = written.filter(pl.col("Variable") == "high")
    assert high_row.height == 1
    assert high_row.select(pl.col("Mean")).item() is None


def test_run_export_descriptive_stats_emits_json_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        stats_cmd,
        "load_combined_dataframe_from_lake",
        lambda **kwargs: pl.DataFrame([{"open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0}]),
    )
    args = argparse.Namespace(
        lake_root="lake/bronze",
        output_csv=str(tmp_path / "stats.csv"),
        start_time="2026-01-01T00:00:00+00:00",
        end_time="2026-01-31T23:59:59+00:00",
        exchanges=None,
        symbols=None,
        timeframes=None,
        instrument_types=["spot", "perp"],
        no_json_output=False,
    )
    stats_cmd.run_export_descriptive_stats(args=args, logger=logging.getLogger("test"))
    assert '"row_count": 1' in capsys.readouterr().out


def test_to_float_or_none_handles_none_and_number() -> None:
    assert stats_cmd._to_float_or_none(None) is None
    assert stats_cmd._to_float_or_none(1) == 1.0
