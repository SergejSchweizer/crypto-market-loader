"""Tests for artifact sample generation service."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from application.dto import ArtifactOptionsDTO, LoaderStorageDTO
from application.services.artifact_service import MAX_FULL_PLOT_POINTS, write_loader_samples_dto
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


def _build_candles(count: int) -> list[SpotCandle]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    output: list[SpotCandle] = []
    for idx in range(count):
        open_time = start + timedelta(minutes=idx)
        output.append(
            SpotCandle(
                exchange="deribit",
                symbol="BTCUSDT",
                interval="1m",
                open_time=open_time,
                close_time=open_time + timedelta(seconds=59, milliseconds=999),
                open_price=100.0 + idx,
                high_price=101.0 + idx,
                low_price=99.0 + idx,
                close_price=100.5 + idx,
                volume=10.0 + idx,
                quote_volume=1000.0 + idx,
                trade_count=10 + idx,
            )
        )
    return output


def _build_open_interest(count: int) -> list[OpenInterestPoint]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    output: list[OpenInterestPoint] = []
    for idx in range(count):
        open_time = start + timedelta(minutes=idx)
        output.append(
            OpenInterestPoint(
                exchange="deribit",
                symbol="BTCUSDT",
                interval="1m",
                open_time=open_time,
                close_time=open_time + timedelta(seconds=59, milliseconds=999),
                open_interest=1000.0 + idx,
                open_interest_value=2000.0 + idx,
            )
        )
    return output


def _build_funding(count: int) -> list[FundingPoint]:
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    output: list[FundingPoint] = []
    for idx in range(count):
        open_time = start + timedelta(minutes=idx)
        output.append(
            FundingPoint(
                exchange="deribit",
                symbol="BTCUSDT",
                interval="1m",
                open_time=open_time,
                close_time=open_time + timedelta(seconds=59, milliseconds=999),
                funding_rate=0.0001 + (idx * 1e-8),
                index_price=10000.0 + idx,
                mark_price=10010.0 + idx,
            )
        )
    return output


def test_full_history_plots_are_capped_to_1000_points(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Full plot series should span full period and cap to ``MAX_FULL_PLOT_POINTS`` values."""

    monkeypatch.chdir(tmp_path)
    candles = _build_candles(1200)
    oi = _build_open_interest(1200)
    funding = _build_funding(1200)
    captured: dict[str, object] = {}

    def fake_save_candle_plots(**kwargs: object) -> list[str]:
        candles_by_exchange = cast(dict[str, dict[str, list[SpotCandle]]], kwargs["candles_by_exchange"])
        series = candles_by_exchange["deribit"]["BTCUSDT"]
        captured["candles"] = len(series)
        captured["candles_first"] = series[0].open_time
        captured["candles_last"] = series[-1].open_time
        output_path = tmp_path / "samples" / "deribit_BTCUSDT_1m_spot.png"
        output_path.write_text("plot", encoding="utf-8")
        return [str(output_path)]

    def fake_save_open_interest_plot(**kwargs: object) -> str:
        times = cast(list[datetime], kwargs["times"])
        captured["oi"] = len(times)
        captured["oi_first"] = times[0]
        captured["oi_last"] = times[-1]
        return str(tmp_path / "samples" / "oi.png")

    def fake_save_funding_plot(**kwargs: object) -> str:
        times = cast(list[datetime], kwargs["times"])
        captured["funding"] = len(times)
        captured["funding_first"] = times[0]
        captured["funding_last"] = times[-1]
        return str(tmp_path / "samples" / "funding.png")

    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles={"spot": {"deribit": {"BTCUSDT": candles}}},
            open_interest={"perp": {"deribit": {"BTCUSDT": oi}}},
            funding={"perp": {"deribit": {"BTCUSDT": funding}}},
        ),
        logger=logging.getLogger("test_artifact_service"),
        options=ArtifactOptionsDTO(generate_plots=True),
        save_candle_plots_fn=fake_save_candle_plots,
        save_open_interest_plot_fn=fake_save_open_interest_plot,
        save_funding_plot_fn=fake_save_funding_plot,
    )

    assert captured["candles"] == MAX_FULL_PLOT_POINTS
    assert captured["oi"] == MAX_FULL_PLOT_POINTS
    assert captured["funding"] == MAX_FULL_PLOT_POINTS
    assert captured["candles_first"] == candles[0].open_time
    assert captured["candles_last"] == candles[-1].open_time
    assert captured["oi_first"] == oi[0].open_time
    assert captured["oi_last"] == oi[-1].open_time
    assert captured["funding_first"] == funding[0].open_time
    assert captured["funding_last"] == funding[-1].open_time
