"""Tests for plot helper utilities."""

from __future__ import annotations

import builtins
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.plotting import (
    build_plot_filename,
    price_value,
    save_candle_plots,
    save_funding_plot,
    save_open_interest_plot,
)
from ingestion.spot import SpotCandle


def _sample_candle() -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 1, 1, tzinfo=UTC),
        close_time=datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=110.0,
        low_price=95.0,
        close_price=105.0,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=20,
    )


def test_price_value_selector() -> None:
    candle = _sample_candle()
    assert price_value(candle, "spot") == 105.0
    assert price_value(candle, "close") == 105.0
    assert price_value(candle, "open") == 100.0
    assert price_value(candle, "high") == 110.0
    assert price_value(candle, "low") == 95.0


def test_build_plot_filename_sanitizes_inputs() -> None:
    file_name = build_plot_filename(
        exchange="deribit",
        symbol="BTC/PERPETUAL",
        interval="1m",
        price_field="close",
    )
    assert file_name.endswith(".png")
    assert "20260101" not in file_name
    assert "BTC_PERPETUAL" in file_name


def test_save_candle_plots_skips_empty_and_writes_png(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")
    candle = _sample_candle()
    out = save_candle_plots(
        {
            "deribit": {
                "BTCUSDT": [candle],
                "EMPTY": [],
            }
        },
        output_dir=str(tmp_path),
        price_field="close",
    )
    assert len(out) == 1
    assert Path(out[0]).exists()
    assert out[0].endswith(".png")


def test_save_open_interest_plot_empty_input_returns_path(tmp_path: Path) -> None:
    out_path = str(tmp_path / "oi.png")
    assert save_open_interest_plot("deribit", "BTC", "1m", [], [], out_path) == out_path


def test_save_funding_plot_empty_input_returns_path(tmp_path: Path) -> None:
    out_path = str(tmp_path / "funding.png")
    assert save_funding_plot("deribit", "BTC", "8h", [], [], out_path) == out_path


def test_save_open_interest_and_funding_plots_write_png(tmp_path: Path) -> None:
    pytest.importorskip("matplotlib")
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC), datetime(2026, 1, 1, 0, 1, tzinfo=UTC)]
    oi_path = save_open_interest_plot(
        "deribit",
        "BTC",
        "1m",
        times,
        [100.0, 101.0],
        str(tmp_path / "oi" / "plot.png"),
    )
    funding_path = save_funding_plot(
        "deribit",
        "BTC",
        "8h",
        times,
        [0.001, -0.002],
        str(tmp_path / "funding" / "plot.png"),
    )
    assert Path(oi_path).exists()
    assert Path(funding_path).exists()


def test_plot_functions_raise_runtime_error_when_matplotlib_missing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    real_import = builtins.__import__

    def _fake_import(name: str, *args: object, **kwargs: object):  # type: ignore[no-untyped-def]
        if name.startswith("matplotlib"):
            raise ImportError("missing matplotlib")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_candle_plots({"deribit": {"BTCUSDT": [_sample_candle()]}}, str(tmp_path), "close")
    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_open_interest_plot(
            "deribit",
            "BTC",
            "1m",
            [datetime(2026, 1, 1, 0, 0, tzinfo=UTC)],
            [1.0],
            str(tmp_path / "oi.png"),
        )
    with pytest.raises(RuntimeError, match="matplotlib is required"):
        save_funding_plot(
            "deribit",
            "BTC",
            "8h",
            [datetime(2026, 1, 1, 0, 0, tzinfo=UTC)],
            [0.1],
            str(tmp_path / "funding.png"),
        )
