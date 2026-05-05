"""Tests for silver command orchestration behavior."""

from __future__ import annotations

import argparse
import logging

from api.commands import silver as silver_cmd


def test_run_silver_build_uses_native_funding_timeframe_for_symbol_discovery(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    captured: list[tuple[str, str]] = []

    def fake_discover_symbols(
        bronze_root: str,
        market: str,
        exchange: str,
        timeframe: str = "1m",
        instrument_type: str | None = None,
    ) -> list[str]:
        del bronze_root, exchange, instrument_type
        captured.append((market, timeframe))
        if market == "funding":
            return ["BTC-PERPETUAL"]
        return []

    monkeypatch.setattr(silver_cmd, "discover_symbols", fake_discover_symbols)
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_observed_for_symbol",
        lambda **kwargs: _report("funding_observed"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_1m_feature_for_symbol",
        lambda **kwargs: _report("funding_1m_feature"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_oi_observed_for_symbol",
        lambda **kwargs: _report("oi_observed"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_oi_1m_feature_for_symbol",
        lambda **kwargs: _report("oi_1m_feature"),
    )
    monkeypatch.setattr(silver_cmd, "write_symbol_report", lambda **kwargs: "/tmp/report.json")

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["funding"],
        symbols=None,
        timeframe="1m",
        no_json_output=True,
    )
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert captured == [("funding", "8h")]


def _report(dataset: str) -> silver_cmd.SilverBuildReport:
    return silver_cmd.SilverBuildReport(
        dataset=dataset,
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timeframe="1m",
        period_start=None,
        period_end=None,
        months_processed=[],
        rows_in=0,
        rows_out=0,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=None,
        max_timestamp=None,
        symbols=["BTC-PERPETUAL"],
        columns=["timestamp", "value"],
    )


def test_run_silver_build_does_not_plot_funding_observed(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(
        silver_cmd,
        "discover_symbols",
        lambda **kwargs: ["BTC-PERPETUAL"],
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_observed_for_symbol",
        lambda **kwargs: _report("funding_observed"),
    )
    monkeypatch.setattr(
        silver_cmd,
        "build_funding_1m_feature_for_symbol",
        lambda **kwargs: _report("funding_1m_feature"),
    )
    monkeypatch.setattr(silver_cmd, "write_symbol_report", lambda **kwargs: "/tmp/report.json")

    calls: list[str] = []

    def fake_write_symbol_plot(**kwargs: object) -> str:
        report = kwargs.get("report")
        if isinstance(report, silver_cmd.SilverBuildReport):
            calls.append(report.dataset)
        return "/tmp/plot.png"

    monkeypatch.setattr(silver_cmd, "write_symbol_plot", fake_write_symbol_plot)

    args = argparse.Namespace(
        bronze_root="lake/bronze",
        silver_root="lake/silver",
        exchange="deribit",
        market=["funding"],
        symbols=None,
        timeframe="1m",
        plot=True,
        no_json_output=True,
    )
    silver_cmd.run_silver_build(args=args, logger=logging.getLogger("test"))

    assert calls == ["funding_1m_feature"]
