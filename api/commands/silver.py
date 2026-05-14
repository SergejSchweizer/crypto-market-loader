"""Silver build command for spot/perp OHLCV transformation."""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Callable
from typing import cast

from application.services.silver_service import (
    SilverBuildReport,
    build_funding_1m_feature_for_symbol,
    build_funding_observed_for_symbol,
    build_oi_1m_feature_for_symbol,
    build_oi_observed_for_symbol,
    build_silver_for_symbol,
    build_trades_1m_feature_for_symbol,
    build_trades_observed_for_symbol,
    discover_symbols,
    write_monthly_sidecars,
)
from ingestion.funding import DERIBIT_FUNDING_NATIVE_INTERVAL


def add_silver_build_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``silver-build`` parser."""

    parser = subparsers.add_parser("silver-build", help="Build silver monthly parquet outputs from bronze data")
    parser.add_argument("--bronze-root", default="lake/bronze", help="Bronze lake root")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument(
        "--market",
        nargs="+",
        choices=["spot", "perp", "oi", "funding", "trades"],
        default=["spot", "perp", "oi", "funding", "trades"],
    )
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument("--timeframe", default="1m", help="Timeframe to process (default: 1m)")
    parser.add_argument("--manifest", action="store_true", help="Generate monthly silver manifest sidecars")
    parser.add_argument("--plot", action="store_true", help="Generate monthly silver plot PNG sidecars")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_silver_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run silver build for configured markets/symbols."""

    bronze_root = cast(str, args.bronze_root)
    silver_root = cast(str, args.silver_root)
    exchange = cast(str, args.exchange)
    timeframe = cast(str, args.timeframe)
    reports: list[dict[str, object]] = []

    def _append_report(report_market: str, symbol_value: str, report: SilverBuildReport) -> None:
        manifest_path: str | None = None
        manifest_paths: list[str] = []
        plot_path: str | None = None
        plot_paths: list[str] = []
        want_manifest = bool(getattr(args, "manifest", False))
        want_plot = bool(getattr(args, "plot", False))
        if want_manifest or want_plot:
            manifest_paths, plot_paths = write_monthly_sidecars(
                silver_root=silver_root,
                market=report_market,
                exchange=exchange,
                symbol=symbol_value,
                report=report,
                write_manifest=want_manifest,
                plot=want_plot,
            )
            manifest_path = manifest_paths[0] if manifest_paths else None
            plot_path = plot_paths[0] if plot_paths else None
        report_dict = report.to_dict()
        report_dict["manifest_path"] = manifest_path
        report_dict["manifest_paths"] = manifest_paths
        report_dict["plot_path"] = plot_path
        report_dict["plot_paths"] = plot_paths
        reports.append(report_dict)

    def _run_funding(symbol: str) -> None:
        observed = build_funding_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=DERIBIT_FUNDING_NATIVE_INTERVAL,
        )
        _append_report("funding_observed", symbol, observed)

        feature = build_funding_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe=DERIBIT_FUNDING_NATIVE_INTERVAL,
        )
        _append_report("funding_1m_feature", symbol, feature)
        logger.info(
            "Silver funding reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )

    def _run_oi(symbol: str) -> None:
        observed = build_oi_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        _append_report("oi_observed", symbol, observed)

        feature = build_oi_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe=timeframe,
        )
        _append_report("oi_1m_feature", symbol, feature)
        logger.info(
            "Silver OI reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )

    def _run_trades(symbol: str) -> None:
        observed = build_trades_observed_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            instrument_type="perp",
            timeframe="tick",
        )
        _append_report("trades_observed", symbol, observed)
        feature = build_trades_1m_feature_for_symbol(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            observed_timeframe="tick",
        )
        _append_report("trades_1m_feature", symbol, feature)
        logger.info(
            "Silver trades reports written symbol=%s observed_rows=%s feature_rows=%s",
            symbol,
            observed.rows_out,
            feature.rows_out,
        )

    def _run_ohlcv(market: str, symbol: str) -> None:
        report = build_silver_for_symbol(
            bronze_root=bronze_root,
            silver_root=silver_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        _append_report(market, symbol, report)
        logger.info(
            "Silver dataset built market=%s symbol=%s rows_in=%s rows_out=%s",
            market,
            symbol,
            report.rows_in,
            report.rows_out,
        )

    market_handlers: dict[str, Callable[[str], None]] = {
        "funding": _run_funding,
        "oi": _run_oi,
        "trades": _run_trades,
    }

    for market in cast(list[str], args.market):
        symbols = cast(list[str] | None, args.symbols)
        bronze_dataset = "funding" if market == "funding" else market
        bronze_instrument = "perp" if market in {"funding", "oi", "trades"} else market
        discovery_timeframe = (
            DERIBIT_FUNDING_NATIVE_INTERVAL
            if market == "funding"
            else "tick"
            if market == "trades"
            else timeframe
        )
        effective_symbols = symbols or discover_symbols(
            bronze_root=bronze_root,
            market=bronze_dataset,
            exchange=exchange,
            timeframe=discovery_timeframe,
            instrument_type=bronze_instrument,
        )
        logger.info("Silver build schedule market=%s symbols=%s timeframe=%s", market, effective_symbols, timeframe)
        handler = market_handlers.get(market)
        for symbol in effective_symbols:
            if handler is not None:
                handler(symbol)
            else:
                _run_ohlcv(market, symbol)

    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: silver-build reports=%s", len(reports))
