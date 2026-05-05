"""Silver build command for spot/perp OHLCV transformation."""

from __future__ import annotations

import argparse
import json
import logging
from typing import cast

from application.services.silver_service import (
    build_silver_for_symbol,
    discover_symbols,
    write_symbol_report,
)


def add_silver_build_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``silver-build`` parser."""

    parser = subparsers.add_parser("silver-build", help="Build silver monthly parquet outputs from bronze data")
    parser.add_argument("--bronze-root", default="lake/bronze", help="Bronze lake root")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument("--market", nargs="+", choices=["spot", "perp"], default=["spot", "perp"])
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument("--timeframe", default="1m", help="Timeframe to process (default: 1m)")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_silver_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run silver build for configured markets/symbols."""

    bronze_root = cast(str, args.bronze_root)
    silver_root = cast(str, args.silver_root)
    exchange = cast(str, args.exchange)
    timeframe = cast(str, args.timeframe)
    reports: list[dict[str, object]] = []

    for market in cast(list[str], args.market):
        symbols = cast(list[str] | None, args.symbols)
        effective_symbols = symbols or discover_symbols(
            bronze_root=bronze_root,
            market=market,
            exchange=exchange,
            timeframe=timeframe,
        )
        logger.info("Silver build schedule market=%s symbols=%s timeframe=%s", market, effective_symbols, timeframe)
        for symbol in effective_symbols:
            report = build_silver_for_symbol(
                bronze_root=bronze_root,
                silver_root=silver_root,
                market=market,
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            )
            report_path = write_symbol_report(
                silver_root=silver_root,
                market=market,
                exchange=exchange,
                symbol=symbol,
                report=report,
            )
            report_dict = report.to_dict()
            report_dict["report_path"] = report_path
            reports.append(report_dict)
            logger.info(
                "Silver report written market=%s symbol=%s rows_in=%s rows_out=%s path=%s",
                market,
                symbol,
                report.rows_in,
                report.rows_out,
                report_path,
            )

    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: silver-build reports=%s", len(reports))
