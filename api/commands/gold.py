"""Gold build command for silver-to-gold symbol datasets."""

from __future__ import annotations

import argparse
import json
import logging
from typing import cast

from application.services.gold_service import build_gold_for_symbol, discover_gold_symbols, normalize_symbol


def add_gold_build_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``gold-build`` parser."""

    parser = subparsers.add_parser("gold-build", help="Build gold per-symbol parquet datasets from silver data")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--gold-root", default="lake/gold", help="Gold lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument("--plot", action="store_true", help="Generate gold feature distribution plot PNG")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_gold_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run gold build for configured symbols."""

    silver_root = cast(str, args.silver_root)
    gold_root = cast(str, args.gold_root)
    exchange = cast(str, args.exchange)
    symbols = cast(list[str] | None, args.symbols)
    effective_symbols = (
        sorted({normalize_symbol(symbol) for symbol in symbols})
        if symbols
        else discover_gold_symbols(silver_root=silver_root, exchange=exchange)
    )
    reports: list[dict[str, object]] = []

    logger.info("Gold build schedule symbols=%s", effective_symbols)
    for symbol in effective_symbols:
        report = build_gold_for_symbol(
            silver_root=silver_root,
            gold_root=gold_root,
            exchange=exchange,
            symbol=symbol,
            plot=bool(getattr(args, "plot", False)),
        )
        reports.append(report.to_dict())
        logger.info("Gold report written symbol=%s rows_out=%s path=%s", symbol, report.rows_out, report.parquet_path)

    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: gold-build reports=%s", len(reports))
