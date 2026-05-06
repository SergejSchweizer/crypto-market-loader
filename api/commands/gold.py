"""Gold build command for silver-to-gold symbol datasets."""

from __future__ import annotations

import argparse
import json
import logging
import re
from typing import cast

from application.services.gold_service import SUPPORTED_GOLD_DATASET_IDS, build_gold_for_symbol, discover_gold_symbols, normalize_symbol


def add_gold_build_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register ``gold-build`` parser."""

    parser = subparsers.add_parser("gold-build", help="Build gold per-symbol parquet datasets from silver data")
    parser.add_argument("--silver-root", default="lake/silver", help="Silver lake root")
    parser.add_argument("--gold-root", default="lake/gold", help="Gold lake root")
    parser.add_argument("--exchange", choices=["deribit"], default="deribit")
    parser.add_argument("--symbols", nargs="+", help="Optional symbol list; auto-discovered when omitted")
    parser.add_argument("--dataset-id", help="Gold dataset identifier (when omitted, build all supported datasets)")
    parser.add_argument("--dataset-version", default="v1.0.0", help="Semantic dataset version")
    parser.add_argument("--auto-version", action="store_true", help="Auto-increment semantic version from prior manifests")
    parser.add_argument("--version-base", default="v1.0.0", help="Base version used when auto-version has no prior manifest")
    parser.add_argument("--manifest", action="store_true", help="Generate gold manifest JSON")
    parser.add_argument("--plot", action="store_true", help="Generate gold feature distribution plot PNG")
    parser.add_argument("--no-json-output", action="store_true", help="Suppress JSON output")


def run_gold_build(args: argparse.Namespace, logger: logging.Logger) -> None:
    """Run gold build for configured symbols."""

    silver_root = cast(str, args.silver_root)
    gold_root = cast(str, args.gold_root)
    exchange = cast(str, args.exchange)
    dataset_id = cast(str | None, args.dataset_id)
    dataset_version = cast(str, args.dataset_version)
    auto_version = bool(getattr(args, "auto_version", False))
    version_base = cast(str, getattr(args, "version_base", "v1.0.0"))
    symbols = cast(list[str] | None, args.symbols)
    effective_symbols = (
        sorted({normalize_symbol(symbol) for symbol in symbols})
        if symbols
        else discover_gold_symbols(silver_root=silver_root, exchange=exchange)
    )
    reports: list[dict[str, object]] = []

    dataset_ids = [dataset_id] if dataset_id else sorted(SUPPORTED_GOLD_DATASET_IDS)
    logger.info("Gold build schedule symbols=%s dataset_ids=%s", effective_symbols, dataset_ids)
    version_re = re.compile(r"^v\d+\.\d+\.\d+$")
    if not auto_version and not version_re.fullmatch(dataset_version):
        raise ValueError(f"Invalid --dataset-version '{dataset_version}'. Expected semantic version like v1.0.0")
    if auto_version and not version_re.fullmatch(version_base):
        raise ValueError(f"Invalid --version-base '{version_base}'. Expected semantic version like v1.0.0")
    for selected_dataset_id in dataset_ids:
        for symbol in effective_symbols:
            try:
                report = build_gold_for_symbol(
                    silver_root=silver_root,
                    gold_root=gold_root,
                    exchange=exchange,
                    symbol=symbol,
                    dataset_id=selected_dataset_id,
                    dataset_version=dataset_version,
                    auto_version=auto_version,
                    version_base=version_base,
                    manifest=bool(getattr(args, "manifest", False)),
                    plot=bool(getattr(args, "plot", False)),
                )
            except ValueError as exc:
                logger.warning(
                    "Gold dataset skipped symbol=%s dataset_id=%s reason=%s",
                    symbol,
                    selected_dataset_id,
                    exc,
                )
                continue
            reports.append(report.to_dict())
            logger.info(
                "Gold dataset written symbol=%s dataset_id=%s rows_out=%s path=%s",
                symbol,
                selected_dataset_id,
                report.rows_out,
                report.parquet_path,
            )

    if not bool(args.no_json_output):
        print(json.dumps({"reports": reports}, indent=2))
    logger.info("Command complete: gold-build reports=%s", len(reports))
