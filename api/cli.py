"""Command-line interface for data ingestion tasks."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.lake import save_spot_candles_parquet_lake
from ingestion.plotting import PriceField, save_candle_plots
from ingestion.spot import (
    Exchange,
    SpotCandle,
    fetch_candles,
    list_supported_intervals,
    normalize_timeframe,
)


class SingleInstanceError(RuntimeError):
    """Raised when another CLI instance is already running."""


class SingleInstanceLock:
    """Non-blocking process lock backed by a lock file."""

    def __init__(self, lock_path: str) -> None:
        self.lock_path = Path(lock_path)
        self._fd: int | None = None

    def __enter__(self) -> "SingleInstanceLock":
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(self._fd)
            self._fd = None
            raise SingleInstanceError(
                "Another l2-synchronizer instance is already running. Exiting."
            ) from exc
        os.ftruncate(self._fd, 0)
        os.write(self._fd, str(os.getpid()).encode("utf-8"))
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None



def _serialize_candle(candle: SpotCandle) -> dict[str, object]:
    """Convert a ``SpotCandle`` into JSON-safe dictionary."""

    data = asdict(candle)
    for key in ("open_time", "close_time"):
        value = data[key]
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data



def build_parser() -> argparse.ArgumentParser:
    """Create top-level CLI parser."""

    parser = argparse.ArgumentParser(description="L2 Synchronizer CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    spot_parser = subparsers.add_parser("fetch-spot", help="Fetch candles from supported exchanges")
    spot_parser.add_argument("--exchange", choices=["binance", "deribit"], default="binance")
    spot_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit"],
        help="Optional list of exchanges to fetch in one run",
    )
    spot_parser.add_argument("--market", choices=["spot", "perp"], default="spot")
    spot_parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols or instrument aliases (exchange specific)",
    )
    spot_parser.add_argument(
        "--timeframe",
        "--interval",
        dest="timeframe",
        default="1h",
        help="Candle timeframe, e.g. M1, M5, H1, D1, 1m, 1h, 1d",
    )
    spot_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of candles per symbol (auto-paginates above 1000)",
    )
    spot_parser.add_argument("--plot", action="store_true", help="Create and save price/volume plots")
    spot_parser.add_argument("--plot-dir", default="plots", help="Output directory for generated plots")
    spot_parser.add_argument(
        "--plot-price",
        choices=["spot", "close", "open", "high", "low"],
        default="close",
        help="Price field to plot (spot maps to close)",
    )
    spot_parser.add_argument(
        "--save-parquet-lake",
        action="store_true",
        help="Save fetched candles to parquet lake partitions",
    )
    spot_parser.add_argument(
        "--lake-root",
        default="lake/bronze",
        help="Root directory for parquet lake files",
    )
    spot_parser.add_argument(
        "--no-json-output",
        action="store_true",
        help="Suppress JSON output from fetch-spot command",
    )

    tf_parser = subparsers.add_parser("list-spot-timeframes", help="List exchange-supported candle timeframes")
    tf_parser.add_argument("--exchange", choices=["binance", "deribit"], default="binance")
    tf_parser.add_argument(
        "--exchanges",
        nargs="+",
        choices=["binance", "deribit"],
        help="Optional list of exchanges to list in one run",
    )

    return parser



def main() -> None:
    """CLI entrypoint."""

    try:
        with SingleInstanceLock(".run/l2-synchronizer.lock"):
            parser = build_parser()
            args = parser.parse_args()

            if args.command == "fetch-spot":
                exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
                output: dict[str, object] = {}
                candles_for_plots: dict[str, dict[str, list[SpotCandle]]] = {}
                for exchange in exchanges:
                    exchange_output: dict[str, object] = {}
                    exchange_candles: dict[str, list[SpotCandle]] = {}
                    try:
                        timeframe = normalize_timeframe(exchange=exchange, value=args.timeframe)
                        for symbol in args.symbols:
                            try:
                                candles = fetch_candles(
                                    exchange=exchange,
                                    symbol=symbol,
                                    interval=timeframe,
                                    limit=args.limit,
                                    market=args.market,
                                )
                                symbol_key = symbol.upper()
                                exchange_output[symbol_key] = [_serialize_candle(item) for item in candles]
                                exchange_candles[symbol_key] = candles
                            except Exception as exc:  # noqa: BLE001
                                exchange_output[symbol.upper()] = {"error": str(exc)}
                    except Exception as exc:  # noqa: BLE001
                        exchange_output["_exchange_error"] = str(exc)
                    output[exchange] = exchange_output
                    if exchange_candles:
                        candles_for_plots[exchange] = exchange_candles

                if args.plot:
                    try:
                        saved_paths = save_candle_plots(
                            candles_by_exchange=candles_for_plots,
                            output_dir=args.plot_dir,
                            price_field=cast(PriceField, args.plot_price),
                        )
                        output["_plots"] = saved_paths
                    except Exception as exc:  # noqa: BLE001
                        output["_plot_error"] = str(exc)

                if args.save_parquet_lake:
                    try:
                        parquet_files = save_spot_candles_parquet_lake(
                            candles_by_exchange=candles_for_plots,
                            market=args.market,
                            lake_root=args.lake_root,
                        )
                        output["_parquet_files"] = parquet_files
                    except Exception as exc:  # noqa: BLE001
                        output["_parquet_error"] = str(exc)

                if not args.no_json_output:
                    print(json.dumps(output, indent=2))
            elif args.command == "list-spot-timeframes":
                exchanges = cast(list[Exchange], args.exchanges if args.exchanges else [args.exchange])
                output = {exchange: list(list_supported_intervals(exchange=exchange)) for exchange in exchanges}
                print(json.dumps(output, indent=2))
    except SingleInstanceError as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
