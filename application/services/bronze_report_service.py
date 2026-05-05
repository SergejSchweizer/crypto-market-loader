"""Bronze report generation over full stored period per symbol."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion.funding import FundingPoint
from ingestion.lake import load_funding_from_lake, load_open_interest_from_lake, load_spot_candles_from_lake
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _report_path(lake_root: str, dataset_type: str, exchange: str, symbol: str, timeframe: str) -> Path:
    return (
        Path(lake_root)
        / "reports"
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / "build_report.json"
    )


def _write_report(path: Path, payload: dict[str, object]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return str(path.resolve())


def _candle_report_payload(
    dataset_type: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    rows: list[SpotCandle],
) -> dict[str, object]:
    null_price_rows = sum(
        1
        for row in rows
        if row.open_price is None or row.high_price is None or row.low_price is None or row.close_price is None
    )
    invalid_ohlc_rows = sum(
        1
        for row in rows
        if row.high_price < max(row.open_price, row.close_price) or row.low_price > min(row.open_price, row.close_price)
    )
    timestamps = [row.open_time for row in rows]
    return {
        "dataset": f"{dataset_type}_1m",
        "rows_out": len(rows),
        "null_price_rows": null_price_rows,
        "invalid_ohlc_rows": invalid_ohlc_rows,
        "min_timestamp": _iso_utc(min(timestamps) if timestamps else None),
        "max_timestamp": _iso_utc(max(timestamps) if timestamps else None),
        "symbols": [symbol],
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
    }


def _oi_report_payload(exchange: str, symbol: str, timeframe: str, rows: list[OpenInterestPoint]) -> dict[str, object]:
    timestamps = [row.open_time for row in rows]
    return {
        "dataset": "oi",
        "rows_out": len(rows),
        "min_timestamp": _iso_utc(min(timestamps) if timestamps else None),
        "max_timestamp": _iso_utc(max(timestamps) if timestamps else None),
        "symbols": [symbol],
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
    }


def _funding_report_payload(exchange: str, symbol: str, timeframe: str, rows: list[FundingPoint]) -> dict[str, object]:
    timestamps = [row.open_time for row in rows]
    return {
        "dataset": "funding",
        "rows_out": len(rows),
        "min_timestamp": _iso_utc(min(timestamps) if timestamps else None),
        "max_timestamp": _iso_utc(max(timestamps) if timestamps else None),
        "symbols": [symbol],
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
    }


def build_bronze_symbol_reports(
    *,
    lake_root: str,
    spot_symbols: set[tuple[str, str, str]],
    perp_symbols: set[tuple[str, str, str]],
    oi_symbols: set[tuple[str, str, str]],
    funding_symbols: set[tuple[str, str, str]],
) -> list[str]:
    """Build full-period bronze reports for provided symbol sets.

    Symbol tuple format: ``(exchange, symbol, timeframe)``.
    """

    written: list[str] = []

    for exchange, symbol, timeframe in sorted(spot_symbols):
        rows = load_spot_candles_from_lake(
            lake_root=lake_root,
            market="spot",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        payload = _candle_report_payload("spot", exchange, symbol, timeframe, rows)
        written.append(_write_report(_report_path(lake_root, "spot", exchange, symbol, timeframe), payload))

    for exchange, symbol, timeframe in sorted(perp_symbols):
        rows = load_spot_candles_from_lake(
            lake_root=lake_root,
            market="perp",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        payload = _candle_report_payload("perp", exchange, symbol, timeframe, rows)
        written.append(_write_report(_report_path(lake_root, "perp", exchange, symbol, timeframe), payload))

    for exchange, symbol, timeframe in sorted(oi_symbols):
        rows = load_open_interest_from_lake(
            lake_root=lake_root,
            market="perp",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        payload = _oi_report_payload(exchange, symbol, timeframe, rows)
        written.append(_write_report(_report_path(lake_root, "oi", exchange, symbol, timeframe), payload))

    for exchange, symbol, timeframe in sorted(funding_symbols):
        rows = load_funding_from_lake(
            lake_root=lake_root,
            market="perp",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
        )
        payload = _funding_report_payload(exchange, symbol, timeframe, rows)
        written.append(_write_report(_report_path(lake_root, "funding", exchange, symbol, timeframe), payload))

    return written
