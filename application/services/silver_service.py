"""Silver transformation service for monthly OHLCV outputs and symbol reports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for silver-build. Install project dependencies.") from exc
    return pl


@dataclass(frozen=True)
class SilverBuildReport:
    """Aggregated silver build report for one symbol across processed months."""

    dataset: str
    exchange: str
    symbol: str
    timeframe: str
    period_start: str | None
    period_end: str | None
    months_processed: list[str]
    rows_in: int
    rows_out: int
    duplicates_removed: int
    invalid_ohlc_rows: int
    null_price_rows: int
    min_timestamp: str | None
    max_timestamp: str | None
    symbols: list[str]

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "months_processed": self.months_processed,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "duplicates_removed": self.duplicates_removed,
            "invalid_ohlc_rows": self.invalid_ohlc_rows,
            "null_price_rows": self.null_price_rows,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "symbols": self.symbols,
        }


def _silver_month_path(
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
) -> Path:
    return (
        Path(silver_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"month={month}"
        / "data.parquet"
    )


def _bronze_month_files(
    bronze_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
) -> list[str]:
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"month={month}"
    )
    return sorted(str(path) for path in root.glob("date=*/data.parquet"))


def discover_symbols(bronze_root: str, market: str, exchange: str, timeframe: str = "1m") -> list[str]:
    """Discover symbols available in bronze for selected market/exchange/timeframe."""

    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
    )
    if not root.exists():
        return []
    symbols: list[str] = []
    for path in root.glob("symbol=*/timeframe=*"):
        symbol_segment = path.parent.name
        tf_segment = path.name
        if not symbol_segment.startswith("symbol=") or not tf_segment.startswith("timeframe="):
            continue
        if tf_segment.split("=", 1)[1] != timeframe:
            continue
        symbols.append(symbol_segment.split("=", 1)[1])
    return sorted(set(symbols))


def discover_months(bronze_root: str, market: str, exchange: str, symbol: str, timeframe: str = "1m") -> list[str]:
    """Discover available bronze months for one symbol."""

    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={market}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    if not root.exists():
        return []
    months: list[str] = []
    for path in root.glob("month=*"):
        name = path.name
        if not name.startswith("month="):
            continue
        months.append(name.split("=", 1)[1])
    return sorted(set(months))


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_silver_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly silver parquet outputs and aggregated report for one symbol."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market=market,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_ohlc_rows = 0
    agg_null_price_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(
            bronze_root=bronze_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        null_price_expr = (
            pl.col("open_price").is_null()
            | pl.col("high_price").is_null()
            | pl.col("low_price").is_null()
            | pl.col("close_price").is_null()
        )
        invalid_ohlc_expr = (
            (pl.col("high_price") < pl.max_horizontal("open_price", "close_price"))
            | (pl.col("low_price") > pl.min_horizontal("open_price", "close_price"))
        )

        null_price_rows = frame.select(null_price_expr.cast(pl.Int64).sum().alias("count")).item()
        invalid_ohlc_rows = frame.select(
            (~null_price_expr & invalid_ohlc_expr).cast(pl.Int64).sum().alias("count")
        ).item()
        cleaned = frame.filter(~null_price_expr & ~invalid_ohlc_expr)
        deduped = (
            cleaned.sort(["open_time", "ingested_at"])
            .unique(
                subset=["exchange", "instrument_type", "symbol", "timeframe", "open_time"],
                keep="last",
                maintain_order=True,
            )
            .sort("open_time")
        )
        duplicates_removed = cleaned.height - deduped.height

        target = _silver_month_path(
            silver_root=silver_root,
            market=market,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        deduped.write_parquet(target)

        month_min = deduped.select(pl.col("open_time").min()).item()
        month_max = deduped.select(pl.col("open_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += deduped.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_ohlc_rows += int(invalid_ohlc_rows)
        agg_null_price_rows += int(null_price_rows)

    report = SilverBuildReport(
        dataset=f"{market}_1m",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=agg_duplicates_removed,
        invalid_ohlc_rows=agg_invalid_ohlc_rows,
        null_price_rows=agg_null_price_rows,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
    )
    return report


def write_symbol_report(*, silver_root: str, market: str, exchange: str, symbol: str, report: SilverBuildReport) -> str:
    """Write aggregated symbol report JSON and return absolute path."""

    target = (
        Path(silver_root)
        / "reports"
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={report.timeframe}"
        / "build_report.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return str(target.resolve())
