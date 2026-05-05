"""Silver transformation service for monthly outputs and symbol reports."""

from __future__ import annotations

import json
import re
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
    columns: list[str]

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
            "columns": self.columns,
        }


SILVER_OHLCV_COLUMNS = [
    "schema_version",
    "dataset_type",
    "exchange",
    "symbol",
    "instrument_type",
    "event_time",
    "ingested_at",
    "run_id",
    "source_endpoint",
    "open_time",
    "close_time",
    "timeframe",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trade_count",
    "origin_payload",
]
SILVER_FUNDING_OBSERVED_COLUMNS = [
    "funding_time",
    "exchange",
    "symbol",
    "base_asset",
    "instrument_type",
    "funding_rate",
    "funding_interval_hours",
    "ingested_at_min",
    "ingested_at_max",
    "source_row_count",
    "silver_built_at",
    "data_quality_status",
]
SILVER_FUNDING_FEATURE_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "funding_rate_last_known",
    "funding_observed_at",
    "minutes_since_funding",
    "is_funding_observation_minute",
    "funding_data_available",
]
SILVER_OI_OBSERVED_COLUMNS = [
    "timestamp",
    "exchange",
    "symbol",
    "open_interest",
    "oi_source_timestamp",
    "ingested_at",
    "source_endpoint",
]
SILVER_OI_M1_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "open_interest",
    "oi_is_observed",
    "oi_is_ffill",
    "minutes_since_oi_observation",
    "oi_observation_lag_sec",
    "oi_source_timestamp",
]


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
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"month={month}"
        / "data.parquet"
    )


def _silver_funding_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    return (
        Path(silver_root)
        / "dataset_type=funding_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"month={month}"
        / "data.parquet"
    )


def _silver_oi_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    return (
        Path(silver_root)
        / "dataset_type=oi_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
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
    instrument_type: str | None = None,
) -> list[str]:
    instrument = instrument_type or market
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"month={month}"
    )
    return sorted(str(path) for path in root.glob("date=*/data.parquet"))


def discover_symbols(
    bronze_root: str,
    market: str,
    exchange: str,
    timeframe: str = "1m",
    instrument_type: str | None = None,
) -> list[str]:
    """Discover symbols available in bronze for selected market/exchange/timeframe."""

    instrument = instrument_type or market
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
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


def discover_months(
    bronze_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
    instrument_type: str | None = None,
) -> list[str]:
    """Discover available bronze months for one symbol."""

    instrument = instrument_type or market
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
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


def _normalize_symbol_expr(pl: Any, col_name: str = "symbol") -> Any:
    return (
        pl.col(col_name)
        .cast(pl.Utf8)
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"[\s/]+", "-")
        .str.replace_all("_", "-")
    )


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
        columns=SILVER_OHLCV_COLUMNS,
    )
    return report


def build_funding_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly ``funding_observed`` silver outputs and aggregated report."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market="funding",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    agg_null_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(
            bronze_root=bronze_root,
            market="funding",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type="perp",
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        frame = frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("funding_time"),
                pl.col("funding_rate").cast(pl.Float64),
                pl.col("symbol").cast(pl.Utf8).alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).alias("exchange"),
                pl.col("instrument_type").cast(pl.Utf8).alias("instrument_type"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
            ]
        )
        frame = frame.filter(pl.col("instrument_type") == "perp")

        null_rate_expr = pl.col("funding_rate").is_null()
        invalid_rate_expr = (~null_rate_expr) & (
            ~pl.col("funding_rate").is_finite() | (pl.col("funding_rate").abs() > 1.0)
        )
        null_rows = frame.select(null_rate_expr.cast(pl.Int64).sum().alias("count")).item()
        invalid_rows = frame.select(invalid_rate_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~null_rate_expr & ~invalid_rate_expr)

        observed = (
            cleaned.group_by(["exchange", "symbol", "funding_time"], maintain_order=True)
            .agg(
                [
                    pl.col("funding_rate").last(),
                    pl.col("instrument_type").last(),
                    pl.col("ingested_at").min().alias("ingested_at_min"),
                    pl.col("ingested_at").max().alias("ingested_at_max"),
                    pl.len().cast(pl.Int64).alias("source_row_count"),
                ]
            )
            .with_columns(
                [
                    pl.col("symbol").str.split("-").list.first().alias("base_asset"),
                    pl.lit(8).cast(pl.Int64).alias("funding_interval_hours"),
                    pl.lit(datetime.now(UTC))
                    .cast(pl.Datetime(time_unit="us", time_zone="UTC"))
                    .alias("silver_built_at"),
                    pl.lit("ok").alias("data_quality_status"),
                ]
            )
            .select(
                [
                    "funding_time",
                    "exchange",
                    "symbol",
                    "base_asset",
                    "instrument_type",
                    "funding_rate",
                    "funding_interval_hours",
                    "ingested_at_min",
                    "ingested_at_max",
                    "source_row_count",
                    "silver_built_at",
                    "data_quality_status",
                ]
            )
            .sort("funding_time")
        )

        duplicates_removed = cleaned.height - observed.height
        target = _silver_month_path(
            silver_root=silver_root,
            market="funding_observed",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("funding_time").min()).item()
        month_max = observed.select(pl.col("funding_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)
        agg_null_rows += int(null_rows)

    return SilverBuildReport(
        dataset="funding_observed",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=agg_duplicates_removed,
        invalid_ohlc_rows=agg_invalid_rows,
        null_price_rows=agg_null_rows,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_FUNDING_OBSERVED_COLUMNS,
    )


def build_funding_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "8h",
) -> SilverBuildReport:
    """Build monthly ``funding_1m_feature`` from ``funding_observed`` using backward asof joins."""

    pl = _require_polars()
    observed_root = (
        Path(silver_root)
        / "dataset_type=funding_observed"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={observed_timeframe}"
    )
    months = sorted(
        {
            path.parent.name.split("=", 1)[1]
            for path in observed_root.glob("month=*/data.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        month_file = observed_root / f"month={month}" / "data.parquet"
        if not month_file.exists():
            continue
        observed = pl.read_parquet(month_file).sort("funding_time")
        if observed.height == 0:
            continue
        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        if month == "9999-12":
            continue
        y, m = month.split("-")
        year = int(y)
        mon = int(m)
        if mon == 12:
            month_end_dt = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            month_end_dt = datetime(year, mon + 1, 1, tzinfo=UTC)
        month_end_exclusive = month_end_dt
        calendar = pl.DataFrame(
            {
                "timestamp": pl.datetime_range(
                    start=month_start,
                    end=month_end_exclusive,
                    interval="1m",
                    closed="left",
                    time_zone="UTC",
                    eager=True,
                )
            }
        )
        right = observed.select(
            [
                pl.col("funding_time"),
                pl.col("funding_rate").alias("funding_rate_last_known"),
                pl.col("funding_time").alias("funding_observed_at"),
            ]
        )
        joined = calendar.join_asof(
            right,
            left_on="timestamp",
            right_on="funding_time",
            strategy="backward",
        )
        feature = (
            joined.with_columns(
                [
                    pl.lit(exchange).alias("exchange"),
                    pl.lit(symbol).alias("symbol"),
                    (
                        (pl.col("timestamp") - pl.col("funding_observed_at")).dt.total_minutes().cast(pl.Int64)
                    ).alias("minutes_since_funding"),
                    (pl.col("timestamp") == pl.col("funding_observed_at")).fill_null(False).alias(
                        "is_funding_observation_minute"
                    ),
                    pl.col("funding_observed_at").is_not_null().alias("funding_data_available"),
                ]
            )
            .select(
                [
                    "timestamp",
                    "exchange",
                    "symbol",
                    "funding_rate_last_known",
                    "funding_observed_at",
                    "minutes_since_funding",
                    "is_funding_observation_minute",
                    "funding_data_available",
                ]
            )
            .sort("timestamp")
        )

        # Hard leakage guard.
        leakage_count = feature.filter(
            pl.col("funding_observed_at").is_not_null() & (pl.col("funding_observed_at") > pl.col("timestamp"))
        ).height
        if leakage_count > 0:
            raise ValueError(f"Funding leakage detected for {exchange}/{symbol}/{month}: {leakage_count} rows")

        target = _silver_funding_feature_month_path(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        feature.write_parquet(target)

        month_min = feature.select(pl.col("timestamp").min()).item()
        month_max = feature.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += observed.height
        agg_rows_out += feature.height

    return SilverBuildReport(
        dataset="funding_1m_feature",
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_FUNDING_FEATURE_COLUMNS,
    )


def build_oi_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly ``oi_observed`` silver outputs from bronze OI observations."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market="oi",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type="perp",
    )
    agg_rows_in = 0
    agg_rows_out = 0
    agg_duplicates_removed = 0
    agg_invalid_rows = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        files = _bronze_month_files(
            bronze_root=bronze_root,
            market="oi",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type="perp",
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue

        frame = frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp"),
                pl.col("open_interest").cast(pl.Float64).alias("open_interest"),
                _normalize_symbol_expr(pl, "symbol").alias("symbol"),
                pl.col("exchange").cast(pl.Utf8).str.strip_chars().str.to_lowercase().alias("exchange"),
                pl.col("ingested_at").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("ingested_at"),
                pl.col("source_endpoint").cast(pl.Utf8).alias("source_endpoint"),
            ]
        )
        if "oi_is_observed" in frame.columns:
            frame = frame.filter(pl.col("oi_is_observed").fill_null(False))

        invalid_expr = (
            pl.col("timestamp").is_null()
            | pl.col("symbol").is_null()
            | (pl.col("symbol").str.len_chars() == 0)
            | pl.col("open_interest").is_null()
            | (~pl.col("open_interest").is_finite())
            | (pl.col("open_interest") < 0.0)
        )
        invalid_rows = frame.select(invalid_expr.cast(pl.Int64).sum().alias("count")).item()
        cleaned = frame.filter(~invalid_expr)
        observed = (
            cleaned.unique(
                subset=["exchange", "symbol", "timestamp", "open_interest"],
                keep="last",
                maintain_order=True,
            )
            .sort(["exchange", "symbol", "timestamp"])
            .with_columns(pl.col("timestamp").alias("oi_source_timestamp"))
            .select(
                [
                    "timestamp",
                    "exchange",
                    "symbol",
                    "open_interest",
                    "oi_source_timestamp",
                    "ingested_at",
                    "source_endpoint",
                ]
            )
        )
        duplicates_removed = cleaned.height - observed.height

        target = _silver_month_path(
            silver_root=silver_root,
            market="oi_observed",
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("timestamp").min()).item()
        month_max = observed.select(pl.col("timestamp").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)

    return SilverBuildReport(
        dataset="oi_observed",
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=agg_duplicates_removed,
        invalid_ohlc_rows=agg_invalid_rows,
        null_price_rows=0,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_OI_OBSERVED_COLUMNS,
    )


def build_oi_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "1m",
) -> SilverBuildReport:
    """Build monthly ``oi_1m_feature`` from ``oi_observed`` using backward asof join."""

    pl = _require_polars()
    observed_root = (
        Path(silver_root)
        / "dataset_type=oi_observed"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={observed_timeframe}"
    )
    months = sorted(
        {
            path.parent.name.split("=", 1)[1]
            for path in observed_root.glob("month=*/data.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        month_file = observed_root / f"month={month}" / "data.parquet"
        if not month_file.exists():
            continue
        observed = pl.read_parquet(month_file).sort("timestamp")
        if observed.height == 0:
            continue

        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        y, m = month.split("-")
        year = int(y)
        mon = int(m)
        month_end_exclusive = datetime(year + 1, 1, 1, tzinfo=UTC) if mon == 12 else datetime(year, mon + 1, 1, tzinfo=UTC)
        calendar = pl.DataFrame(
            {
                "timestamp_m1": pl.datetime_range(
                    start=month_start,
                    end=month_end_exclusive,
                    interval="1m",
                    closed="left",
                    time_zone="UTC",
                    eager=True,
                )
            }
        )
        right = observed.select(
            [
                pl.col("timestamp").alias("oi_source_timestamp"),
                pl.col("open_interest").alias("open_interest_observed"),
            ]
        )
        joined = calendar.join_asof(
            right.sort("oi_source_timestamp"),
            left_on="timestamp_m1",
            right_on="oi_source_timestamp",
            strategy="backward",
        )
        feature = (
            joined.with_columns(
                [
                    pl.lit(exchange).alias("exchange"),
                    pl.lit(symbol).alias("symbol"),
                    pl.col("open_interest_observed").alias("open_interest"),
                    (pl.col("timestamp_m1") == pl.col("oi_source_timestamp")).fill_null(False).alias("oi_is_observed"),
                    (pl.col("timestamp_m1") != pl.col("oi_source_timestamp")).fill_null(True).alias("oi_is_ffill"),
                    ((pl.col("timestamp_m1") - pl.col("oi_source_timestamp")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_oi_observation"
                    ),
                    ((pl.col("timestamp_m1") - pl.col("oi_source_timestamp")).dt.total_seconds().cast(pl.Int64)).alias(
                        "oi_observation_lag_sec"
                    ),
                ]
            )
            .select(
                [
                    "timestamp_m1",
                    "exchange",
                    "symbol",
                    "open_interest",
                    "oi_is_observed",
                    "oi_is_ffill",
                    "minutes_since_oi_observation",
                    "oi_observation_lag_sec",
                    "oi_source_timestamp",
                ]
            )
            .sort("timestamp_m1")
        )

        leakage_count = feature.filter(
            pl.col("oi_source_timestamp").is_not_null() & (pl.col("oi_source_timestamp") > pl.col("timestamp_m1"))
        ).height
        if leakage_count > 0:
            raise ValueError(f"OI leakage detected for {exchange}/{symbol}/{month}: {leakage_count} rows")

        target = _silver_oi_feature_month_path(
            silver_root=silver_root,
            exchange=exchange,
            symbol=symbol,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        feature.write_parquet(target)

        month_min = feature.select(pl.col("timestamp_m1").min()).item()
        month_max = feature.select(pl.col("timestamp_m1").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += observed.height
        agg_rows_out += feature.height

    return SilverBuildReport(
        dataset="oi_1m_feature",
        exchange=exchange,
        symbol=symbol,
        timeframe="1m",
        period_start=months[0] if months else None,
        period_end=months[-1] if months else None,
        months_processed=months,
        rows_in=agg_rows_in,
        rows_out=agg_rows_out,
        duplicates_removed=0,
        invalid_ohlc_rows=0,
        null_price_rows=0,
        min_timestamp=_iso_utc(min_timestamp),
        max_timestamp=_iso_utc(max_timestamp),
        symbols=[symbol],
        columns=SILVER_OI_M1_FEATURE_COLUMNS,
    )


def write_symbol_report(*, silver_root: str, market: str, exchange: str, symbol: str, report: SilverBuildReport) -> str:
    """Write aggregated symbol manifest JSON and return absolute path."""

    target = (
        Path(silver_root)
        / "reports"
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={report.timeframe}"
        / "manifest.json"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report.to_dict(), indent=2), encoding="utf-8")
    return str(target.resolve())


def build_samples_plot_filename(zone: str, exchange: str, symbol: str, market: str) -> str:
    """Build normalized plot filename ``zone_exchange_symbol_market.png``."""

    def _safe(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("_")

    return f"{_safe(zone)}_{_safe(exchange)}_{_safe(symbol)}_{_safe(market)}.png"


def _downsample_series(
    xs: list[object],
    ys: list[float],
    *,
    max_points: int = 3000,
) -> tuple[list[object], list[float]]:
    """Downsample paired series to at most ``max_points`` with evenly spaced indices."""

    if max_points <= 0:
        raise ValueError("max_points must be positive")
    length = min(len(xs), len(ys))
    if length <= max_points:
        return xs[:length], ys[:length]
    if length == 0:
        return [], []

    target_points = min(max_points, length)
    if target_points == 1:
        sampled_indices = [0]
    else:
        sampled_indices = sorted(
            {
                int(round(i * (length - 1) / (target_points - 1)))
                for i in range(target_points)
            }
        )
    return [xs[index] for index in sampled_indices], [ys[index] for index in sampled_indices]


def _aggregate_even_buckets(
    xs: list[object],
    ys: list[float],
    volumes: list[float] | None = None,
    *,
    max_points: int = 3000,
) -> tuple[list[object], list[float], list[float] | None]:
    """Aggregate series into evenly sized buckets to avoid aliasing zigzags."""

    if max_points <= 0:
        raise ValueError("max_points must be positive")
    length = min(len(xs), len(ys))
    if volumes is not None:
        length = min(length, len(volumes))
    if length == 0:
        return [], [], [] if volumes is not None else None
    if length <= max_points:
        base_volumes = volumes[:length] if volumes is not None else None
        return xs[:length], ys[:length], base_volumes

    bucket_count = max_points
    agg_xs: list[object] = []
    agg_ys: list[float] = []
    agg_vols: list[float] | None = [] if volumes is not None else None
    for i in range(bucket_count):
        start = int(round(i * length / bucket_count))
        end = int(round((i + 1) * length / bucket_count))
        if end <= start:
            continue
        chunk_y = ys[start:end]
        mid = start + ((end - start) // 2)
        agg_xs.append(xs[mid])
        agg_ys.append(sum(chunk_y) / len(chunk_y))
        if agg_vols is not None and volumes is not None:
            agg_vols.append(sum(volumes[start:end]))
    return agg_xs, agg_ys, agg_vols


def write_symbol_plot(
    *,
    silver_root: str,
    zone: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    period_start: str | None,
    period_end: str | None,
    report: SilverBuildReport | None = None,
    samples_root: str = "samples",
) -> str | None:
    """Render one professional plot per symbol from silver parquet output."""

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for silver plot generation.") from exc

    pl = _require_polars()

    def _report_legend_lines() -> list[str]:
        if report is None:
            return []
        months = f"{len(report.months_processed)}m"
        return [
            f"rows in/out: {report.rows_in}/{report.rows_out}",
            f"dups removed: {report.duplicates_removed}",
            f"invalid/null: {report.invalid_ohlc_rows}/{report.null_price_rows}",
            f"months: {months}",
            f"range: {report.min_timestamp or '-'} -> {report.max_timestamp or '-'}",
        ]

    def _dataset_type_from_report(value: str) -> str:
        if value in {"spot_1m", "perp_1m"}:
            return value.split("_", 1)[0]
        return value

    data_zone = (
        _dataset_type_from_report(report.dataset)
        if (zone == "silver" and report is not None)
        else zone
    )
    plot_market = "funding_1m" if data_zone == "funding_1m_feature" else data_zone
    if data_zone == "oi_1m_feature":
        plot_market = "oi_m1_feature"
    data_root = (
        Path(silver_root)
        / f"dataset_type={data_zone}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    files = sorted(data_root.glob("month=*/data.parquet"))
    if not files:
        return None

    frame = pl.read_parquet([str(path) for path in files])
    if frame.height == 0:
        return None

    if data_zone in {"funding_observed", "oi_observed"}:
        return None

    if data_zone in {"spot", "perp"}:
        x_col = "open_time"
        y_col = "close_price"
        y_label = "Close Price"
        title_suffix = "Price"
    elif data_zone == "funding_1m_feature":
        x_col = "timestamp"
        y_col = "funding_rate_last_known"
        y_label = "Funding Rate (Last Known)"
        title_suffix = "Funding 1m Feature"
    elif data_zone == "oi_1m_feature":
        x_col = "timestamp_m1"
        y_col = "open_interest"
        y_label = "Open Interest"
        title_suffix = "OI M1 Feature"
    else:
        return None

    series = frame.select([x_col, y_col]).drop_nulls().sort(x_col)
    if series.height == 0:
        return None

    xs = series.get_column(x_col).to_list()
    ys = series.get_column(y_col).to_list()
    values = [float(value) for value in ys if value is not None]
    if not values:
        return None

    if data_zone in {"spot", "perp"}:
        if "volume" in frame.columns:
            volume_series = frame.select([x_col, "volume"]).drop_nulls().sort(x_col)
            volume_map = {
                item[0]: float(item[1])
                for item in volume_series.iter_rows()
                if item[0] is not None and item[1] is not None
            }
            base_volumes = [volume_map.get(x, 0.0) for x in xs]
        else:
            base_volumes = [0.0] * len(xs)
        xs, values, volume_values = _aggregate_even_buckets(xs, values, base_volumes, max_points=3000)
        if volume_values is None:
            volume_values = [0.0] * len(xs)

        fig, (price_ax, volume_ax) = plt.subplots(
            2,
            1,
            figsize=(13, 7.2),
            sharex=True,
            gridspec_kw={"height_ratios": [7, 3]},
            constrained_layout=True,
        )
        fig.patch.set_facecolor("#0b1220")
        for axis in (price_ax, volume_ax):
            axis.set_facecolor("#0f172a")
            axis.grid(alpha=0.35, linestyle="--", linewidth=0.8, color="#334155")
            axis.spines["top"].set_visible(False)
            axis.spines["right"].set_visible(False)
            axis.spines["left"].set_color("#64748b")
            axis.spines["bottom"].set_color("#64748b")
            axis.tick_params(colors="#cbd5e1")

        price_ax.plot(xs, values, color="#38bdf8", linewidth=2.0)
        price_ax.fill_between(xs, values, [min(values)] * len(values), color="#0ea5e9", alpha=0.22)
        price_ax.set_title(
            f"{exchange.upper()} {symbol} {title_suffix}",
            fontsize=13.5,
            fontweight="semibold",
            color="#e2e8f0",
            pad=10,
        )
        price_ax.set_ylabel(y_label, color="#cbd5e1")
        price_ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.2f}"))
        price_ax.legend(
            [f"{title_suffix}"],
            loc="upper left",
            frameon=True,
            framealpha=0.78,
            facecolor="#111827",
            edgecolor="#334155",
            labelcolor="#e2e8f0",
            fontsize=8.5,
        )
        legend_lines = _report_legend_lines()
        if legend_lines:
            price_ax.text(
                0.01,
                0.99,
                "\n".join(legend_lines),
                transform=price_ax.transAxes,
                va="top",
                ha="left",
                fontsize=8.5,
                color="#e2e8f0",
                bbox={"facecolor": "#111827", "alpha": 0.82, "edgecolor": "#334155"},
            )

        volume_ax.fill_between(xs, volume_values, [0.0] * len(volume_values), color="#2563eb", alpha=0.5)
        volume_ax.plot(xs, volume_values, color="#60a5fa", linewidth=1.2, alpha=0.95)
        volume_ax.set_ylabel("Volume", color="#cbd5e1")
        volume_ax.set_xlabel("Time (UTC)", color="#cbd5e1")
        volume_ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
        volume_ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        volume_ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        fig.autofmt_xdate(rotation=0, ha="center")
    else:
        xs, values = _downsample_series(xs, values, max_points=3000)
        fig, ax = plt.subplots(figsize=(13, 6.5), constrained_layout=True)
        fig.patch.set_facecolor("#0b1220")
        ax.set_facecolor("#0f172a")
        ax.plot(xs, values, color="#22d3ee", linewidth=2.0)
        ax.fill_between(xs, values, [min(values)] * len(values), color="#0891b2", alpha=0.22)
        ax.grid(alpha=0.35, linestyle="--", linewidth=0.8, color="#334155")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.spines["left"].set_color("#64748b")
        ax.spines["bottom"].set_color("#64748b")
        ax.tick_params(colors="#cbd5e1")
        ax.set_title(
            f"{exchange.upper()} {symbol} {title_suffix}",
            fontsize=13.5,
            fontweight="semibold",
            color="#e2e8f0",
            pad=10,
        )
        ax.set_xlabel("Time (UTC)", color="#cbd5e1")
        ax.set_ylabel(y_label, color="#cbd5e1")
        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.6f}" if "Rate" in y_label else "{x:,.2f}"))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.legend(
            [y_label],
            loc="upper left",
            frameon=True,
            framealpha=0.78,
            facecolor="#111827",
            edgecolor="#334155",
            labelcolor="#e2e8f0",
            fontsize=8.5,
        )
        legend_lines = _report_legend_lines()
        if legend_lines:
            ax.text(
                0.01,
                0.99,
                "\n".join(legend_lines),
                transform=ax.transAxes,
                va="top",
                ha="left",
                fontsize=8.5,
                color="#e2e8f0",
                bbox={"facecolor": "#111827", "alpha": 0.82, "edgecolor": "#334155"},
            )
        fig.autofmt_xdate(rotation=0, ha="center")

    filename = build_samples_plot_filename(zone=zone, exchange=exchange, symbol=symbol, market=plot_market)
    output_path = Path(samples_root) / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata = {
        "zone": zone,
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "period_start": period_start or "",
        "period_end": period_end or "",
    }
    if report is not None:
        metadata.update(
            {
                "report_dataset": report.dataset,
                "report_rows_in": str(report.rows_in),
                "report_rows_out": str(report.rows_out),
                "report_duplicates_removed": str(report.duplicates_removed),
                "report_invalid_ohlc_rows": str(report.invalid_ohlc_rows),
                "report_null_price_rows": str(report.null_price_rows),
                "report_min_timestamp": report.min_timestamp or "",
                "report_max_timestamp": report.max_timestamp or "",
                "report_months_processed": ",".join(report.months_processed),
            }
        )
    fig.savefig(output_path, dpi=180, metadata=metadata)
    plt.close(fig)
    return str(output_path.resolve())
