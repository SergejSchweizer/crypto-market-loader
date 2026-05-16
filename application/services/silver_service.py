"""Silver transformation service for monthly outputs and symbol reports."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from application.services.gold_service import _feature_hash, _feature_metadata, _write_feature_distribution_plot


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
SILVER_TRADES_M1_FEATURE_COLUMNS = [
    "timestamp_m1",
    "exchange",
    "symbol",
    "instrument_type",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "quote_volume",
    "trade_count",
    "buy_volume",
    "sell_volume",
    "buy_trade_count",
    "sell_trade_count",
    "buy_volume_share",
]
SILVER_TRADES_OBSERVED_COLUMNS = [
    "trade_time",
    "exchange",
    "symbol",
    "instrument_type",
    "trade_id",
    "price",
    "quantity",
    "side",
]


def _build_trade_feature_frame(pl: Any, frame: Any, *, symbol: str) -> Any:
    """Build 1m trade-flow feature frame from observed tick trades."""

    enriched = frame.with_columns(
        [
            pl.col("trade_time").dt.truncate("1m").alias("timestamp_m1"),
            (pl.col("price") * pl.col("quantity")).alias("notional"),
            (pl.col("side") == "buy").alias("is_buy"),
            (pl.col("side") == "sell").alias("is_sell"),
        ]
    )
    return (
        enriched.group_by(["timestamp_m1", "exchange", "symbol", "instrument_type"], maintain_order=True)
        .agg(
            [
                pl.col("price").first().alias("open_price"),
                pl.col("price").max().alias("high_price"),
                pl.col("price").min().alias("low_price"),
                pl.col("price").last().alias("close_price"),
                pl.col("quantity").sum().alias("volume"),
                pl.col("notional").sum().alias("quote_volume"),
                pl.len().cast(pl.Int64).alias("trade_count"),
                pl.col("quantity").filter(pl.col("is_buy")).sum().fill_null(0.0).alias("buy_volume"),
                pl.col("quantity").filter(pl.col("is_sell")).sum().fill_null(0.0).alias("sell_volume"),
                pl.col("is_buy").cast(pl.Int64).sum().alias("buy_trade_count"),
                pl.col("is_sell").cast(pl.Int64).sum().alias("sell_trade_count"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("volume") > 0.0)
                .then(pl.col("buy_volume") / pl.col("volume"))
                .otherwise(0.0)
                .alias("buy_volume_share"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .sort("timestamp_m1")
        .select(SILVER_TRADES_M1_FEATURE_COLUMNS)
    )


def _build_trade_observed_frame(pl: Any, frame: Any) -> tuple[Any, int, int]:
    """Validate/clean raw bronze trade rows and return observed ticks + quality counts."""

    typed = frame.with_columns(
        [
            pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("trade_time"),
            pl.col("price").cast(pl.Float64),
            pl.col("quantity").cast(pl.Float64),
            pl.col("trade_id").cast(pl.Utf8),
            pl.col("side").cast(pl.Utf8).str.to_lowercase(),
            pl.col("symbol").cast(pl.Utf8).alias("symbol"),
            pl.col("exchange").cast(pl.Utf8).str.to_lowercase().alias("exchange"),
            pl.col("instrument_type").cast(pl.Utf8).str.to_lowercase().alias("instrument_type"),
        ]
    )
    invalid_expr = (
        pl.col("trade_time").is_null()
        | pl.col("trade_id").is_null()
        | pl.col("price").is_null()
        | (~pl.col("price").is_finite())
        | (pl.col("price") <= 0.0)
        | pl.col("quantity").is_null()
        | (~pl.col("quantity").is_finite())
        | (pl.col("quantity") <= 0.0)
    )
    invalid_rows = int(typed.select(invalid_expr.cast(pl.Int64).sum()).item() or 0)
    cleaned = typed.filter(~invalid_expr)
    observed = (
        cleaned.sort(["trade_time", "ingested_at"])
        .unique(
            subset=["exchange", "instrument_type", "symbol", "trade_time", "trade_id"],
            keep="last",
            maintain_order=True,
        )
        .sort("trade_time")
        .select(SILVER_TRADES_OBSERVED_COLUMNS)
    )
    return observed, invalid_rows, cleaned.height


def _silver_month_path(
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    timeframe: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
    )


def _silver_funding_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / "dataset_type=funding_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
    )


def _silver_oi_feature_month_path(
    silver_root: str,
    exchange: str,
    symbol: str,
    month: str,
) -> Path:
    year = month.split("-", 1)[0]
    stem = f"{symbol}-{month}"
    return (
        Path(silver_root)
        / "dataset_type=oi_1m_feature"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / "timeframe=1m"
        / f"year={year}"
        / f"month={month}"
        / f"{stem}.parquet"
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
    year = month.split("-", 1)[0]
    root = (
        Path(bronze_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
    )
    files = {
        *root.glob(f"year={year}/month={month}/date=*/data.parquet"),
        *root.glob(f"month={month}/date=*/data.parquet"),
    }
    return sorted(str(path) for path in files)


def discover_symbols(
    bronze_root: str,
    market: str,
    exchange: str,
    timeframe: str = "1m",
    instrument_type: str | None = None,
) -> list[str]:
    """Discover symbols available in bronze for selected market/exchange/timeframe."""

    instrument = instrument_type or market
    root = Path(bronze_root) / f"dataset_type={market}" / f"exchange={exchange}" / f"instrument_type={instrument}"
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
    months: set[str] = set()
    for path in root.glob("year=*/month=*"):
        name = path.name
        if name.startswith("month="):
            months.add(name.split("=", 1)[1])
    for path in root.glob("month=*"):
        name = path.name
        if name.startswith("month="):
            months.add(name.split("=", 1)[1])
    return sorted(months)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_silver_plot(frame: Any, output_path: Path) -> str | None:
    pl = _require_polars()
    ts_col = next(
        (candidate for candidate in ("timestamp_m1", "open_time", "timestamp") if candidate in frame.columns), None
    )
    if ts_col is None:
        return None
    if ts_col != "timestamp_m1":
        frame = frame.with_columns(pl.col(ts_col).alias("timestamp_m1"))
    if "exchange" not in frame.columns:
        frame = frame.with_columns(pl.lit("deribit").alias("exchange"))
    if "symbol" not in frame.columns:
        frame = frame.with_columns(pl.lit("unknown").alias("symbol"))
    return _write_feature_distribution_plot(frame, output_path, normalize_y=False)


def _with_timestamp_m1(frame: Any) -> Any:
    pl = _require_polars()
    ts_col = next(
        (candidate for candidate in ("timestamp_m1", "open_time", "timestamp") if candidate in frame.columns), None
    )
    if ts_col is None or ts_col == "timestamp_m1":
        return frame
    return frame.with_columns(pl.col(ts_col).alias("timestamp_m1"))


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
        invalid_ohlc_expr = (pl.col("high_price") < pl.max_horizontal("open_price", "close_price")) | (
            pl.col("low_price") > pl.min_horizontal("open_price", "close_price")
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
    cutoff_time: datetime | None = None,
) -> SilverBuildReport:
    """Build monthly ``funding_1m_feature`` from ``funding_observed`` using backward asof joins.

    Args:
        silver_root: Path to silver layer root.
        exchange: Exchange identifier.
        symbol: Trading symbol.
        observed_timeframe: Funding observation timeframe (default: 8h).
        cutoff_time: Latest timestamp to include in calendar. Defaults to now (UTC).
            Prevents forward-carrying of funding data beyond current time.
    """

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
            for path in observed_root.glob("year=*/month=*/*.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    if cutoff_time is None:
        cutoff_time = datetime.now(UTC)

    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        year = month.split("-", 1)[0]
        month_file = observed_root / f"year={year}" / f"month={month}" / f"{symbol}-{month}.parquet"
        if not month_file.exists():
            continue
        observed = pl.read_parquet(month_file).sort("funding_time")
        if observed.height == 0:
            continue
        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        if month == "9999-12":
            continue
        observed_max = observed.select(pl.col("funding_time").max()).item()
        if not isinstance(observed_max, datetime):
            continue
        month_end_exclusive = observed_max + timedelta(minutes=1)
        if cutoff_time < observed_max:
            month_end_exclusive = cutoff_time + timedelta(minutes=1)
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
                    ((pl.col("timestamp") - pl.col("funding_observed_at")).dt.total_minutes().cast(pl.Int64)).alias(
                        "minutes_since_funding"
                    ),
                    (pl.col("timestamp") == pl.col("funding_observed_at"))
                    .fill_null(False)
                    .alias("is_funding_observation_minute"),
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
    cutoff_time: datetime | None = None,
) -> SilverBuildReport:
    """Build monthly ``oi_1m_feature`` from ``oi_observed`` using backward asof join.

    Args:
        silver_root: Root path for silver datasets.
        exchange: Exchange identifier.
        symbol: Trading symbol.
        observed_timeframe: Input observation timeframe.
        cutoff_time: Latest timestamp to include in generated feature output.
            Defaults to now (UTC)."""

    if cutoff_time is None:
        cutoff_time = datetime.now(UTC)

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
            for path in observed_root.glob("year=*/month=*/*.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        year = month.split("-", 1)[0]
        month_file = observed_root / f"year={year}" / f"month={month}" / f"{symbol}-{month}.parquet"
        if not month_file.exists():
            continue
        observed = pl.read_parquet(month_file).sort("timestamp")
        if observed.height == 0:
            continue

        month_start = datetime.fromisoformat(f"{month}-01T00:00:00+00:00")
        observed_max = observed.select(pl.col("timestamp").max()).item()
        if not isinstance(observed_max, datetime):
            continue
        month_end_exclusive = observed_max + timedelta(minutes=1)
        if cutoff_time < observed_max:
            month_end_exclusive = cutoff_time + timedelta(minutes=1)
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


def build_perp_trades_1m_feature_for_symbol(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    observed_timeframe: str = "tick",
    observed_dataset_type: str = "perp_trades_observed",
    output_dataset_type: str = "perp_trades_1m_feature",
) -> SilverBuildReport:
    """Build monthly trade 1m features from observed tick-trade data."""

    pl = _require_polars()
    observed_root = (
        Path(silver_root)
        / f"dataset_type={observed_dataset_type}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={observed_timeframe}"
    )
    months = sorted(
        {
            path.parent.name.split("=", 1)[1]
            for path in observed_root.glob("year=*/month=*/*.parquet")
            if path.parent.name.startswith("month=")
        }
    )
    agg_rows_in = 0
    agg_rows_out = 0
    min_timestamp: datetime | None = None
    max_timestamp: datetime | None = None

    for month in months:
        year = month.split("-", 1)[0]
        month_file = observed_root / f"year={year}" / f"month={month}" / f"{symbol}-{month}.parquet"
        if not month_file.exists():
            continue
        frame = pl.read_parquet(month_file).sort("trade_time")
        rows_in = frame.height
        if rows_in == 0:
            continue

        feature = _build_trade_feature_frame(pl, frame, symbol=symbol)

        target = _silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe="1m",
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

        agg_rows_in += rows_in
        agg_rows_out += feature.height

    return SilverBuildReport(
        dataset=output_dataset_type,
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
        columns=SILVER_TRADES_M1_FEATURE_COLUMNS,
    )


def build_perp_trades_observed_for_symbol(
    *,
    bronze_root: str,
    silver_root: str,
    exchange: str,
    symbol: str,
    instrument_type: str = "perp",
    timeframe: str = "tick",
    bronze_dataset_type: str = "trades",
    output_dataset_type: str = "perp_trades_observed",
) -> SilverBuildReport:
    """Build monthly observed tick-trade dataset from bronze trade records."""

    pl = _require_polars()
    months = discover_months(
        bronze_root=bronze_root,
        market=bronze_dataset_type,
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        instrument_type=instrument_type,
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
            market=bronze_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
            instrument_type=instrument_type,
        )
        if not files:
            continue
        frame = pl.scan_parquet(files).collect()
        rows_in = frame.height
        if rows_in == 0:
            continue
        observed, invalid_rows, cleaned_rows = _build_trade_observed_frame(pl, frame)
        duplicates_removed = cleaned_rows - observed.height
        target = _silver_month_path(
            silver_root=silver_root,
            market=output_dataset_type,
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            month=month,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        observed.write_parquet(target)

        month_min = observed.select(pl.col("trade_time").min()).item()
        month_max = observed.select(pl.col("trade_time").max()).item()
        if isinstance(month_min, datetime) and (min_timestamp is None or month_min < min_timestamp):
            min_timestamp = month_min
        if isinstance(month_max, datetime) and (max_timestamp is None or month_max > max_timestamp):
            max_timestamp = month_max

        agg_rows_in += rows_in
        agg_rows_out += observed.height
        agg_duplicates_removed += int(duplicates_removed)
        agg_invalid_rows += int(invalid_rows)

    return SilverBuildReport(
        dataset=output_dataset_type,
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
        columns=SILVER_TRADES_OBSERVED_COLUMNS,
    )


def write_monthly_sidecars(
    *,
    silver_root: str,
    market: str,
    exchange: str,
    symbol: str,
    report: SilverBuildReport,
    write_manifest: bool = True,
    plot: bool = False,
) -> tuple[list[str], list[str]]:
    """Write per-month manifest and plot sidecars next to silver monthly parquet files."""

    pl = _require_polars()
    base_root = (
        Path(silver_root)
        / f"dataset_type={market}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"timeframe={report.timeframe}"
    )
    manifest_paths: list[str] = []
    plot_paths: list[str] = []

    for month in report.months_processed:
        year = month.split("-", 1)[0]
        stem = f"{symbol}-{month}"
        parquet_path = base_root / f"year={year}" / f"month={month}" / f"{stem}.parquet"
        if not parquet_path.exists():
            continue
        frame = pl.read_parquet(parquet_path)
        frame_for_gold = _with_timestamp_m1(frame)
        plotted: str | None = None

        if plot:
            plotted = _write_silver_plot(frame_for_gold, parquet_path.with_suffix(".png"))
            if plotted is not None:
                plot_paths.append(plotted)

        if write_manifest:
            min_ts: datetime | None = None
            max_ts: datetime | None = None
            if "timestamp_m1" in frame_for_gold.columns and frame_for_gold.height > 0:
                min_v = frame_for_gold.select(pl.col("timestamp_m1").min()).item()
                max_v = frame_for_gold.select(pl.col("timestamp_m1").max()).item()
                min_ts = min_v if isinstance(min_v, datetime) else None
                max_ts = max_v if isinstance(max_v, datetime) else None
            payload = {
                "dataset": report.dataset,
                "exchange": report.exchange,
                "symbol": report.symbol,
                "build_date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "column_hash": _feature_hash(frame.columns),
                "rows_out": frame.height,
                "columns": frame.columns,
                "min_timestamp": _iso_utc(min_ts),
                "max_timestamp": _iso_utc(max_ts),
                "source_silver_datasets": {
                    report.dataset: {
                        "columns": frame.columns,
                        "rows": frame.height,
                        "source_symbols": sorted(set(frame.get_column("symbol").cast(pl.Utf8).to_list()))
                        if "symbol" in frame.columns
                        else [report.symbol],
                    }
                },
                "feature_metadata": _feature_metadata(pl, frame_for_gold, report.exchange),
                "plot_generated": plotted is not None,
            }
            manifest_path = parquet_path.with_suffix(".json")
            manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            manifest_paths.append(str(manifest_path.resolve()))

    return manifest_paths, plot_paths
