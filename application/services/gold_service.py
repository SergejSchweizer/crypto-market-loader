"""Gold transformation service for per-symbol model-ready datasets."""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _require_polars() -> Any:
    try:
        import polars as pl
    except ImportError as exc:
        raise RuntimeError("polars is required for gold-build. Install project dependencies.") from exc
    return pl


@dataclass(frozen=True)
class GoldBuildReport:
    """Aggregated gold build report for one symbol."""

    exchange: str
    symbol: str
    rows_out: int
    columns: list[str]
    min_timestamp: str | None
    max_timestamp: str | None
    parquet_path: str
    manifest_path: str
    plot_path: str | None
    hash_string: str

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "rows_out": self.rows_out,
            "columns": self.columns,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "parquet_path": self.parquet_path,
            "manifest_path": self.manifest_path,
            "plot_path": self.plot_path,
            "hash_string": self.hash_string,
        }


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _git_commit_short() -> str:
    try:
        out = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], text=True).strip()
        return out or "nogit"
    except Exception:
        return "nogit"


def normalize_symbol(value: str) -> str:
    """Normalize to canonical base asset symbol used across the repo (e.g. BTC, ETH, SOL)."""

    raw = value.strip().upper()
    normalized = raw.replace("_", "-").replace("/", "-")
    parts = [part for part in normalized.split("-") if part]
    if parts:
        return parts[0]
    for candidate in ("BTC", "ETH", "SOL"):
        if raw.startswith(candidate):
            return candidate
    return raw


def discover_gold_symbols(silver_root: str, exchange: str) -> list[str]:
    """Discover symbols that have at least one required silver dataset."""

    required = [
        ("spot", "1m"),
        ("perp", "1m"),
        ("oi_1m_feature", "1m"),
        ("funding_1m_feature", "1m"),
    ]
    by_dataset: list[set[str]] = []
    for dataset_type, timeframe in required:
        root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
        symbols: set[str] = set()
        if root.exists():
            for path in root.glob("symbol=*/timeframe=*"):
                if path.name != f"timeframe={timeframe}":
                    continue
                parent = path.parent.name
                if parent.startswith("symbol="):
                    symbols.add(normalize_symbol(parent.split("=", 1)[1]))
        by_dataset.append(symbols)
    if not by_dataset:
        return []
    return sorted({normalize_symbol(item) for item in set.intersection(*by_dataset)})


def _read_dataset_frame(
    *,
    silver_root: str,
    exchange: str,
    symbol: str,
    dataset_type: str,
    timeframe: str,
) -> Any:
    pl = _require_polars()
    dataset_root = Path(silver_root) / f"dataset_type={dataset_type}" / f"exchange={exchange}"
    files: list[Path] = []
    symbol_dirs = sorted(dataset_root.glob(f"symbol=*/timeframe={timeframe}"))
    for sym_dir in symbol_dirs:
        sym_segment = sym_dir.parent.name
        if not sym_segment.startswith("symbol="):
            continue
        raw_symbol = sym_segment.split("=", 1)[1]
        if normalize_symbol(raw_symbol) != symbol:
            continue
        files.extend(sorted(sym_dir.glob("month=*/data.parquet")))
    if not files:
        raise ValueError(f"Missing silver dataset for symbol={symbol}: {dataset_type}")
    frame = pl.read_parquet([str(path) for path in files])
    return frame


def _prepare_spot_or_perp(pl: Any, frame: Any, prefix: str, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("open_time").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_price").cast(pl.Float64).alias(f"{prefix}_open_price"),
                pl.col("high_price").cast(pl.Float64).alias(f"{prefix}_high_price"),
                pl.col("low_price").cast(pl.Float64).alias(f"{prefix}_low_price"),
                pl.col("close_price").cast(pl.Float64).alias(f"{prefix}_close_price"),
                pl.col("volume").cast(pl.Float64).alias(f"{prefix}_volume"),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_oi(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp_m1").cast(pl.Datetime(time_unit="us", time_zone="UTC")),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("open_interest").cast(pl.Float64).alias("oi_open_interest"),
                pl.col("oi_is_observed").cast(pl.Boolean),
                pl.col("oi_is_ffill").cast(pl.Boolean),
                pl.col("minutes_since_oi_observation").cast(pl.Int64),
                pl.col("oi_observation_lag_sec").cast(pl.Int64),
            ]
        )
        .sort("timestamp_m1")
    )


def _prepare_funding(pl: Any, frame: Any, symbol: str) -> Any:
    return (
        frame.with_columns(
            [
                pl.col("timestamp").cast(pl.Datetime(time_unit="us", time_zone="UTC")).alias("timestamp_m1"),
                pl.lit(symbol).alias("symbol"),
            ]
        )
        .select(
            [
                "timestamp_m1",
                "exchange",
                "symbol",
                pl.col("funding_rate_last_known").cast(pl.Float64),
                pl.col("minutes_since_funding").cast(pl.Int64),
                pl.col("is_funding_observation_minute").cast(pl.Boolean),
                pl.col("funding_data_available").cast(pl.Boolean),
            ]
        )
        .sort("timestamp_m1")
    )


def _feature_hash(columns: list[str]) -> str:
    payload = "|".join(columns)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def _json_payload_hash(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]


def _write_feature_distribution_plot(frame: Any, output_path: Path) -> str | None:
    try:
        import matplotlib.dates as mdates
        import matplotlib.ticker as mticker
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    numeric_cols = [col for col, dtype in zip(frame.columns, frame.dtypes, strict=False) if dtype.is_numeric()]
    if not numeric_cols:
        return None

    row_count = len(numeric_cols)
    fig = plt.figure(figsize=(16, max(3.12 * row_count, 8.4)), facecolor="#0b1220", constrained_layout=True)
    grid = fig.add_gridspec(row_count, 2, width_ratios=[8, 2], wspace=0.08, hspace=0.40)
    fig.text(0.06, 0.985, "Feature", color="#e2e8f0", fontsize=11, fontweight="semibold", ha="left", va="top")
    fig.text(
        0.86,
        0.985,
        "Distribution",
        color="#e2e8f0",
        fontsize=11,
        fontweight="semibold",
        ha="center",
        va="top",
    )

    for idx, col in enumerate(numeric_cols):
        series_df = frame.select(["timestamp_m1", col]).drop_nulls().sort("timestamp_m1")
        values = series_df.get_column(col).to_list()
        ts = series_df.get_column("timestamp_m1").to_list()
        left_ax = fig.add_subplot(grid[idx, 0])
        right_ax = fig.add_subplot(grid[idx, 1])

        for axis in (left_ax, right_ax):
            axis.set_facecolor("#0f172a")
            axis.tick_params(colors="#cbd5e1", labelsize=8)
            axis.spines["top"].set_visible(False)
            axis.spines["right"].set_visible(False)
            axis.spines["left"].set_color("#64748b")
            axis.spines["bottom"].set_color("#64748b")

        if values:
            arr = [float(v) for v in values]
            arr_sorted = sorted(arr)
            p50 = arr_sorted[len(arr_sorted) // 2]
            p95 = arr_sorted[min(len(arr_sorted) - 1, int(len(arr_sorted) * 0.95))]
            missing_values = frame.height - len(arr)
            stats_text = "\n".join(
                [
                    f"feature: {col}",
                    f"count: {len(arr)}",
                    f"missing: {missing_values}",
                    f"mean: {sum(arr)/len(arr):.6g}",
                    f"min/max: {min(arr):.6g} / {max(arr):.6g}",
                    f"p50/p95: {p50:.6g} / {p95:.6g}",
                ]
            )
            left_ax.plot(ts, arr, color="#38bdf8", linewidth=1.6, alpha=0.95, label=stats_text)
            left_ax.fill_between(ts, arr, [min(arr)] * len(arr), color="#0ea5e9", alpha=0.16)
            left_ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            left_ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
            left_ax.tick_params(axis="x", rotation=0)
            right_ax.hist(arr, bins=30, color="#22d3ee", alpha=0.85)
            right_ax.xaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.4g}"))
        else:
            left_ax.text(
                0.02,
                0.5,
                f"feature: {col}\nno data",
                va="center",
                ha="left",
                color="#e2e8f0",
                fontsize=9.5,
                transform=left_ax.transAxes,
            )
        left_ax.legend(
            loc="upper left",
            frameon=True,
            framealpha=0.82,
            facecolor="#111827",
            edgecolor="#334155",
            labelcolor="#e2e8f0",
            fontsize=7.8,
            handlelength=1.6,
        )
        right_ax.set_title(col, color="#cbd5e1", fontsize=8.5, pad=2)
        right_ax.set_xlabel("value", color="#cbd5e1", fontsize=8)
        right_ax.set_yticks([])
        right_ax.tick_params(axis="x", colors="#cbd5e1", labelsize=7)
        right_ax.grid(alpha=0.25, linestyle="--", linewidth=0.8, color="#334155")

    fig.suptitle("Gold Feature Distributions", color="#e2e8f0", fontsize=14, fontweight="semibold")
    fig.savefig(output_path, dpi=180)
    plt.close(fig)
    return str(output_path.resolve())


def _feature_source_dataset(column_name: str) -> str:
    if column_name.startswith("spot_"):
        return "spot_1m"
    if column_name.startswith("perp_"):
        return "perp_1m"
    if column_name.startswith("oi_"):
        return "oi_1m_feature"
    if column_name.startswith("funding_"):
        return "funding_1m_feature"
    return "gold_merged"


def _feature_metadata(pl: Any, frame: Any, exchange: str) -> dict[str, dict[str, object]]:
    meta: dict[str, dict[str, object]] = {}
    for col, dtype in zip(frame.columns, frame.dtypes, strict=False):
        null_count = int(frame.select(pl.col(col).is_null().sum()).item())
        time_filtered = frame.filter(pl.col(col).is_not_null()) if col != "timestamp_m1" else frame
        feature_min_ts = time_filtered.select(pl.col("timestamp_m1").min()).item() if "timestamp_m1" in frame.columns else None
        feature_max_ts = time_filtered.select(pl.col("timestamp_m1").max()).item() if "timestamp_m1" in frame.columns else None
        row: dict[str, object] = {
            "dtype": str(dtype),
            "null_count": null_count,
            "missing_values": null_count,
            "non_null_count": int(frame.height - null_count),
            "source_dataset": _feature_source_dataset(col),
            "source_exchange": exchange,
            "time_range": {
                "min_timestamp": _iso_utc(feature_min_ts if isinstance(feature_min_ts, datetime) else None),
                "max_timestamp": _iso_utc(feature_max_ts if isinstance(feature_max_ts, datetime) else None),
            },
        }
        if dtype.is_numeric():
            stats = frame.select(
                [
                    pl.col(col).drop_nulls().count().alias("count"),
                    pl.col(col).drop_nulls().mean().alias("mean"),
                    pl.col(col).drop_nulls().std().alias("std"),
                    pl.col(col).drop_nulls().min().alias("min"),
                    pl.col(col).drop_nulls().max().alias("max"),
                ]
            ).to_dicts()[0]
            row.update(stats)
        meta[col] = row
    return meta


def build_gold_for_symbol(
    *,
    silver_root: str,
    gold_root: str,
    exchange: str,
    symbol: str,
    plot: bool = False,
) -> GoldBuildReport:
    """Build one gold parquet dataset + manifest for a symbol."""

    pl = _require_polars()
    symbol = normalize_symbol(symbol)
    spot_raw = _read_dataset_frame(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        dataset_type="spot",
        timeframe="1m",
    )
    perp_raw = _read_dataset_frame(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        dataset_type="perp",
        timeframe="1m",
    )
    oi_raw = _read_dataset_frame(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        dataset_type="oi_1m_feature",
        timeframe="1m",
    )
    funding_raw = _read_dataset_frame(
        silver_root=silver_root,
        exchange=exchange,
        symbol=symbol,
        dataset_type="funding_1m_feature",
        timeframe="1m",
    )

    spot = _prepare_spot_or_perp(pl, spot_raw, "spot", symbol)
    perp = _prepare_spot_or_perp(pl, perp_raw, "perp", symbol)
    oi = _prepare_oi(pl, oi_raw, symbol)
    funding = _prepare_funding(pl, funding_raw, symbol)

    key_cols = ["timestamp_m1", "exchange", "symbol"]
    merged = spot.join(perp, on=key_cols, how="full", coalesce=True)
    merged = merged.join(oi, on=key_cols, how="full", coalesce=True)
    merged = merged.join(funding, on=key_cols, how="full", coalesce=True)
    merged = merged.sort("timestamp_m1")

    cols = merged.columns
    min_ts = merged.select(pl.col("timestamp_m1").min()).item()
    max_ts = merged.select(pl.col("timestamp_m1").max()).item()
    manifest = {
        "dataset": "gold_symbol_dataset",
        "exchange": exchange,
        "symbol": symbol,
        "build_date_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "column_hash": _feature_hash(cols),
        "rows_out": merged.height,
        "columns": cols,
        "min_timestamp": _iso_utc(min_ts if isinstance(min_ts, datetime) else None),
        "max_timestamp": _iso_utc(max_ts if isinstance(max_ts, datetime) else None),
        "source_silver_datasets": {
            "spot_1m": {
                "columns": spot_raw.columns,
                "rows": spot_raw.height,
                "source_symbols": sorted(set(spot_raw.get_column("symbol").cast(pl.Utf8).to_list()))
                if "symbol" in spot_raw.columns
                else [],
            },
            "perp_1m": {
                "columns": perp_raw.columns,
                "rows": perp_raw.height,
                "source_symbols": sorted(set(perp_raw.get_column("symbol").cast(pl.Utf8).to_list()))
                if "symbol" in perp_raw.columns
                else [],
            },
            "oi_1m_feature": {
                "columns": oi_raw.columns,
                "rows": oi_raw.height,
                "source_symbols": sorted(set(oi_raw.get_column("symbol").cast(pl.Utf8).to_list()))
                if "symbol" in oi_raw.columns
                else [],
            },
            "funding_1m_feature": {
                "columns": funding_raw.columns,
                "rows": funding_raw.height,
                "source_symbols": sorted(set(funding_raw.get_column("symbol").cast(pl.Utf8).to_list()))
                if "symbol" in funding_raw.columns
                else [],
            },
        },
        "feature_metadata": _feature_metadata(pl, merged, exchange),
    }
    json_hash = _json_payload_hash(manifest)
    git_hash = _git_commit_short()
    hash_string = f"{json_hash}_{git_hash}"
    root = Path(gold_root)
    root.mkdir(parents=True, exist_ok=True)
    parquet_path = root / f"{symbol}_{hash_string}.parquet"
    merged.write_parquet(parquet_path)
    written_plot: str | None = None
    if plot:
        plot_path = root / f"{symbol}_{hash_string}.png"
        written_plot = _write_feature_distribution_plot(merged, plot_path)
    manifest["plot_generated"] = written_plot is not None
    manifest_path = root / f"{symbol}_{hash_string}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return GoldBuildReport(
        exchange=exchange,
        symbol=symbol,
        rows_out=merged.height,
        columns=cols,
        min_timestamp=manifest["min_timestamp"],
        max_timestamp=manifest["max_timestamp"],
        parquet_path=str(parquet_path.resolve()),
        manifest_path=str(manifest_path.resolve()),
        plot_path=written_plot,
        hash_string=hash_string,
    )
