"""Plot generation utilities for fetched candle data."""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Literal

from ingestion.spot import SpotCandle

PriceField = Literal["spot", "close", "open", "high", "low"]

# Unified dark schema (aligned with silver plots).
_FIGURE_FACE = "#0b1220"
_AXIS_FACE = "#0f172a"
_GRID_COLOR = "#334155"
_SPINE_COLOR = "#64748b"
_TEXT_COLOR = "#e2e8f0"
_MUTED_TEXT_COLOR = "#cbd5e1"


def price_value(candle: SpotCandle, price_field: PriceField) -> float:
    """Select a price value from a candle based on requested field."""

    if price_field in {"spot", "close"}:
        return candle.close_price
    if price_field == "open":
        return candle.open_price
    if price_field == "high":
        return candle.high_price
    return candle.low_price


def build_plot_filename(
    exchange: str,
    symbol: str,
    interval: str,
    price_field: PriceField,
) -> str:
    """Build a stable output filename for a candle plot."""

    def safe(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)

    return f"{safe(exchange)}_{safe(symbol)}_{safe(interval)}_{safe(price_field)}.png"


def save_candle_plots(
    candles_by_exchange: dict[str, dict[str, list[SpotCandle]]],
    output_dir: str,
    price_field: PriceField,
) -> list[str]:
    """Render and save price/volume plots for fetched candles.

    Args:
        candles_by_exchange: Nested mapping ``exchange -> symbol -> candles``.
        output_dir: Directory where plots are written.
        price_field: Price field to plot (``spot`` maps to close).

    Returns:
        Absolute plot file paths.

    Raises:
        RuntimeError: If plotting dependency is unavailable.
    """

    try:
        import matplotlib.dates as mdates
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting. Install project dependencies first.") from exc

    plot_root = Path(output_dir)
    plot_root.mkdir(parents=True, exist_ok=True)

    saved_paths: list[str] = []
    for exchange, symbol_map in candles_by_exchange.items():
        for symbol, candles in symbol_map.items():
            if not candles:
                continue

            times = [item.open_time for item in candles]
            prices = [price_value(item, price_field=price_field) for item in candles]
            volumes = [item.volume for item in candles]
            interval = candles[0].interval

            figure, (price_axis, volume_axis) = plt.subplots(
                2,
                1,
                figsize=(12, 7),
                sharex=True,
                gridspec_kw={"height_ratios": [7, 3]},
            )
            figure.patch.set_facecolor(_FIGURE_FACE)

            for axis in (price_axis, volume_axis):
                axis.set_facecolor(_AXIS_FACE)
                axis.grid(alpha=0.35, linestyle="--", linewidth=0.8, color=_GRID_COLOR)
                axis.spines["top"].set_visible(False)
                axis.spines["right"].set_visible(False)
                axis.spines["left"].set_color(_SPINE_COLOR)
                axis.spines["bottom"].set_color(_SPINE_COLOR)
                axis.tick_params(axis="x", colors=_MUTED_TEXT_COLOR)
                axis.tick_params(axis="y", colors=_MUTED_TEXT_COLOR)

            price_axis.plot(times, prices, color="#38bdf8", linewidth=4.0, alpha=0.18)
            price_axis.plot(times, prices, color="#22d3ee", linewidth=2.2, label=f"{price_field} price")
            price_axis.fill_between(times, prices, min(prices), color="#0ea5e9", alpha=0.14)
            price_axis.set_ylabel(f"{price_field} price", color=_TEXT_COLOR)
            price_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
            price_axis.set_title(
                f"{exchange.upper()}  {symbol}  ({interval})",
                fontsize=13,
                fontweight="semibold",
                color=_TEXT_COLOR,
                pad=10,
            )
            price_axis.legend(
                loc="upper left",
                frameon=True,
                framealpha=0.75,
                facecolor="#020617",
                edgecolor=_SPINE_COLOR,
                labelcolor=_TEXT_COLOR,
                fontsize=9,
            )

            time_points = mdates.date2num(times)  # type: ignore[no-untyped-call]
            if len(time_points) > 1:
                deltas = [current - previous for previous, current in zip(time_points, time_points[1:], strict=False)]
                median_delta = sorted(deltas)[len(deltas) // 2]
                bar_width = max(median_delta * 0.58, 0.00005)
            else:
                bar_width = 0.0008

            volume_colors = ["#2f9e44"]
            volume_colors.extend(
                "#2f9e44" if current >= previous else "#c92a2a"
                for previous, current in zip(prices, prices[1:], strict=False)
            )
            volume_axis.bar(
                times,
                volumes,
                color=volume_colors,
                width=bar_width,
                alpha=0.82,
                linewidth=0.3,
                edgecolor="#e2e8f0",
            )
            volume_axis.set_ylabel("volume", color=_TEXT_COLOR)
            volume_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
            volume_axis.set_xlabel("time (UTC)", color=_TEXT_COLOR)
            volume_axis.xaxis.set_major_formatter(mdates.DateFormatter("%m.%Y"))  # type: ignore[no-untyped-call]
            volume_axis.legend(
                handles=[
                    mpatches.Patch(color="#2f9e44", label="Price up vs previous candle"),
                    mpatches.Patch(color="#c92a2a", label="Price down vs previous candle"),
                ],
                loc="upper left",
                frameon=True,
                framealpha=0.75,
                facecolor="#020617",
                edgecolor=_SPINE_COLOR,
                labelcolor=_TEXT_COLOR,
                fontsize=8,
            )

            figure.autofmt_xdate()
            figure.tight_layout()

            file_name = build_plot_filename(
                exchange=exchange,
                symbol=symbol,
                interval=interval,
                price_field=price_field,
            )
            file_path = plot_root / file_name

            figure.savefig(file_path, dpi=180)
            plt.close(figure)
            saved_paths.append(str(file_path.resolve()))

    return saved_paths


def save_open_interest_plot(
    exchange: str,
    symbol: str,
    interval: str,
    times: list[datetime],
    open_interest_values: list[float],
    output_path: str,
) -> str:
    """Render and save one open-interest line plot."""

    if not times or not open_interest_values:
        return output_path

    try:
        import matplotlib.dates as mdates
        import matplotlib.patches as mpatches
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting. Install project dependencies first.") from exc

    figure, (axis, delta_axis) = plt.subplots(
        2,
        1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={"height_ratios": [7, 3]},
    )
    figure.patch.set_facecolor(_FIGURE_FACE)
    for panel in (axis, delta_axis):
        panel.set_facecolor(_AXIS_FACE)
        panel.grid(alpha=0.35, linestyle="--", linewidth=0.8, color=_GRID_COLOR)
        panel.spines["top"].set_visible(False)
        panel.spines["right"].set_visible(False)
        panel.spines["left"].set_color(_SPINE_COLOR)
        panel.spines["bottom"].set_color(_SPINE_COLOR)
        panel.tick_params(axis="x", colors=_MUTED_TEXT_COLOR)
        panel.tick_params(axis="y", colors=_MUTED_TEXT_COLOR)

    axis.plot(times, open_interest_values, color="#2dd4bf", linewidth=2.2, label="open interest")  # type: ignore[arg-type]
    axis.fill_between(times, open_interest_values, min(open_interest_values), color="#14b8a6", alpha=0.14)  # type: ignore[arg-type]
    axis.set_title(
        f"{exchange.upper()}  {symbol}  ({interval})  Open Interest",
        fontsize=12,
        fontweight="semibold",
        color=_TEXT_COLOR,
        pad=10,
    )
    axis.set_ylabel("open interest", color=_TEXT_COLOR)
    axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    axis.legend(
        loc="upper left",
        frameon=True,
        framealpha=0.75,
        facecolor="#020617",
        edgecolor=_SPINE_COLOR,
        labelcolor=_TEXT_COLOR,
        fontsize=9,
    )

    deltas = [0.0]
    deltas.extend(curr - prev for prev, curr in zip(open_interest_values, open_interest_values[1:], strict=False))
    delta_colors = ["#22c55e" if value >= 0 else "#ef4444" for value in deltas]
    delta_axis.bar(times, deltas, color=delta_colors, alpha=0.82, linewidth=0.0)
    delta_axis.axhline(0.0, color="#64748b", linewidth=0.9, alpha=0.8)
    delta_axis.set_ylabel("delta", color=_TEXT_COLOR)
    delta_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
    delta_axis.set_xlabel("time (UTC)", color=_TEXT_COLOR)
    delta_axis.xaxis.set_major_formatter(mdates.DateFormatter("%m.%Y"))  # type: ignore[no-untyped-call]
    delta_axis.legend(
        handles=[
            mpatches.Patch(color="#22c55e", label="OI up"),
            mpatches.Patch(color="#ef4444", label="OI down"),
        ],
        loc="upper left",
        frameon=True,
        framealpha=0.75,
        facecolor="#020617",
        edgecolor=_SPINE_COLOR,
        labelcolor=_TEXT_COLOR,
        fontsize=8,
    )

    figure.autofmt_xdate()
    figure.tight_layout()

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=180)
    plt.close(figure)
    return str(path.resolve())


def save_funding_plot(
    exchange: str,
    symbol: str,
    interval: str,
    times: list[datetime],
    funding_values: list[float],
    output_path: str,
) -> str:
    """Render and save one funding-rate line plot."""

    if not times or not funding_values:
        return output_path

    try:
        import matplotlib.dates as mdates
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError as exc:
        raise RuntimeError("matplotlib is required for plotting. Install project dependencies first.") from exc

    figure, (axis, abs_axis) = plt.subplots(
        2,
        1,
        figsize=(12, 6),
        sharex=True,
        gridspec_kw={"height_ratios": [7, 3]},
    )
    figure.patch.set_facecolor(_FIGURE_FACE)
    for panel in (axis, abs_axis):
        panel.set_facecolor(_AXIS_FACE)
        panel.grid(alpha=0.35, linestyle="--", linewidth=0.8, color=_GRID_COLOR)
        panel.spines["top"].set_visible(False)
        panel.spines["right"].set_visible(False)
        panel.spines["left"].set_color(_SPINE_COLOR)
        panel.spines["bottom"].set_color(_SPINE_COLOR)
        panel.tick_params(axis="x", colors=_MUTED_TEXT_COLOR)
        panel.tick_params(axis="y", colors=_MUTED_TEXT_COLOR)

    axis.plot(times, funding_values, color="#a78bfa", linewidth=2.2, label="funding rate")  # type: ignore[arg-type]
    axis.fill_between(times, funding_values, 0.0, color="#8b5cf6", alpha=0.14)  # type: ignore[arg-type]
    axis.axhline(0.0, color="#64748b", linewidth=0.9, alpha=0.8)
    axis.set_title(
        f"{exchange.upper()}  {symbol}  ({interval})  Funding Rate",
        fontsize=12,
        fontweight="semibold",
        color=_TEXT_COLOR,
        pad=10,
    )
    axis.set_ylabel("funding rate", color=_TEXT_COLOR)
    axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.6f}"))
    axis.legend(
        loc="upper left",
        frameon=True,
        framealpha=0.75,
        facecolor="#020617",
        edgecolor=_SPINE_COLOR,
        labelcolor=_TEXT_COLOR,
        fontsize=9,
    )

    abs_values = [abs(value) for value in funding_values]
    abs_axis.plot(times, abs_values, color="#f59e0b", linewidth=1.8, label="abs(funding)")
    abs_axis.fill_between(times, abs_values, 0.0, color="#f59e0b", alpha=0.12)
    abs_axis.set_ylabel("abs rate", color=_TEXT_COLOR)
    abs_axis.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.6f}"))
    abs_axis.set_xlabel("time (UTC)", color=_TEXT_COLOR)
    abs_axis.xaxis.set_major_formatter(mdates.DateFormatter("%m.%Y"))  # type: ignore[no-untyped-call]
    abs_axis.legend(
        loc="upper left",
        frameon=True,
        framealpha=0.75,
        facecolor="#020617",
        edgecolor=_SPINE_COLOR,
        labelcolor=_TEXT_COLOR,
        fontsize=8,
    )

    figure.autofmt_xdate()
    figure.tight_layout()

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, dpi=180)
    plt.close(figure)
    return str(path.resolve())
