"""Sample artifact generation service for loader runs."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TypeVar

from application.dto import ArtifactOptionsDTO, LoaderStorageDTO
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.plotting import save_candle_plots, save_funding_plot, save_open_interest_plot
from ingestion.spot import Market, SpotCandle

MAX_FULL_PLOT_POINTS = 1000
T = TypeVar("T")


def _funding_open_time(item: FundingPoint) -> datetime:
    """Return funding row open timestamp for stable sorting."""

    return item.open_time


def _downsample_full_period(items: list[T], max_points: int = MAX_FULL_PLOT_POINTS) -> list[T]:
    """Downsample a sorted series across its full period to at most ``max_points`` items."""

    if len(items) <= max_points:
        return items
    if max_points <= 1:
        return [items[-1]]

    last_index = len(items) - 1
    sampled_indices = [int(idx * last_index / (max_points - 1)) for idx in range(max_points)]
    return [items[item_index] for item_index in sampled_indices]


def write_loader_samples_dto(
    storage: LoaderStorageDTO,
    logger: logging.Logger,
    options: ArtifactOptionsDTO | None = None,
    save_candle_plots_fn: Callable[..., list[str]] = save_candle_plots,
    save_open_interest_plot_fn: Callable[..., str] = save_open_interest_plot,
    save_funding_plot_fn: Callable[..., str] = save_funding_plot,
) -> None:
    """Write full-history plot sample artifacts."""

    resolved_options = options or ArtifactOptionsDTO()
    sample_dir = Path("samples")
    sample_dir.mkdir(parents=True, exist_ok=True)

    def _safe_name(value: str) -> str:
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)

    layer_name = _safe_name(resolved_options.sample_layer.strip().lower() or "bronze")

    for market, candle_by_exchange in storage.candles.items():
        for exchange, candle_by_symbol in candle_by_exchange.items():
            for symbol_key, candles in candle_by_symbol.items():
                if not candles:
                    continue
                timeframe = candles[0].interval
                base_name = (
                    f"{layer_name}_{market}_{_safe_name(exchange)}_{_safe_name(symbol_key)}_"
                    f"{_safe_name(timeframe)}_sample_10_rows"
                )

                if resolved_options.generate_plots:
                    sorted_candles = sorted(candles, key=lambda value: value.open_time)
                    plot_candles = _downsample_full_period(sorted_candles)
                    generated = save_candle_plots_fn(
                        candles_by_exchange={exchange: {symbol_key: plot_candles}},
                        output_dir=str(sample_dir),
                        price_field="spot",
                    )
                    if generated:
                        source = Path(generated[0])
                        target = sample_dir / f"{base_name}.png"
                        if source.resolve() != target.resolve():
                            source.replace(target)
                        logger.info("Sample plot written path=%s", target)

    for market, oi_by_exchange in storage.open_interest.items():
        for exchange, oi_by_symbol in oi_by_exchange.items():
            for symbol_key, oi_items in oi_by_symbol.items():
                if not oi_items:
                    continue
                timeframe = oi_items[0].interval
                base_name = (
                    f"{layer_name}_oi_{market}_{_safe_name(exchange)}_"
                    f"{_safe_name(symbol_key)}_{_safe_name(timeframe)}_sample_10_rows"
                )

                if resolved_options.generate_plots:
                    sorted_items = _downsample_full_period(sorted(oi_items, key=lambda value: value.open_time))
                    plot_path = sample_dir / f"{base_name}.png"
                    save_open_interest_plot_fn(
                        exchange=exchange,
                        symbol=symbol_key,
                        interval=timeframe,
                        times=[item.open_time for item in sorted_items],
                        open_interest_values=[item.open_interest for item in sorted_items],
                        output_path=str(plot_path),
                    )
                    logger.info("Sample plot written path=%s", plot_path)

    for market, funding_by_exchange in storage.funding.items():
        for exchange, funding_by_symbol in funding_by_exchange.items():
            for symbol_key, funding_items in funding_by_symbol.items():
                if not funding_items:
                    continue
                timeframe = funding_items[0].interval
                base_name = (
                    f"{layer_name}_funding_{market}_{_safe_name(exchange)}_"
                    f"{_safe_name(symbol_key)}_{_safe_name(timeframe)}_sample_10_rows"
                )

                if resolved_options.generate_plots:
                    funding_sorted_items = _downsample_full_period(sorted(funding_items, key=_funding_open_time))
                    plot_path = sample_dir / f"{base_name}.png"
                    save_funding_plot_fn(
                        exchange=exchange,
                        symbol=symbol_key,
                        interval=timeframe,
                        times=[item.open_time for item in funding_sorted_items],
                        funding_values=[item.funding_rate for item in funding_sorted_items],
                        output_path=str(plot_path),
                    )
                    logger.info("Sample plot written path=%s", plot_path)


def write_loader_samples(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    logger: logging.Logger,
    generate_plots: bool = True,
    save_candle_plots_fn: Callable[..., list[str]] = save_candle_plots,
    save_open_interest_plot_fn: Callable[..., str] = save_open_interest_plot,
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    save_funding_plot_fn: Callable[..., str] = save_funding_plot,
) -> None:
    """Backward-compatible wrapper using DTO service."""

    write_loader_samples_dto(
        storage=LoaderStorageDTO(
            candles=candles_for_storage,
            open_interest=open_interest_for_storage,
            funding=funding_for_storage or {},
        ),
        logger=logger,
        options=ArtifactOptionsDTO(generate_plots=generate_plots),
        save_candle_plots_fn=save_candle_plots_fn,
        save_open_interest_plot_fn=save_open_interest_plot_fn,
        save_funding_plot_fn=save_funding_plot_fn,
    )
