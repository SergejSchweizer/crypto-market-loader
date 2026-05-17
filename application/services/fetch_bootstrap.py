"""Shared bootstrap/history helper utilities for fetch orchestration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

TRow = TypeVar("TRow")


def fetch_bounded_daily_rows(
    *,
    day_windows: list[tuple[int, int]],
    range_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    dedupe_key: Callable[[TRow], int],
    on_history_chunk: Callable[[list[TRow]], None] | None,
) -> list[TRow]:
    """Fetch bounded history window-by-window with deterministic deduplication."""

    rows_buffer: list[TRow] = []
    for day_start_ms, day_end_ms in day_windows:
        day_rows = range_fetcher(
            start_open_ms=day_start_ms,
            end_open_ms=day_end_ms,
            **fetch_kwargs,
        )
        if day_rows and on_history_chunk is not None:
            on_history_chunk(day_rows)
        rows_buffer.extend(day_rows)
    unique_by_open_time = {dedupe_key(item): item for item in rows_buffer}
    return [unique_by_open_time[key] for key in sorted(unique_by_open_time)]


def fetch_bootstrap_history_rows(
    *,
    history_fetcher: Callable[..., list[TRow]],
    fetch_kwargs: dict[str, object],
    on_history_chunk: Callable[[list[TRow]], None] | None,
    wrap_chunk_callback: Callable[[Callable[[list[TRow]], None] | None], Callable[[list[TRow]], None] | None],
    filter_rows: Callable[[list[TRow]], list[TRow]],
) -> list[TRow]:
    """Run history fetch with wrapped callback and then return filtered rows."""

    rows = history_fetcher(
        on_history_chunk=wrap_chunk_callback(on_history_chunk),
        **fetch_kwargs,
    )
    return filter_rows(rows)
