"""Tests for fetch bootstrap helper module."""

from __future__ import annotations

from datetime import UTC, datetime

from application.services.fetch_bootstrap import fetch_bootstrap_history_rows, fetch_bounded_daily_rows
from ingestion.spot import SpotCandle


def _spot(open_time: datetime, close: float) -> SpotCandle:
    return SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=open_time,
        close_time=open_time,
        open_price=close,
        high_price=close,
        low_price=close,
        close_price=close,
        volume=1.0,
        quote_volume=1.0,
        trade_count=1,
    )


def test_fetch_bounded_daily_rows_dedupes_with_stable_last_write_wins() -> None:
    ts0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    ts1 = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    first = _spot(ts0, 1.0)
    replacement = _spot(ts0, 2.0)
    second = _spot(ts1, 3.0)

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = int(kwargs["start_open_ms"])
        if start_open_ms == 1:
            return [first, replacement]
        return [second]

    rows = fetch_bounded_daily_rows(
        day_windows=[(1, 2), (3, 4)],
        range_fetcher=_range_fetcher,
        fetch_kwargs={},
        dedupe_key=lambda row: int(row.open_time.timestamp() * 1000),
        on_history_chunk=None,
    )
    assert [row.close_price for row in rows] == [2.0, 3.0]


def test_fetch_bootstrap_history_rows_applies_callback_wrapper_and_filter() -> None:
    ts0 = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    ts1 = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    observed_chunk_lengths: list[int] = []

    def _history_fetcher(**kwargs: object) -> list[SpotCandle]:
        callback = kwargs["on_history_chunk"]
        assert callable(callback)
        callback([_spot(ts0, 1.0), _spot(ts1, 2.0)])
        return [_spot(ts0, 1.0), _spot(ts1, 2.0)]

    def _wrap(callback: object) -> object:
        assert callable(callback)

        def _wrapped(rows: list[SpotCandle]) -> None:
            callback(rows[:1])

        return _wrapped

    rows = fetch_bootstrap_history_rows(
        history_fetcher=_history_fetcher,
        fetch_kwargs={},
        on_history_chunk=lambda rows: observed_chunk_lengths.append(len(rows)),
        wrap_chunk_callback=_wrap,
        filter_rows=lambda rows: rows[1:],
    )
    assert observed_chunk_lengths == [1]
    assert len(rows) == 1
    assert rows[0].open_time == ts1
