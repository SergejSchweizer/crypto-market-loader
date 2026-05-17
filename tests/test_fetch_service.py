"""Tests for fetch orchestration service."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, cast

import pytest

import application.services.fetch_service as fetch_service_module
from application.dto import CandleFetchTaskDTO, FundingFetchTaskDTO, OpenInterestFetchTaskDTO, TradeFetchTaskDTO
from application.services.fetch_service import (
    _filter_chunk_callback,
    _filter_rows_by_start_bound,
    _heartbeat_seconds,
    _ranges_in_random_order,
    _run_with_optional_timeout,
    _split_range_into_utc_days,
    _task_timeout_seconds,
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_funding,
    fetch_symbol_open_interest,
    fetch_symbol_trades,
    fetch_trade_tasks_parallel,
)
from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import SpotCandle
from ingestion.trades import TradeTick


def _sleep_then_empty(**kwargs: object) -> list[SpotCandle]:
    del kwargs
    import time

    time.sleep(0.2)
    return []


def _sleep_then_empty_oi(**kwargs: object) -> list[OpenInterestPoint]:
    del kwargs
    import time

    time.sleep(0.2)
    return []


def test_fetch_candle_tasks_parallel_splits_success_and_errors() -> None:
    task_ok = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m")
    task_fail = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="ETHUSDT", timeframe="1m")

    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.5,
        volume=10.0,
        quote_volume=1000.0,
        trade_count=10,
    )

    def fake_symbol_fetcher(
        exchange: str, market: str, symbol: str, timeframe: str, lake_root: str
    ) -> list[SpotCandle]:
        del exchange, market, timeframe, lake_root
        if symbol == "ETHUSDT":
            raise RuntimeError("boom")
        return [candle]

    result = fetch_candle_tasks_parallel(
        tasks=[task_ok, task_fail],
        lake_root="lake/bronze",
        concurrency=2,
        logger=logging.getLogger("test"),
        symbol_fetcher=fake_symbol_fetcher,
    )

    assert (task_ok.exchange, task_ok.market, task_ok.symbol, task_ok.timeframe) in result.rows
    assert (task_fail.exchange, task_fail.market, task_fail.symbol, task_fail.timeframe) in result.errors


def test_fetch_open_interest_tasks_parallel_splits_success_and_errors() -> None:
    task_ok = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")
    task_fail = OpenInterestFetchTaskDTO(exchange="deribit", symbol="ETHUSDT", timeframe="1m")

    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=2000.0,
    )

    def fake_symbol_fetcher(
        exchange: str, market: str, symbol: str, timeframe: str, lake_root: str
    ) -> list[OpenInterestPoint]:
        del exchange, market, timeframe, lake_root
        if symbol == "ETHUSDT":
            raise RuntimeError("boom")
        return [point]

    result = fetch_open_interest_tasks_parallel(
        tasks=[task_ok, task_fail],
        lake_root="lake/bronze",
        concurrency=2,
        logger=logging.getLogger("test"),
        symbol_fetcher=fake_symbol_fetcher,
    )

    assert (task_ok.exchange, task_ok.symbol, task_ok.timeframe) in result.rows
    assert (task_fail.exchange, task_fail.symbol, task_fail.timeframe) in result.errors


def test_fetch_symbol_candles_tail_delta_only_uses_latest_open_time() -> None:
    interval_ms = 60_000
    latest_open_time = datetime(2026, 4, 27, 10, 3, tzinfo=UTC)
    end_open_ms = int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        start_open_ms = int(cast(Any, kwargs["start_open_ms"]))
        gap_end_ms = int(cast(Any, kwargs["end_open_ms"]))
        calls.append((start_open_ms, gap_end_ms))
        return []

    candles = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        symbol_normalizer=lambda **kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **kwargs: interval_ms,
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: [],
        range_fetcher=_range_fetcher,
        latest_open_time_reader=lambda **kwargs: latest_open_time,
        tail_delta_only=True,
    )

    assert candles == []
    expected_start_ms = int(datetime(2026, 4, 27, 10, 4, tzinfo=UTC).timestamp() * 1000)
    assert calls == [(expected_start_ms, end_open_ms)]


def test_fetch_symbol_candles_tail_delta_only_uses_start_bound_when_no_history() -> None:
    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2022, 4, 29, 0, 2, tzinfo=UTC).timestamp() * 1000)
    on_start = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [
            SpotCandle(
                exchange="deribit",
                symbol="BTCUSDT",
                interval="1m",
                open_time=on_start,
                close_time=on_start,
                open_price=1.0,
                high_price=1.0,
                low_price=1.0,
                close_price=1.0,
                volume=1.0,
                quote_volume=1.0,
                trade_count=1,
            )
        ]

    candles = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        symbol_normalizer=lambda **kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        latest_open_time_reader=lambda **kwargs: None,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in candles] == [on_start]


def test_fetch_symbol_open_interest_tail_latest_none_uses_start_bound_range_fetch() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 10, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=10.0,
        open_interest_value=20.0,
    )

    def _range_fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "1m",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        latest_open_time_reader=lambda **kwargs: None,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )
    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in rows] == [point.open_time]


def test_fetch_symbol_funding_tail_latest_none_uses_start_bound_range_fetch() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2026, 4, 27, 8, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    point = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2026, 4, 27, 0, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 7, 59, 59, tzinfo=UTC),
        funding_rate=0.001,
        index_price=1.0,
        mark_price=1.0,
    )

    def _range_fetcher(**kwargs: object) -> list[FundingPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "8h",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        latest_open_time_reader=lambda **kwargs: None,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )
    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in rows] == [point.open_time]


def test_fetch_symbol_open_interest_tail_latest_none_uses_history_fetch_and_dedupes() -> None:
    point_time = datetime(2026, 4, 27, 10, 0, tzinfo=UTC)
    duplicate = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=point_time,
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=10.0,
        open_interest_value=20.0,
    )
    replacement = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=point_time,
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=11.0,
        open_interest_value=21.0,
    )
    next_point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=UTC),
        open_interest=12.0,
        open_interest_value=22.0,
    )

    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "1m",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: int(datetime(2026, 4, 27, 10, 2, tzinfo=UTC).timestamp() * 1000),
        latest_open_time_reader=lambda **kwargs: None,
        history_fetcher=lambda **kwargs: [duplicate, replacement, next_point],
        range_fetcher=lambda **kwargs: pytest.fail("range_fetcher should not be called when no start bound is set"),
        tail_delta_only=True,
    )

    assert [item.open_time for item in rows] == [point_time, next_point.open_time]
    assert rows[0].open_interest == replacement.open_interest


def test_fetch_symbol_funding_tail_latest_none_uses_history_fetch_and_dedupes() -> None:
    point_time = datetime(2026, 4, 27, 0, 0, tzinfo=UTC)
    duplicate = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=point_time,
        close_time=datetime(2026, 4, 27, 7, 59, 59, tzinfo=UTC),
        funding_rate=0.001,
        index_price=1.0,
        mark_price=1.0,
    )
    replacement = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=point_time,
        close_time=datetime(2026, 4, 27, 7, 59, 59, tzinfo=UTC),
        funding_rate=0.002,
        index_price=1.0,
        mark_price=1.0,
    )
    next_point = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2026, 4, 27, 8, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 15, 59, 59, tzinfo=UTC),
        funding_rate=0.003,
        index_price=1.0,
        mark_price=1.0,
    )

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "8h",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **kwargs: int(datetime(2026, 4, 27, 16, 0, tzinfo=UTC).timestamp() * 1000),
        latest_open_time_reader=lambda **kwargs: None,
        history_fetcher=lambda **kwargs: [duplicate, replacement, next_point],
        range_fetcher=lambda **kwargs: pytest.fail("range_fetcher should not be called when no start bound is set"),
        tail_delta_only=True,
    )

    assert [item.open_time for item in rows] == [point_time, next_point.open_time]
    assert rows[0].funding_rate == replacement.funding_rate


def test_fetch_symbol_candles_full_gap_fill_includes_head_gap_from_start_bound() -> None:
    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    existing_first = datetime(2022, 4, 29, 0, 3, tzinfo=UTC)
    end_open_ms = int(datetime(2022, 4, 29, 0, 5, tzinfo=UTC).timestamp() * 1000)
    interval_ms = 60_000
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return []

    candles = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [existing_first],
        symbol_normalizer=lambda **kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **kwargs: interval_ms,
        now_open_resolver=lambda **kwargs: end_open_ms,
        ranges_builder=lambda **kwargs: [],
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert candles == []
    assert calls == [(start_bound_ms, int(existing_first.timestamp() * 1000) - interval_ms)]


def test_split_range_into_utc_days_splits_cross_day_windows() -> None:
    start_ms = int(datetime(2026, 4, 27, 23, 59, tzinfo=UTC).timestamp() * 1000)
    end_ms = int(datetime(2026, 4, 28, 0, 1, tzinfo=UTC).timestamp() * 1000)
    windows = _split_range_into_utc_days(start_ms, end_ms)
    assert len(windows) == 2
    assert windows[0][0] == start_ms
    assert windows[0][1] == int(datetime(2026, 4, 27, 23, 59, 59, 999000, tzinfo=UTC).timestamp() * 1000)
    assert windows[1][0] == int(datetime(2026, 4, 28, 0, 0, tzinfo=UTC).timestamp() * 1000)
    assert windows[1][1] == end_ms


def test_fetch_symbol_candles_tail_delta_only_passes_history_chunk_callback() -> None:
    captured: dict[str, object] = {}

    def _history_fetcher(**kwargs: object) -> list[SpotCandle]:
        captured.update(kwargs)
        return []

    candles = fetch_symbol_candles(
        exchange="deribit",
        market="perp",
        symbol="ETH",
        timeframe="1m",
        lake_root="lake/bronze",
        symbol_normalizer=lambda **kwargs: "ETH-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000),
        history_fetcher=_history_fetcher,
        range_fetcher=lambda **kwargs: [],
        latest_open_time_reader=lambda **kwargs: None,
        tail_delta_only=True,
        on_history_chunk=lambda rows: None,
    )

    assert candles == []
    assert "on_history_chunk" in captured
    assert callable(captured["on_history_chunk"])


def test_fetch_symbol_trades_full_gap_fill_respects_start_bound_for_existing_symbol() -> None:
    earliest_existing = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    start_bound = datetime(2022, 4, 29, 0, 1, tzinfo=UTC)
    end_open_time = datetime(2022, 4, 29, 0, 1, 30, tzinfo=UTC)
    start_bound_ms = int(start_bound.timestamp() * 1000)
    end_open_ms = int(end_open_time.timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[TradeTick]:
        start_open_ms = int(cast(Any, kwargs["start_open_ms"]))
        gap_end_ms = int(cast(Any, kwargs["end_open_ms"]))
        calls.append((start_open_ms, gap_end_ms))
        return [
            TradeTick(
                exchange="deribit",
                symbol="BTC",
                instrument_type="perp",
                trade_id="b",
                trade_time=datetime(2022, 4, 29, 0, 1, 1, tzinfo=UTC),
                price=100.0,
                quantity=1.0,
                side="buy",
                is_maker=False,
                source_endpoint="public_trades",
            ),
            TradeTick(
                exchange="deribit",
                symbol="BTC",
                instrument_type="perp",
                trade_id="a",
                trade_time=datetime(2022, 4, 29, 0, 1, 0, tzinfo=UTC),
                price=99.0,
                quantity=1.0,
                side="sell",
                is_maker=False,
                source_endpoint="public_trades",
            ),
            TradeTick(
                exchange="deribit",
                symbol="BTC",
                instrument_type="perp",
                trade_id="a",
                trade_time=datetime(2022, 4, 29, 0, 1, 0, tzinfo=UTC),
                price=99.0,
                quantity=1.0,
                side="sell",
                is_maker=False,
                source_endpoint="public_trades",
            ),
        ]

    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [earliest_existing],
        symbol_normalizer=lambda **kwargs: "BTC",
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [(row.trade_time, row.trade_id) for row in rows] == [
        (datetime(2022, 4, 29, 0, 1, 0, tzinfo=UTC), "a"),
        (datetime(2022, 4, 29, 0, 1, 1, tzinfo=UTC), "b"),
    ]


def test_fetch_symbol_trades_full_gap_fill_fetches_internal_and_tail_gaps() -> None:
    first_existing = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    second_existing = datetime(2022, 4, 29, 0, 2, tzinfo=UTC)
    end_open = datetime(2022, 4, 29, 0, 3, tzinfo=UTC)
    end_open_ms = int(end_open.timestamp() * 1000)
    gap_one_ms = int(datetime(2022, 4, 29, 0, 1, tzinfo=UTC).timestamp() * 1000)
    gap_tail_ms = int(datetime(2022, 4, 29, 0, 3, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[TradeTick]:
        start_open_ms = int(cast(Any, kwargs["start_open_ms"]))
        end_ms = int(cast(Any, kwargs["end_open_ms"]))
        calls.append((start_open_ms, end_ms))
        return []

    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [first_existing, second_existing],
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when open_times exist"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
    )

    assert rows == []
    assert set(calls) == {(gap_one_ms, gap_one_ms), (gap_tail_ms, gap_tail_ms)}


def test_fetch_symbol_trades_bootstrap_with_start_bound_uses_day_range_fetch() -> None:
    start_bound = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)
    end_open_time = datetime(2022, 4, 29, 0, 1, 30, tzinfo=UTC)
    start_bound_ms = int(start_bound.timestamp() * 1000)
    end_open_ms = int(end_open_time.timestamp() * 1000)
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[TradeTick]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [
            TradeTick(
                exchange="deribit",
                symbol="BTC",
                instrument_type="perp",
                trade_id="a",
                trade_time=datetime(2022, 4, 29, 0, 0, 10, tzinfo=UTC),
                price=100.0,
                quantity=1.0,
                side="buy",
                is_maker=False,
                source_endpoint="public_trades",
            )
        ]

    rows = fetch_symbol_trades(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [],
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [(row.trade_time, row.trade_id) for row in rows] == [(datetime(2022, 4, 29, 0, 0, 10, tzinfo=UTC), "a")]


def test_fetch_symbol_candles_bootstrap_with_start_bound_uses_day_range_fetch() -> None:
    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2022, 4, 29, 0, 1, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    candle = SpotCandle(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2022, 4, 29, 0, 0, tzinfo=UTC),
        close_time=datetime(2022, 4, 29, 0, 0, 59, 999000, tzinfo=UTC),
        open_price=1.0,
        high_price=1.0,
        low_price=1.0,
        close_price=1.0,
        volume=1.0,
        quote_volume=1.0,
        trade_count=1,
    )

    def _range_fetcher(**kwargs: object) -> list[SpotCandle]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [candle]

    rows = fetch_symbol_candles(
        exchange="deribit",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [],
        symbol_normalizer=lambda **kwargs: "BTCUSDT",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in rows] == [candle.open_time]


def test_fetch_symbol_open_interest_bootstrap_with_start_bound_uses_day_range_fetch() -> None:
    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2022, 4, 29, 0, 1, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2022, 4, 29, 0, 0, tzinfo=UTC),
        close_time=datetime(2022, 4, 29, 0, 0, 59, 999000, tzinfo=UTC),
        open_interest=123.0,
        open_interest_value=456.0,
    )

    def _range_fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [],
        timeframe_normalizer=lambda **kwargs: "1m",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in rows] == [point.open_time]


def test_fetch_symbol_funding_bootstrap_with_start_bound_uses_day_range_fetch() -> None:
    start_bound_ms = int(datetime(2022, 4, 29, 0, 0, tzinfo=UTC).timestamp() * 1000)
    end_open_ms = int(datetime(2022, 4, 29, 8, 0, tzinfo=UTC).timestamp() * 1000)
    calls: list[tuple[int, int]] = []
    point = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2022, 4, 29, 0, 0, tzinfo=UTC),
        close_time=datetime(2022, 4, 29, 7, 59, 59, tzinfo=UTC),
        funding_rate=0.001,
        index_price=1.0,
        mark_price=1.0,
    )

    def _range_fetcher(**kwargs: object) -> list[FundingPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        open_times_reader=lambda **kwargs: [],
        timeframe_normalizer=lambda **kwargs: "8h",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **kwargs: end_open_ms,
        history_fetcher=lambda **kwargs: pytest.fail("history_fetcher should not be called when start bound is set"),
        range_fetcher=_range_fetcher,
        tail_delta_only=False,
        start_open_ms_bound=start_bound_ms,
    )

    assert calls == [(start_bound_ms, end_open_ms)]
    assert [item.open_time for item in rows] == [point.open_time]


def test_fetch_candle_tasks_parallel_records_rows_for_slow_fetcher(monkeypatch: pytest.MonkeyPatch) -> None:
    task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m")

    def _slow_fetcher(exchange: str, market: str, symbol: str, timeframe: str, lake_root: str) -> list[SpotCandle]:
        del exchange, market, symbol, timeframe, lake_root
        import time

        time.sleep(0.05)
        return []

    result = fetch_candle_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=_slow_fetcher,
    )

    key = (task.exchange, task.market, task.symbol, task.timeframe)
    assert key in result.rows
    assert key not in result.errors


def test_fetch_candle_tasks_parallel_emits_heartbeat_without_timeout(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.delenv("DEPTH_FETCH_TASK_TIMEOUT_S", raising=False)
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "0.1")
    task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m")

    def _slow_fetcher(exchange: str, market: str, symbol: str, timeframe: str, lake_root: str) -> list[SpotCandle]:
        del exchange, market, symbol, timeframe, lake_root
        import time

        time.sleep(0.25)
        return []

    with caplog.at_level(logging.INFO):
        fetch_candle_tasks_parallel(
            tasks=[task],
            lake_root="lake/bronze",
            concurrency=1,
            logger=logging.getLogger("test"),
            symbol_fetcher=_slow_fetcher,
        )

    assert any("Fetch heartbeat type=ohlcv" in message for message in caplog.messages)


def test_task_timeout_seconds_uses_safe_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEPTH_FETCH_TASK_TIMEOUT_S", raising=False)
    timeout_s = _task_timeout_seconds()
    assert timeout_s is None


def test_heartbeat_seconds_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEPTH_FETCH_HEARTBEAT_S", raising=False)
    assert _heartbeat_seconds() == 30.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "x")
    assert _heartbeat_seconds() == 30.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "0")
    assert _heartbeat_seconds() == 30.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "5")
    assert _heartbeat_seconds() == 5.0


def test_ranges_in_random_order_single_and_multi() -> None:
    assert _ranges_in_random_order([(1, 2)]) == [(1, 2)]
    values = _ranges_in_random_order([(1, 2), (3, 4), (5, 6)])
    assert values == [(1, 2), (3, 4), (5, 6)]


def test_filter_rows_and_chunk_callback_by_start_bound() -> None:
    start_bound_ms = int(datetime(2026, 4, 27, 10, 1, tzinfo=UTC).timestamp() * 1000)
    row_old = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=1.0,
        open_interest_value=1.0,
    )
    row_new = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 1, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 1, 59, 999000, tzinfo=UTC),
        open_interest=2.0,
        open_interest_value=2.0,
    )
    assert _filter_rows_by_start_bound([row_old, row_new], start_bound_ms) == [row_new]
    seen: list[int] = []
    cb = _filter_chunk_callback(lambda rows: seen.append(len(rows)), start_bound_ms)
    assert cb is not None
    cb([row_old, row_new])
    assert seen == [1]


def test_run_with_optional_timeout_falls_back_when_process_start_eio(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

    class _FakeProcess:
        def start(self) -> None:
            raise OSError(5, "Input/output error")

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_service_module.mp, "get_context", lambda _: _FakeContext())
    value = _run_with_optional_timeout(
        lambda **_: "ok",
        timeout_s=1.0,
        heartbeat_s=0.1,
        heartbeat=lambda _elapsed_s: None,
        use_process_timeout=True,
    )
    assert value == "ok"


def test_fetch_open_interest_tasks_parallel_times_out_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "0.05")
    task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")

    result = fetch_open_interest_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=_sleep_then_empty_oi,
    )

    key = (task.exchange, task.symbol, task.timeframe)
    assert key in result.errors
    assert "timed out" in result.errors[key]


def test_fetch_open_interest_tasks_parallel_wires_task_chunk_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=2000.0,
    )
    seen: list[tuple[str, str, int]] = []

    def _fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        cb = cast(Any, kwargs["on_history_chunk"])
        cb([point])
        return [point]

    def _on_chunk(task_dto: OpenInterestFetchTaskDTO, rows: list[OpenInterestPoint]) -> None:
        seen.append((task_dto.exchange, task_dto.symbol, len(rows)))

    def _run_inline(
        fn: Any,
        *,
        timeout_s: float | None,
        heartbeat_s: float,
        heartbeat: Any,
        use_process_timeout: bool = False,
        **kwargs: object,
    ) -> Any:
        del timeout_s, heartbeat_s, heartbeat, use_process_timeout
        return fn(**kwargs)

    monkeypatch.setattr(fetch_service_module, "_run_with_optional_timeout", _run_inline)

    result = fetch_open_interest_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_chunk=_on_chunk,
    )

    assert seen == [("deribit", "BTCUSDT", 1)]
    assert (task.exchange, task.symbol, task.timeframe) in result.rows


def test_fetch_funding_tasks_parallel_emits_task_chunks() -> None:
    task = FundingFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="8h")
    seen: list[tuple[str, str, int]] = []

    def _fetcher(**kwargs: object) -> list[object]:
        cb = cast(Any, kwargs["on_history_chunk"])
        cb([object()])
        return [object()]

    def _on_chunk(task_dto: FundingFetchTaskDTO, rows: list[object]) -> None:
        seen.append((task_dto.exchange, task_dto.symbol, len(rows)))

    result = fetch_funding_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_chunk=cast(Any, _on_chunk),
    )

    assert seen == [("deribit", "BTCUSDT", 1)]
    assert (task.exchange, task.symbol, task.timeframe) in result.rows


@pytest.mark.parametrize(
    ("exc_name", "exc_type"),
    [
        ("TypeError", TypeError),
        ("ValueError", ValueError),
        ("TimeoutError", TimeoutError),
    ],
)
def test_run_with_optional_timeout_maps_worker_error_types(
    monkeypatch: pytest.MonkeyPatch, exc_name: str, exc_type: type[Exception]
) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

        def empty(self) -> bool:
            return False

        def get_nowait(self) -> tuple[str, tuple[str, str]]:
            return ("err", (exc_name, "boom"))

    class _FakeProcess:
        def start(self) -> None:
            return None

        def join(self, timeout: float | None = None) -> None:
            del timeout
            return None

        def is_alive(self) -> bool:
            return False

        def terminate(self) -> None:
            return None

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_service_module.mp, "get_context", lambda _: _FakeContext())

    with pytest.raises(exc_type, match="boom"):
        _run_with_optional_timeout(
            lambda **_: "ok",
            timeout_s=1.0,
            heartbeat_s=0.1,
            heartbeat=lambda _elapsed_s: None,
            use_process_timeout=True,
        )


def test_run_with_optional_timeout_raises_when_worker_exits_without_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeQueue:
        def close(self) -> None:
            return None

        def join_thread(self) -> None:
            return None

        def empty(self) -> bool:
            return True

    class _FakeProcess:
        exitcode = 0

        def start(self) -> None:
            return None

        def join(self, timeout: float | None = None) -> None:
            del timeout
            return None

        def is_alive(self) -> bool:
            return False

        def terminate(self) -> None:
            return None

    class _FakeContext:
        def Queue(self, maxsize: int = 1) -> _FakeQueue:  # noqa: N802
            del maxsize
            return _FakeQueue()

        def Process(self, target: object, args: tuple[object, ...]) -> _FakeProcess:  # noqa: N802
            del target, args
            return _FakeProcess()

    monkeypatch.setattr(fetch_service_module.mp, "get_context", lambda _: _FakeContext())

    with pytest.raises(RuntimeError, match="exited without result"):
        _run_with_optional_timeout(
            lambda **_: "ok",
            timeout_s=1.0,
            heartbeat_s=0.1,
            heartbeat=lambda _elapsed_s: None,
            use_process_timeout=True,
        )


def test_fetch_funding_tasks_parallel_retries_without_history_chunk_when_unsupported() -> None:
    task = FundingFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="8h")
    called_with_chunk: list[bool] = []

    def _fetcher(**kwargs: object) -> list[object]:
        has_chunk = "on_history_chunk" in kwargs
        called_with_chunk.append(has_chunk)
        if has_chunk:
            raise TypeError("unexpected keyword argument 'on_history_chunk'")
        return [object()]

    result = fetch_funding_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_chunk=lambda _task, _rows: None,
    )

    key = (task.exchange, task.symbol, task.timeframe)
    assert key in result.rows
    assert key not in result.errors
    assert called_with_chunk == [True, False]


def test_fetch_open_interest_tasks_parallel_retries_without_history_chunk_when_unsupported() -> None:
    task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")
    called_with_chunk: list[bool] = []

    def _fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        has_chunk = "on_history_chunk" in kwargs
        called_with_chunk.append(has_chunk)
        if has_chunk:
            raise TypeError("unexpected keyword argument 'on_history_chunk'")
        return []

    result = fetch_open_interest_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=_fetcher,
        on_task_chunk=lambda _task, _rows: None,
    )

    key = (task.exchange, task.symbol, task.timeframe)
    assert key in result.rows
    assert key not in result.errors
    assert called_with_chunk == [True, False]


def test_fetch_trade_tasks_parallel_records_errors() -> None:
    task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")

    def _failing_fetcher(**kwargs: object) -> list[TradeTick]:
        del kwargs
        raise RuntimeError("trade boom")

    result = fetch_trade_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _failing_fetcher),
    )

    key = (task.exchange, task.market, task.symbol)
    assert key in result.errors
    assert "trade boom" in result.errors[key]


def test_fetch_trade_tasks_parallel_classifies_network_unreachable_errors() -> None:
    task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")

    def _failing_fetcher(**kwargs: object) -> list[TradeTick]:
        del kwargs
        raise RuntimeError("Connection error for x: [Errno 113] No route to host")

    result = fetch_trade_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _failing_fetcher),
    )

    key = (task.exchange, task.market, task.symbol)
    assert key in result.errors
    assert result.errors[key].startswith("[NET_UNREACHABLE]")


def test_fetch_open_interest_tasks_parallel_mixed_results_and_on_task_complete() -> None:
    task_ok = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")
    task_fail = OpenInterestFetchTaskDTO(exchange="deribit", symbol="ETHUSDT", timeframe="1m")
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=2000.0,
    )
    completed: list[tuple[str, int]] = []

    def _fetcher(**kwargs: object) -> list[OpenInterestPoint]:
        symbol = cast(str, kwargs["symbol"])
        if symbol == "ETHUSDT":
            raise RuntimeError("oi boom")
        return [point]

    def _on_complete(task: OpenInterestFetchTaskDTO, rows: list[OpenInterestPoint]) -> None:
        completed.append((task.symbol, len(rows)))

    result = fetch_open_interest_tasks_parallel(
        tasks=[task_ok, task_fail],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=_fetcher,
        on_task_complete=_on_complete,
    )

    ok_key = (task_ok.exchange, task_ok.symbol, task_ok.timeframe)
    fail_key = (task_fail.exchange, task_fail.symbol, task_fail.timeframe)
    assert ok_key in result.rows
    assert fail_key in result.errors
    assert completed == [("BTCUSDT", 1)]


def test_fetch_funding_tasks_parallel_mixed_results_and_on_task_complete() -> None:
    task_ok = FundingFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="8h")
    task_fail = FundingFetchTaskDTO(exchange="deribit", symbol="ETHUSDT", timeframe="8h")
    completed: list[tuple[str, int]] = []

    def _fetcher(**kwargs: object) -> list[object]:
        symbol = cast(str, kwargs["symbol"])
        if symbol == "ETHUSDT":
            raise RuntimeError("funding boom")
        return [object(), object()]

    def _on_complete(task: FundingFetchTaskDTO, rows: list[object]) -> None:
        completed.append((task.symbol, len(rows)))

    result = fetch_funding_tasks_parallel(
        tasks=[task_ok, task_fail],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_complete=cast(Any, _on_complete),
    )

    ok_key = (task_ok.exchange, task_ok.symbol, task_ok.timeframe)
    fail_key = (task_fail.exchange, task_fail.symbol, task_fail.timeframe)
    assert ok_key in result.rows
    assert fail_key in result.errors
    assert completed == [("BTCUSDT", 2)]


def test_fetch_open_interest_tasks_parallel_records_on_task_complete_errors() -> None:
    task = OpenInterestFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="1m")
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTCUSDT",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 0, 59, 999000, tzinfo=UTC),
        open_interest=1000.0,
        open_interest_value=2000.0,
    )

    result = fetch_open_interest_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=lambda **kwargs: [point],
        on_task_complete=lambda _task, _rows: (_ for _ in ()).throw(RuntimeError("complete boom")),
    )

    key = (task.exchange, task.symbol, task.timeframe)
    assert key in result.errors
    assert "complete boom" in result.errors[key]


def test_fetch_funding_tasks_parallel_records_on_task_complete_errors() -> None:
    task = FundingFetchTaskDTO(exchange="deribit", symbol="BTCUSDT", timeframe="8h")

    result = fetch_funding_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, (lambda **kwargs: [object()])),
        on_task_complete=cast(Any, (lambda _task, _rows: (_ for _ in ()).throw(RuntimeError("funding complete boom")))),
    )

    key = (task.exchange, task.symbol, task.timeframe)
    assert key in result.errors
    assert "funding complete boom" in result.errors[key]


def test_fetch_candle_tasks_parallel_retries_without_history_chunk_when_unsupported() -> None:
    task = CandleFetchTaskDTO(exchange="deribit", market="spot", symbol="BTCUSDT", timeframe="1m")
    seen_chunk_kwarg: list[bool] = []

    def _fetcher(
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        lake_root: str,
        on_history_chunk: Any | None = None,
    ) -> list[SpotCandle]:
        del exchange, market, symbol, timeframe, lake_root
        has_chunk = on_history_chunk is not None
        seen_chunk_kwarg.append(has_chunk)
        if has_chunk:
            raise TypeError("unexpected keyword argument 'on_history_chunk'")
        return []

    result = fetch_candle_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_chunk=lambda _task, _rows: None,
    )

    key = (task.exchange, task.market, task.symbol, task.timeframe)
    assert key in result.rows
    assert key not in result.errors
    assert seen_chunk_kwarg == [True, False]


def test_fetch_symbol_open_interest_non_perp_returns_empty() -> None:
    assert (
        fetch_symbol_open_interest(
            exchange="deribit",
            market="spot",
            symbol="BTC",
            timeframe="1m",
            lake_root="lake/bronze",
        )
        == []
    )


def test_fetch_symbol_open_interest_tail_requires_latest_reader() -> None:
    with pytest.raises(ValueError, match="latest_open_time_reader is required"):
        fetch_symbol_open_interest(
            exchange="deribit",
            market="perp",
            symbol="BTC",
            timeframe="1m",
            lake_root="lake/bronze",
            tail_delta_only=True,
            latest_open_time_reader=None,
        )


def test_fetch_symbol_open_interest_tail_latest_none_uses_history() -> None:
    point = OpenInterestPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="1m",
        open_time=datetime(2026, 4, 27, 10, 2, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 10, 2, 59, 999000, tzinfo=UTC),
        open_interest=10.0,
        open_interest_value=20.0,
    )
    rows = fetch_symbol_open_interest(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="1m",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "1m",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 60_000,
        now_open_resolver=lambda **kwargs: int(datetime(2026, 4, 27, 10, 5, tzinfo=UTC).timestamp() * 1000),
        latest_open_time_reader=lambda **kwargs: None,
        history_fetcher=lambda **kwargs: [point],
        range_fetcher=lambda **kwargs: [],
        tail_delta_only=True,
    )
    assert [item.open_time for item in rows] == [point.open_time]


def test_fetch_symbol_funding_non_perp_and_tail_requires_reader() -> None:
    assert (
        fetch_symbol_funding(
            exchange="deribit",
            market="spot",
            symbol="BTC",
            timeframe="8h",
            lake_root="lake/bronze",
        )
        == []
    )
    with pytest.raises(ValueError, match="latest_open_time_reader is required"):
        fetch_symbol_funding(
            exchange="deribit",
            market="perp",
            symbol="BTC",
            timeframe="8h",
            lake_root="lake/bronze",
            tail_delta_only=True,
            latest_open_time_reader=None,
        )


def test_fetch_symbol_funding_tail_and_gapfill_paths() -> None:
    point = FundingPoint(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        interval="8h",
        open_time=datetime(2026, 4, 27, 8, 0, tzinfo=UTC),
        close_time=datetime(2026, 4, 27, 15, 59, 59, tzinfo=UTC),
        funding_rate=0.001,
        index_price=1.0,
        mark_price=1.0,
    )
    calls: list[tuple[int, int]] = []

    def _range_fetcher(**kwargs: object) -> list[FundingPoint]:
        calls.append((int(cast(Any, kwargs["start_open_ms"])), int(cast(Any, kwargs["end_open_ms"]))))
        return [point]

    rows = fetch_symbol_funding(
        exchange="deribit",
        market="perp",
        symbol="BTC",
        timeframe="8h",
        lake_root="lake/bronze",
        timeframe_normalizer=lambda **kwargs: "8h",
        symbol_normalizer=lambda **kwargs: "BTC-PERPETUAL",
        interval_ms_resolver=lambda **kwargs: 8 * 60 * 60 * 1000,
        now_open_resolver=lambda **kwargs: int(datetime(2026, 4, 27, 16, 0, tzinfo=UTC).timestamp() * 1000),
        latest_open_time_reader=lambda **kwargs: datetime(2026, 4, 27, 0, 0, tzinfo=UTC),
        history_fetcher=lambda **kwargs: [],
        range_fetcher=_range_fetcher,
        tail_delta_only=True,
    )
    assert rows
    assert calls


def test_fetch_trade_tasks_parallel_emits_task_chunks() -> None:
    task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")
    seen: list[tuple[str, str, int]] = []

    tick = TradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="perp",
        trade_id="a",
        trade_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )

    def _fetcher(**kwargs: object) -> list[TradeTick]:
        cb = cast(Any, kwargs["on_history_chunk"])
        cb([tick])
        return [tick]

    def _on_chunk(task_dto: TradeFetchTaskDTO, rows: list[TradeTick]) -> None:
        seen.append((task_dto.exchange, task_dto.symbol, len(rows)))

    result = fetch_trade_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, _fetcher),
        on_task_chunk=cast(Any, _on_chunk),
    )

    assert seen == [("deribit", "BTC", 1)]
    assert (task.exchange, task.market, task.symbol) in result.rows


def test_fetch_trade_tasks_parallel_records_on_task_complete_errors() -> None:
    task = TradeFetchTaskDTO(exchange="deribit", market="perp", symbol="BTC")
    tick = TradeTick(
        exchange="deribit",
        symbol="BTC",
        instrument_type="perp",
        trade_id="a",
        trade_time=datetime(2026, 4, 27, 10, 0, tzinfo=UTC),
        price=100.0,
        quantity=1.0,
        side="buy",
        is_maker=False,
        source_endpoint="public_trades",
    )

    result = fetch_trade_tasks_parallel(
        tasks=[task],
        lake_root="lake/bronze",
        concurrency=1,
        logger=logging.getLogger("test"),
        symbol_fetcher=cast(Any, (lambda **kwargs: [tick])),
        on_task_complete=cast(Any, (lambda _task, _rows: (_ for _ in ()).throw(RuntimeError("trade complete boom")))),
    )

    key = (task.exchange, task.market, task.symbol)
    assert key in result.errors
    assert "trade complete boom" in result.errors[key]
