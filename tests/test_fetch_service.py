"""Tests for fetch orchestration service."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, cast

import pytest

import application.services.fetch_service as fetch_service_module
from application.dto import CandleFetchTaskDTO, FundingFetchTaskDTO, OpenInterestFetchTaskDTO
from application.services.fetch_service import (
    _run_with_optional_timeout,
    _split_range_into_utc_days,
    _task_timeout_seconds,
    fetch_candle_tasks_parallel,
    fetch_funding_tasks_parallel,
    fetch_open_interest_tasks_parallel,
    fetch_symbol_candles,
    fetch_symbol_trades,
)
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
    pre_start = datetime(2022, 4, 28, 23, 59, tzinfo=UTC)
    on_start = datetime(2022, 4, 29, 0, 0, tzinfo=UTC)

    def _history_fetcher(**kwargs: object) -> list[SpotCandle]:
        del kwargs
        return [
            SpotCandle(
                exchange="deribit",
                symbol="BTCUSDT",
                interval="1m",
                open_time=pre_start,
                close_time=pre_start,
                open_price=1.0,
                high_price=1.0,
                low_price=1.0,
                close_price=1.0,
                volume=1.0,
                quote_volume=1.0,
                trade_count=1,
            ),
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
            ),
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
        history_fetcher=_history_fetcher,
        range_fetcher=lambda **kwargs: pytest.fail("range_fetcher should not be called in bootstrap path"),
        latest_open_time_reader=lambda **kwargs: None,
        tail_delta_only=True,
        start_open_ms_bound=start_bound_ms,
    )

    assert [item.open_time for item in candles] == [on_start]


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
                instrument_type="trades",
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
                instrument_type="trades",
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
                instrument_type="trades",
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
        market="trades",
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
    assert [(row.trade_time, row.trade_id) for row in rows] == [
        (datetime(2022, 4, 29, 0, 0, 10, tzinfo=UTC), "a")
    ]


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
