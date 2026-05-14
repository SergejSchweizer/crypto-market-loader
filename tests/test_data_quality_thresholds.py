"""Data-quality threshold checks on representative fixture data."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

pl = pytest.importorskip("polars")


def _coverage_ratio(frame, ts_col: str) -> float:
    min_ts = frame.select(pl.col(ts_col).min()).item()
    max_ts = frame.select(pl.col(ts_col).max()).item()
    if not isinstance(min_ts, datetime) or not isinstance(max_ts, datetime):
        return 0.0
    expected = int(((max_ts - min_ts).total_seconds() // 60) + 1)
    return 0.0 if expected <= 0 else float(frame.height / expected)


def test_quality_thresholds_trades_1m_fixture() -> None:
    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
            ],
            "trades_close_price": [100.0, 101.0, 102.0],
            "trades_volume": [10.0, 11.0, 12.0],
            "trades_buy_volume_share": [0.6, 0.55, 0.58],
        }
    )
    assert _coverage_ratio(frame, "timestamp_m1") >= 0.99
    assert frame["trades_close_price"].null_count() == 0
    assert frame["trades_volume"].null_count() == 0
    drift = abs(float(frame["trades_buy_volume_share"].mean()) - 0.5)
    assert drift <= 0.2


def test_quality_thresholds_gold_fixture_missing_values_bounded() -> None:
    frame = pl.DataFrame(
        {
            "timestamp_m1": [
                datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 1, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 2, tzinfo=UTC),
                datetime(2026, 5, 1, 0, 3, tzinfo=UTC),
            ],
            "spot_close_price": [1.0, 1.1, None, 1.2],
            "perp_close_price": [10.0, 10.2, 10.3, 10.4],
            "trades_trade_count": [5, 6, 4, 7],
        }
    )
    assert _coverage_ratio(frame, "timestamp_m1") >= 0.99
    missing_ratio = frame["spot_close_price"].null_count() / frame.height
    assert missing_ratio <= 0.30
