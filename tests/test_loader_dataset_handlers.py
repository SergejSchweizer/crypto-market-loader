"""Tests for dataset-specific loader helpers."""

from __future__ import annotations

from api.commands.loader_dataset_handlers import build_trade_tasks


def test_build_trade_tasks_uses_only_perp_market() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        randomized_symbols=["BTCUSDT", "ETHUSDT"],
        ohlcv_markets=["spot", "perp"],
        trades_requested=True,
    )

    assert tasks == [
        ("deribit", "perp", "BTCUSDT"),
        ("deribit", "perp", "ETHUSDT"),
    ]


def test_build_trade_tasks_returns_empty_when_not_requested() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        randomized_symbols=["BTCUSDT"],
        ohlcv_markets=["spot"],
        trades_requested=False,
    )

    assert tasks == []
