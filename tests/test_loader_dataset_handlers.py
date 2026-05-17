"""Tests for dataset-specific loader helpers."""

from __future__ import annotations

from api.commands.loader_dataset_handlers import build_trade_tasks


def test_build_trade_tasks_uses_only_perp_market() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC", "ETH"],
        option_trade_symbols=["BTC", "ETH"],
        perp_trades_requested=True,
        option_trades_requested=False,
    )

    assert tasks == [
        ("deribit", "perp", "BTC"),
        ("deribit", "perp", "ETH"),
    ]


def test_build_trade_tasks_returns_empty_when_not_requested() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC"],
        option_trade_symbols=["BTC"],
        perp_trades_requested=False,
        option_trades_requested=False,
    )

    assert tasks == []


def test_build_trade_tasks_includes_option_market_when_requested() -> None:
    tasks = build_trade_tasks(
        exchanges=["deribit"],
        perp_trade_symbols=["BTC"],
        option_trade_symbols=["ETH"],
        perp_trades_requested=True,
        option_trades_requested=True,
    )
    assert tasks == [("deribit", "perp", "BTC"), ("deribit", "option", "ETH")]
