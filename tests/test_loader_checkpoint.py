"""Tests for loader checkpoint helper utilities."""

from __future__ import annotations

from api.commands.loader_checkpoint import apply_checkpoint_filter, has_checkpoint_state


def _serialize(parts: tuple[object, ...]) -> str:
    return "|".join(str(item) for item in parts)


def test_apply_checkpoint_filter_drops_completed_tasks() -> None:
    candle_tasks = [("deribit", "spot", "BTC", "1m"), ("deribit", "perp", "ETH", "1m")]
    oi_tasks = [("deribit", "BTC", "1m")]
    funding_tasks = [("deribit", "ETH", "1m")]
    trade_tasks = [("deribit", "perp", "BTC"), ("deribit", "option", "ETH")]
    completed = {
        "candle": {_serialize(("deribit", "spot", "BTC", "1m"))},
        "oi": set(),
        "funding": {_serialize(("deribit", "ETH", "1m"))},
        "trade": {_serialize(("deribit", "option", "ETH"))},
    }

    pending = apply_checkpoint_filter(
        candle_tasks=candle_tasks,
        oi_tasks=oi_tasks,
        funding_tasks=funding_tasks,
        trade_tasks=trade_tasks,
        completed=completed,
        key_serializer=_serialize,
    )
    assert pending.candle_tasks == [("deribit", "perp", "ETH", "1m")]
    assert pending.oi_tasks == [("deribit", "BTC", "1m")]
    assert pending.funding_tasks == []
    assert pending.trade_tasks == [("deribit", "perp", "BTC")]


def test_has_checkpoint_state_detects_any_completed_bucket() -> None:
    empty = {"candle": set(), "oi": set(), "funding": set(), "trade": set()}
    assert not has_checkpoint_state(empty)
    non_empty = {"candle": {"x"}, "oi": set(), "funding": set(), "trade": set()}
    assert has_checkpoint_state(non_empty)
