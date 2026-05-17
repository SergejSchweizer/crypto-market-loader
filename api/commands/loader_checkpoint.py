"""Checkpoint filtering helpers for bronze loader resume behavior."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeAlias

from ingestion.spot import Exchange, Market
from ingestion.trades import TradeMarket

CandleTask: TypeAlias = tuple[Exchange, Market, str, str]
OpenInterestTask: TypeAlias = tuple[Exchange, str, str]
FundingTask: TypeAlias = tuple[Exchange, str, str]
TradeTask: TypeAlias = tuple[Exchange, TradeMarket, str]


@dataclass(frozen=True)
class PendingTaskGroups:
    """Pending task groups after applying completed-checkpoint filtering."""

    candle_tasks: list[CandleTask]
    oi_tasks: list[OpenInterestTask]
    funding_tasks: list[FundingTask]
    trade_tasks: list[TradeTask]


def apply_checkpoint_filter(
    *,
    candle_tasks: list[CandleTask],
    oi_tasks: list[OpenInterestTask],
    funding_tasks: list[FundingTask],
    trade_tasks: list[TradeTask],
    completed: dict[str, set[str]],
    key_serializer: Callable[[tuple[object, ...]], str],
) -> PendingTaskGroups:
    """Filter task groups against completed checkpoint sets."""

    pending_candles = [
        task for task in candle_tasks if key_serializer((task[0], task[1], task[2], task[3])) not in completed["candle"]
    ]
    pending_oi = [task for task in oi_tasks if key_serializer((task[0], task[1], task[2])) not in completed["oi"]]
    pending_funding = [
        task for task in funding_tasks if key_serializer((task[0], task[1], task[2])) not in completed["funding"]
    ]
    pending_trades = [
        task for task in trade_tasks if key_serializer((task[0], task[1], task[2])) not in completed["trade"]
    ]
    return PendingTaskGroups(
        candle_tasks=pending_candles,
        oi_tasks=pending_oi,
        funding_tasks=pending_funding,
        trade_tasks=pending_trades,
    )


def has_checkpoint_state(completed: dict[str, set[str]]) -> bool:
    """Return whether any checkpoint category has completed entries."""

    return any(completed.get(name, set()) for name in ("candle", "oi", "funding", "trade", "option_instruments"))
