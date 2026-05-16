"""Tick-trade ingestion interface (Deribit-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from ingestion.exchanges import deribit_trades
from ingestion.spot import Exchange, normalize_storage_symbol

TradeMarket = Literal["spot", "perp"]


@dataclass(frozen=True)
class TradeTick:
    """Historical trade tick."""

    exchange: str
    symbol: str
    instrument_type: TradeMarket
    trade_id: str
    trade_time: datetime
    price: float
    quantity: float
    side: str
    is_maker: bool
    source_endpoint: str


def _parse_trade_row(exchange: Exchange, symbol: str, market: TradeMarket, row: dict[str, object]) -> TradeTick:
    ts_ms = int(cast(Any, row).get("timestamp", 0))
    trade_id = str(cast(Any, row).get("trade_id", ""))
    direction = str(cast(Any, row).get("direction", "")).lower()
    side = "buy" if direction == "buy" else "sell" if direction == "sell" else "unknown"
    liquidity = str(cast(Any, row).get("liquidation", "")).lower()
    return TradeTick(
        exchange=exchange,
        symbol=normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market),
        instrument_type=market,
        trade_id=trade_id,
        trade_time=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
        price=float(cast(Any, row).get("price", 0.0)),
        quantity=float(cast(Any, row).get("amount", 0.0)),
        side=side,
        is_maker=liquidity == "m",
        source_endpoint="public_trades",
    )


def fetch_trades_all_history(
    exchange: Exchange,
    symbol: str,
    market: TradeMarket,
    on_history_chunk: Callable[[list[TradeTick]], None] | None = None,
) -> list[TradeTick]:
    """Fetch all available historical trades."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)

    def _on_page(rows: list[dict[str, object]]) -> None:
        if on_history_chunk is None:
            return
        on_history_chunk([_parse_trade_row(exchange, normalized_symbol, market, row) for row in rows])

    rows = deribit_trades.fetch_trades_all(
        symbol=normalized_symbol,
        market=market,
        on_page=_on_page if on_history_chunk is not None else None,
    )
    if on_history_chunk is not None:
        return []
    return [_parse_trade_row(exchange, normalized_symbol, market, row) for row in rows]


def fetch_trades_range(
    exchange: Exchange,
    symbol: str,
    market: TradeMarket,
    start_open_ms: int,
    end_open_ms: int,
    page_size: int = deribit_trades.DERIBIT_TRADES_DEFAULT_PAGE_SIZE,
) -> list[TradeTick]:
    """Fetch historical trades by inclusive range."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market)
    rows = deribit_trades.fetch_trades_range(
        symbol=normalized_symbol,
        market=market,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
        count=page_size,
    )
    return [_parse_trade_row(exchange, normalized_symbol, market, row) for row in rows]
