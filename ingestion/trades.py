"""Tick-trade ingestion interface (Deribit-only)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, cast

from ingestion.exchanges import deribit_option_trades
from ingestion.exchanges import deribit_trades
from ingestion.spot import Exchange, normalize_storage_symbol

TradeMarket = Literal["spot", "perp", "option"]
OptionType = Literal["call", "put", "unknown"]


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


@dataclass(frozen=True)
class OptionTradeTick:
    """Historical option trade tick."""

    exchange: str
    symbol: str
    instrument_type: Literal["option"]
    instrument_name: str
    expiry: str
    strike: float
    option_type: OptionType
    trade_id: str
    trade_time: datetime
    price: float
    quantity: float
    side: str
    is_maker: bool
    source_endpoint: str


def _canonical_underlying_symbol(symbol: str) -> str:
    upper = symbol.upper().strip()
    if not upper:
        raise ValueError("symbol cannot be empty")
    if upper.endswith("-PERPETUAL"):
        return upper.split("-", 1)[0]
    if "_" in upper:
        return upper.split("_", 1)[0]
    if upper.endswith("USDC"):
        return upper[:-4]
    if upper.endswith("USDT"):
        return upper[:-4]
    if upper.endswith("USD"):
        return upper[:-3]
    return upper


def _normalize_trade_symbol(exchange: Exchange, symbol: str, market: TradeMarket) -> str:
    if market == "option":
        return _canonical_underlying_symbol(symbol)
    return normalize_storage_symbol(exchange=exchange, symbol=symbol, market=cast(Literal["spot", "perp"], market))


def _parse_trade_row(exchange: Exchange, symbol: str, market: TradeMarket, row: dict[str, object]) -> TradeTick:
    ts_ms = int(cast(Any, row).get("timestamp", 0))
    trade_id = str(cast(Any, row).get("trade_id", ""))
    side = _parse_side(row)
    is_maker = _parse_is_maker(row)
    return TradeTick(
        exchange=exchange,
        symbol=normalize_storage_symbol(exchange=exchange, symbol=symbol, market=market),
        instrument_type=market,
        trade_id=trade_id,
        trade_time=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
        price=float(cast(Any, row).get("price", 0.0)),
        quantity=float(cast(Any, row).get("amount", 0.0)),
        side=side,
        is_maker=is_maker,
        source_endpoint="public_trades",
    )


def _parse_option_trade_row(exchange: Exchange, symbol: str, row: dict[str, object]) -> OptionTradeTick:
    ts_ms = int(cast(Any, row).get("timestamp", 0))
    trade_id = str(cast(Any, row).get("trade_id", ""))
    side = _parse_side(row)
    is_maker = _parse_is_maker(row)
    instrument_name = str(cast(Any, row).get("instrument_name", ""))
    expiry, strike, option_type = _parse_option_contract_fields(instrument_name)
    return OptionTradeTick(
        exchange=exchange,
        symbol=_canonical_underlying_symbol(symbol),
        instrument_type="option",
        instrument_name=instrument_name,
        expiry=expiry,
        strike=strike,
        option_type=option_type,
        trade_id=trade_id,
        trade_time=datetime.fromtimestamp(ts_ms / 1000, tz=UTC),
        price=float(cast(Any, row).get("price", 0.0)),
        quantity=float(cast(Any, row).get("amount", 0.0)),
        side=side,
        is_maker=is_maker,
        source_endpoint="public_option_trades",
    )


def _parse_side(row: dict[str, object]) -> str:
    """Normalize Deribit trade side field."""

    direction = str(cast(Any, row).get("direction", "")).lower()
    if direction == "buy":
        return "buy"
    if direction == "sell":
        return "sell"
    return "unknown"


def _parse_is_maker(row: dict[str, object]) -> bool:
    """Normalize Deribit maker/taker marker."""

    return str(cast(Any, row).get("liquidation", "")).lower() == "m"


def _parse_option_contract_fields(instrument_name: str) -> tuple[str, float, OptionType]:
    """Parse expiry/strike/option_type from Deribit option instrument name."""

    if not instrument_name:
        return "", 0.0, "unknown"
    parts = instrument_name.split("-")
    if len(parts) < 4:
        return "", 0.0, "unknown"
    expiry = parts[1]
    try:
        strike = float(parts[2])
    except ValueError:
        strike = 0.0
    suffix = parts[3].upper()
    option_type: OptionType = "unknown"
    if suffix == "C":
        option_type = "call"
    elif suffix == "P":
        option_type = "put"
    return expiry, strike, option_type


def fetch_trades_all_history(
    exchange: Exchange,
    symbol: str,
    market: TradeMarket,
    on_history_chunk: Callable[[list[TradeTick | OptionTradeTick]], None] | None = None,
) -> list[TradeTick | OptionTradeTick]:
    """Fetch all available historical trades."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = _normalize_trade_symbol(exchange=exchange, symbol=symbol, market=market)
    if market == "option":
        rows = deribit_option_trades.fetch_option_trades_all(currency=normalized_symbol)
        parsed = [_parse_option_trade_row(exchange, normalized_symbol, row) for row in rows]
        if on_history_chunk is not None and parsed:
            on_history_chunk(parsed)
            return []
        return parsed

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
) -> list[TradeTick | OptionTradeTick]:
    """Fetch historical trades by inclusive range."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = _normalize_trade_symbol(exchange=exchange, symbol=symbol, market=market)
    if market == "option":
        rows = deribit_option_trades.fetch_option_trades_range(
            currency=normalized_symbol,
            start_open_ms=start_open_ms,
            end_open_ms=end_open_ms,
            count=page_size,
        )
        return [_parse_option_trade_row(exchange, normalized_symbol, row) for row in rows]
    rows = deribit_trades.fetch_trades_range(
        symbol=normalized_symbol,
        market=market,
        start_open_ms=start_open_ms,
        end_open_ms=end_open_ms,
        count=page_size,
    )
    return [_parse_trade_row(exchange, normalized_symbol, market, row) for row in rows]
