"""Dataset-specific task planning and output assembly helpers for bronze loader."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Literal, cast

from ingestion.funding import FundingPoint
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Exchange, Market, SpotCandle
from ingestion.trades import OptionTradeTick, TradeMarket, TradeTick

DataType = Literal["spot", "perp", "oi", "funding", "perp_trades", "option_trades"]


def build_trade_tasks(
    *,
    exchanges: list[Exchange],
    randomized_perp_trade_symbols: list[str],
    randomized_option_trade_symbols: list[str],
    perp_trades_requested: bool,
    option_trades_requested: bool,
) -> list[tuple[Exchange, TradeMarket, str]]:
    """Build trade task tuples for requested exchanges/symbols."""

    if not perp_trades_requested and not option_trades_requested:
        return []
    tasks: list[tuple[Exchange, TradeMarket, str]] = []
    for exchange in exchanges:
        if perp_trades_requested:
            for symbol in randomized_perp_trade_symbols:
                tasks.append((exchange, "perp", symbol))
        if option_trades_requested:
            for symbol in randomized_option_trade_symbols:
                tasks.append((exchange, "option", symbol))
    return tasks


def _trade_dataset_key(market: TradeMarket) -> str:
    """Return output dataset key for one trade market."""

    return "option_trades" if market == "option" else "trades"


def _serialize_trade_row(item: TradeTick | OptionTradeTick) -> dict[str, object]:
    """Serialize one trade row for command JSON output."""

    row: dict[str, object] = {
        "exchange": item.exchange,
        "symbol": item.symbol,
        "instrument_type": item.instrument_type,
        "trade_id": item.trade_id,
        "trade_time": item.trade_time.isoformat(),
        "price": item.price,
        "quantity": item.quantity,
        "side": item.side,
        "is_maker": item.is_maker,
    }
    if isinstance(item, OptionTradeTick):
        row["instrument_name"] = item.instrument_name
        row["expiry"] = item.expiry
        row["strike"] = item.strike
        row["option_type"] = item.option_type
    return row


def populate_ohlcv_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, Market, str, str]],
    task_results: dict[tuple[Exchange, Market, str, str], list[SpotCandle]],
    task_errors: dict[tuple[Exchange, Market, str, str], str],
    multi_market: bool,
    candle_serializer: Callable[[SpotCandle], dict[str, object]],
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
) -> None:
    """Populate JSON output and storage bucket for OHLCV tasks."""

    for exchange, market, symbol, timeframe in tasks:
        exchange_output = cast(dict[str, object], output[exchange])
        symbol_key = symbol.upper()
        result_key = (exchange, market, symbol, timeframe)
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault(market, {}))
        else:
            market_bucket = exchange_output
        if result_key in task_errors:
            market_bucket[symbol_key] = {"error": task_errors[result_key]}
            continue
        candles = task_results.get(result_key, [])
        market_bucket[symbol_key] = [candle_serializer(item) for item in candles]
        by_market = candles_for_storage.setdefault(market, {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = candles


def populate_oi_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, str, str]],
    results: dict[tuple[Exchange, str, str], list[OpenInterestPoint]],
    errors: dict[tuple[Exchange, str, str], str],
    multi_market: bool,
    storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
) -> None:
    """Populate JSON output and storage bucket for OI tasks."""

    for exchange, symbol, timeframe in tasks:
        symbol_key = symbol.upper()
        oi_key = (exchange, symbol, timeframe)
        exchange_output = cast(dict[str, object], output[exchange])
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault("oi", {}))
        else:
            market_bucket = exchange_output
        if oi_key in errors:
            market_bucket[symbol_key] = {"error": errors[oi_key]}
            continue
        rows = results.get(oi_key, [])
        market_bucket[symbol_key] = [
            {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "interval": item.interval,
                "open_time": item.open_time.isoformat(),
                "close_time": item.close_time.isoformat(),
                "open_interest": item.open_interest,
                "open_interest_value": item.open_interest_value,
            }
            for item in rows
        ]
        by_market = storage.setdefault("perp", {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows


def populate_funding_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, str, str]],
    results: dict[tuple[Exchange, str, str], list[FundingPoint]],
    errors: dict[tuple[Exchange, str, str], str],
    multi_market: bool,
    storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]],
) -> None:
    """Populate JSON output and storage bucket for funding tasks."""

    for exchange, symbol, timeframe in tasks:
        symbol_key = symbol.upper()
        funding_key = (exchange, symbol, timeframe)
        exchange_output = cast(dict[str, object], output[exchange])
        if multi_market:
            market_bucket = cast(dict[str, object], exchange_output.setdefault("funding", {}))
        else:
            market_bucket = exchange_output
        if funding_key in errors:
            market_bucket[symbol_key] = {"error": errors[funding_key]}
            continue
        rows = results.get(funding_key, [])
        market_bucket[symbol_key] = [
            {
                "exchange": item.exchange,
                "symbol": item.symbol,
                "interval": item.interval,
                "open_time": item.open_time.isoformat(),
                "close_time": item.close_time.isoformat(),
                "funding_rate": item.funding_rate,
                "index_price": item.index_price,
                "mark_price": item.mark_price,
            }
            for item in rows
        ]
        by_market = storage.setdefault("perp", {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows


def populate_trades_output(
    *,
    output: dict[str, object],
    tasks: Iterable[tuple[Exchange, TradeMarket, str]],
    results: dict[tuple[Exchange, TradeMarket, str], list[TradeTick | OptionTradeTick]],
    errors: dict[tuple[Exchange, TradeMarket, str], str],
    multi_market: bool,
    storage: dict[TradeMarket, dict[str, dict[str, list[TradeTick | OptionTradeTick]]]],
) -> None:
    """Populate JSON output and storage bucket for trade tasks."""

    for exchange, market, symbol in tasks:
        symbol_key = symbol.upper()
        trade_key = (exchange, market, symbol)
        exchange_output = cast(dict[str, object], output[exchange])
        dataset_key = _trade_dataset_key(market)
        if multi_market:
            trades_bucket = cast(dict[str, object], exchange_output.setdefault(dataset_key, {}))
        else:
            trades_bucket = exchange_output
        market_bucket = cast(dict[str, object], trades_bucket.setdefault(market, {}))
        if trade_key in errors:
            market_bucket[symbol_key] = {"error": errors[trade_key]}
            continue
        rows = results.get(trade_key, [])
        market_bucket[symbol_key] = [_serialize_trade_row(item) for item in rows]
        by_market = storage.setdefault(market, {})
        by_exchange = by_market.setdefault(exchange, {})
        by_exchange[symbol_key] = rows
