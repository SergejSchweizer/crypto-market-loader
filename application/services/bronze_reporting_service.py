"""Reporting helpers for Bronze command summaries."""

from __future__ import annotations


def trade_error_breakdown(trade_errors: dict[tuple[str, str, str], str]) -> dict[str, int]:
    """Return classified trade error counts."""

    total = len(trade_errors)
    net_unreachable = sum(1 for message in trade_errors.values() if str(message).startswith("[NET_UNREACHABLE]"))
    net_timeout = sum(1 for message in trade_errors.values() if str(message).startswith("[NET_TIMEOUT]"))
    other = total - net_unreachable - net_timeout
    return {
        "total": total,
        "net_unreachable": net_unreachable,
        "net_timeout": net_timeout,
        "other": other,
    }


def symbol_progress_rows(
    *,
    candle_tasks: list[tuple[str, str, str, str]],
    oi_tasks: list[tuple[str, str, str]],
    funding_tasks: list[tuple[str, str, str]],
    trade_tasks: list[tuple[str, str, str]],
    candle_results: dict[tuple[str, str, str, str], object],
    oi_results: dict[tuple[str, str, str], object],
    funding_results: dict[tuple[str, str, str], object],
    trade_results: dict[tuple[str, str, str], object],
) -> list[dict[str, object]]:
    """Return per-symbol progress rows with success ratio."""

    symbol_totals: dict[str, int] = {}
    symbol_success: dict[str, int] = {}
    for _exchange, _market, symbol, _timeframe in candle_tasks:
        symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
    for _exchange, symbol, _timeframe in oi_tasks:
        symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
    for _exchange, symbol, _timeframe in funding_tasks:
        symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
    for _exchange, _market, symbol in trade_tasks:
        symbol_totals[symbol] = symbol_totals.get(symbol, 0) + 1
    for _exchange, _market, symbol, _timeframe in candle_results:
        symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
    for _exchange, symbol, _timeframe in oi_results:
        symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
    for _exchange, symbol, _timeframe in funding_results:
        symbol_success[symbol] = symbol_success.get(symbol, 0) + 1
    for _exchange, _market, symbol in trade_results:
        symbol_success[symbol] = symbol_success.get(symbol, 0) + 1

    return [
        {
            "symbol": symbol,
            "success": symbol_success.get(symbol, 0),
            "total": total,
            "ratio": round(symbol_success.get(symbol, 0) / total, 4) if total > 0 else 0.0,
        }
        for symbol, total in sorted(symbol_totals.items())
    ]
