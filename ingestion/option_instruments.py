"""Option instruments metadata ingestion interface (Deribit-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.exchanges import deribit_option_instruments
from ingestion.spot import Exchange
from ingestion.trades import _canonical_underlying_symbol


@dataclass(frozen=True)
class OptionInstrumentMetadata:
    """Normalized option instrument metadata snapshot row."""

    exchange: str
    symbol: str
    instrument_type: str
    instrument_name: str
    base_currency: str
    quote_currency: str
    settlement_currency: str
    strike: float
    option_type: str
    creation_timestamp: datetime
    expiration_timestamp: datetime
    contract_size: float
    tick_size: float
    source_endpoint: str
    open_time: datetime
    close_time: datetime
    interval: str


def _parse_option_instrument_row(exchange: Exchange, symbol: str, row: dict[str, object]) -> OptionInstrumentMetadata:
    creation_ms = int(cast(Any, row).get("creation_timestamp", 0))
    expiry_ms = int(cast(Any, row).get("expiration_timestamp", 0))
    open_time = datetime.fromtimestamp(max(creation_ms, 0) / 1000, tz=UTC)
    close_time = datetime.fromtimestamp(max(expiry_ms, 0) / 1000, tz=UTC)
    option_type = str(cast(Any, row).get("option_type", "unknown")).lower()
    return OptionInstrumentMetadata(
        exchange=exchange,
        symbol=_canonical_underlying_symbol(symbol),
        instrument_type="option",
        instrument_name=str(cast(Any, row).get("instrument_name", "")),
        base_currency=str(cast(Any, row).get("base_currency", "")),
        quote_currency=str(cast(Any, row).get("quote_currency", "")),
        settlement_currency=str(cast(Any, row).get("settlement_currency", "")),
        strike=float(cast(Any, row).get("strike", 0.0)),
        option_type=option_type if option_type in {"call", "put"} else "unknown",
        creation_timestamp=open_time,
        expiration_timestamp=close_time,
        contract_size=float(cast(Any, row).get("contract_size", 0.0)),
        tick_size=float(cast(Any, row).get("tick_size", 0.0)),
        source_endpoint="public_get_instruments",
        open_time=open_time,
        close_time=close_time,
        interval="snapshot",
    )


def fetch_option_instruments(
    exchange: Exchange,
    symbol: str,
    start_open_ms_bound: int | None = None,
) -> list[OptionInstrumentMetadata]:
    """Fetch and normalize option instruments metadata for one underlying symbol."""

    if exchange != "deribit":
        raise ValueError(f"Unsupported exchange '{exchange}'")
    normalized_symbol = _canonical_underlying_symbol(symbol)
    rows = deribit_option_instruments.fetch_option_instruments(currency=normalized_symbol)
    normalized_rows = [_parse_option_instrument_row(exchange, normalized_symbol, row) for row in rows]
    if start_open_ms_bound is None:
        return normalized_rows
    return [
        row for row in normalized_rows if int(row.open_time.timestamp() * 1000) >= start_open_ms_bound
    ]
