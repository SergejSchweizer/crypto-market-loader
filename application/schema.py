"""Canonical dataset/instrument schema contracts for market data storage."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CliDataType = Literal["spot", "perp", "oi", "funding", "perp_trades", "option_trades", "option_instruments"]
DatasetType = Literal[
    "spot", "perp", "oi", "funding", "perp_trades", "option_trades", "option_instruments", "l2_orderbook"
]
InstrumentType = Literal["spot", "perp", "option"]


@dataclass(frozen=True)
class DatasetContract:
    """Mapping contract between CLI data type and storage partition semantics."""

    cli_data_type: CliDataType
    dataset_type: DatasetType
    instrument_type: InstrumentType


_CONTRACTS: dict[CliDataType, DatasetContract] = {
    "spot": DatasetContract(cli_data_type="spot", dataset_type="spot", instrument_type="spot"),
    "perp": DatasetContract(cli_data_type="perp", dataset_type="perp", instrument_type="perp"),
    "oi": DatasetContract(cli_data_type="oi", dataset_type="oi", instrument_type="perp"),
    "funding": DatasetContract(cli_data_type="funding", dataset_type="funding", instrument_type="perp"),
    "perp_trades": DatasetContract(
        cli_data_type="perp_trades",
        dataset_type="perp_trades",
        instrument_type="perp",
    ),
    "option_trades": DatasetContract(
        cli_data_type="option_trades",
        dataset_type="option_trades",
        instrument_type="option",
    ),
    "option_instruments": DatasetContract(
        cli_data_type="option_instruments",
        dataset_type="option_instruments",
        instrument_type="option",
    ),
}


def dataset_contract(cli_data_type: CliDataType) -> DatasetContract:
    """Return canonical storage contract for one CLI data type."""

    return _CONTRACTS[cli_data_type]
