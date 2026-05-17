"""Tests for canonical dataset contract mapping."""

from __future__ import annotations

from application.schema import dataset_contract


def test_dataset_contract_maps_spot_perp_oi() -> None:
    spot = dataset_contract("spot")
    perp = dataset_contract("perp")
    oi = dataset_contract("oi")
    funding = dataset_contract("funding")
    trades = dataset_contract("perp_trades")
    option_trades = dataset_contract("option_trades")

    assert spot.dataset_type == "spot"
    assert spot.instrument_type == "spot"

    assert perp.dataset_type == "perp"
    assert perp.instrument_type == "perp"

    assert oi.dataset_type == "oi"
    assert oi.instrument_type == "perp"

    assert funding.dataset_type == "funding"
    assert funding.instrument_type == "perp"
    assert trades.dataset_type == "perp_trades"
    assert trades.instrument_type == "perp"
    assert option_trades.dataset_type == "option_trades"
    assert option_trades.instrument_type == "option"
