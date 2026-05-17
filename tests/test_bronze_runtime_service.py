"""Tests for bronze runtime service helpers."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from application.dto import BronzeFetchPlanDTO
from application.services import bronze_runtime_service as runtime


def _plan() -> BronzeFetchPlanDTO:
    return BronzeFetchPlanDTO(
        exchanges=["deribit"],
        data_types=["spot"],
        ohlcv_markets=["spot"],
        symbols=["BTC"],
        perp_trade_symbols=["BTC"],
        option_trade_symbols=["BTC"],
        candle_tasks=[("deribit", "spot", "BTC", "1m")],
        oi_tasks=[],
        funding_tasks=[],
        trade_tasks=[],
    )


def test_load_checkpoint_handles_unreadable_stale_and_missing_completed(tmp_path: Path) -> None:
    path = tmp_path / "chk.json"
    logger = logging.getLogger("test")

    path.write_text("{", encoding="utf-8")
    assert runtime.load_bronze_checkpoint(path, "fp", logger) == {
        "candle": set(),
        "oi": set(),
        "funding": set(),
        "trade": set(),
    }

    path.write_text(json.dumps({"fingerprint": "other", "completed": {}}), encoding="utf-8")
    assert runtime.load_bronze_checkpoint(path, "fp", logger) == {
        "candle": set(),
        "oi": set(),
        "funding": set(),
        "trade": set(),
    }

    path.write_text(json.dumps({"fingerprint": "fp", "completed": []}), encoding="utf-8")
    assert runtime.load_bronze_checkpoint(path, "fp", logger) == {
        "candle": set(),
        "oi": set(),
        "funding": set(),
        "trade": set(),
    }


def test_fingerprint_and_write_checkpoint_roundtrip(tmp_path: Path) -> None:
    args = argparse.Namespace(
        exchange="deribit",
        lake_root="lake/bronze",
        tail_delta_only=True,
        start_date=None,
        symbol_start_dates=None,
        exchange_symbol_start_dates=None,
    )
    fp = runtime.bronze_checkpoint_fingerprint(args, _plan())
    assert len(fp) == 64

    path = tmp_path / "x" / "chk.json"
    completed = {"candle": {"a"}, "oi": set(), "funding": set(), "trade": {"b"}}
    runtime.write_bronze_checkpoint(path, fingerprint=fp, completed=completed)
    loaded = runtime.load_bronze_checkpoint(path, fp, logging.getLogger("test"))
    assert loaded["candle"] == {"a"}
    assert loaded["trade"] == {"b"}
