"""Tests for funding timeframe normalization behavior."""

from __future__ import annotations

from ingestion.funding import normalize_funding_timeframe


def test_normalize_funding_timeframe_uses_native_deribit_interval() -> None:
    assert normalize_funding_timeframe("deribit", "1m") == "8h"
    assert normalize_funding_timeframe("deribit", "M1") == "8h"
