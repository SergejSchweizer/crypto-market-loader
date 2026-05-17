"""Tests for gold command helper behavior."""

from __future__ import annotations

import argparse
import logging

import pytest

from api.commands import gold as gold_cmd


def test_resolve_dataset_ids_returns_single_when_explicit() -> None:
    assert gold_cmd._resolve_dataset_ids("gold.market.full.m1") == ["gold.market.full.m1"]


def test_resolve_dataset_ids_returns_sorted_supported_when_missing() -> None:
    expected = sorted(gold_cmd.SUPPORTED_GOLD_DATASET_IDS)
    assert gold_cmd._resolve_dataset_ids(None) == expected


def test_resolve_gold_symbols_normalizes_and_deduplicates() -> None:
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=["btc", "BTC-PERPETUAL", "BTC_USDC", "ETH"],
        silver_root="unused",
        exchange="deribit",
    )
    assert symbols == ["BTC", "ETH"]


def test_resolve_gold_symbols_uses_discovery_when_symbols_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gold_cmd,
        "discover_gold_symbols_for_dataset",
        lambda silver_root, exchange, dataset_id: ["BTC", "ETH"],
    )
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=None,
        silver_root="lake/silver",
        exchange="deribit",
        dataset_id="gold.market.full.m1",
    )
    assert symbols == ["BTC", "ETH"]


def test_resolve_gold_symbols_uses_global_discovery_when_dataset_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gold_cmd, "discover_gold_symbols", lambda silver_root, exchange: ["SOL", "XRP"])
    symbols = gold_cmd._resolve_gold_symbols(
        symbols=None,
        silver_root="lake/silver",
        exchange="deribit",
        dataset_id=None,
    )
    assert symbols == ["SOL", "XRP"]


def test_validate_version_args_rejects_invalid_dataset_version() -> None:
    with pytest.raises(ValueError, match="Invalid --dataset-version"):
        gold_cmd._validate_version_args(auto_version=False, dataset_version="1.2.3", version_base="v1.0.0")


def test_validate_version_args_rejects_invalid_version_base_in_auto_mode() -> None:
    with pytest.raises(ValueError, match="Invalid --version-base"):
        gold_cmd._validate_version_args(auto_version=True, dataset_version="v1.2.3", version_base="1.0.0")


def test_run_gold_build_uses_helpers_and_emits_reports(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_resolve_dataset_ids", lambda dataset_id: [dataset_id or "gold.market.full.m1"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    class _Report:
        rows_out = 1
        parquet_path = "/tmp/data.parquet"

        def to_dict(self) -> dict[str, object]:
            return {"symbol": "BTC", "dataset_id": "gold.market.full.m1", "rows_out": 1}

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", lambda **kwargs: _Report())

    args = argparse.Namespace(
        silver_root="lake/silver",
        gold_root="lake/gold",
        l2_root="remote_l2_m1_features",
        exchange="deribit",
        dataset_id="gold.market.full.m1",
        dataset_version="v1.0.0",
        auto_version=False,
        version_base="v1.0.0",
        symbols=None,
        l2_validation_mode="strict",
        no_json_output=False,
    )
    gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))
    payload = capsys.readouterr().out
    assert "gold.market.full.m1" in payload


def test_run_gold_build_skips_symbol_on_value_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(gold_cmd, "_resolve_dataset_ids", lambda dataset_id: [dataset_id or "gold.market.full.m1"])
    monkeypatch.setattr(gold_cmd, "_resolve_gold_symbols", lambda **kwargs: ["BTC"])
    monkeypatch.setattr(gold_cmd, "_validate_version_args", lambda **kwargs: None)

    def _raise_for_build(**kwargs: object) -> object:
        raise ValueError("missing silver prerequisite")

    monkeypatch.setattr(gold_cmd, "build_gold_for_symbol", _raise_for_build)

    args = argparse.Namespace(
        silver_root="lake/silver",
        gold_root="lake/gold",
        l2_root="remote_l2_m1_features",
        exchange="deribit",
        dataset_id="gold.market.full.m1",
        dataset_version="v1.0.0",
        auto_version=False,
        version_base="v1.0.0",
        symbols=None,
        l2_validation_mode="strict",
        no_json_output=False,
    )
    with caplog.at_level(logging.INFO):
        gold_cmd.run_gold_build(args=args, logger=logging.getLogger("test"))
    payload = capsys.readouterr().out
    assert payload.strip() == '{\n  "reports": []\n}'
    assert (
        "Gold dataset skipped symbol=BTC dataset_id=gold.market.full.m1 reason=missing silver prerequisite"
        in caplog.text
    )
