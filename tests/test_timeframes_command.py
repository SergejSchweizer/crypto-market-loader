"""Tests for list-spot-timeframes command behavior."""

from __future__ import annotations

import argparse
import logging

import pytest

from api.commands import timeframes as timeframes_cmd


def test_run_list_spot_timeframes_uses_single_exchange_default(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list[str] = []

    def _intervals(exchange: str) -> list[str]:
        seen.append(exchange)
        return ["1m", "5m"]

    monkeypatch.setattr(timeframes_cmd, "list_supported_intervals", _intervals)
    args = argparse.Namespace(exchange="deribit", exchanges=None)
    timeframes_cmd.run_list_spot_timeframes(args=args, logger=logging.getLogger("test"))
    assert seen == ["deribit"]
    assert '"deribit": [' in capsys.readouterr().out


def test_run_list_spot_timeframes_uses_exchanges_list(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list[str] = []

    def _intervals(exchange: str) -> list[str]:
        seen.append(exchange)
        return ["15m"]

    monkeypatch.setattr(timeframes_cmd, "list_supported_intervals", _intervals)
    args = argparse.Namespace(exchange="deribit", exchanges=["deribit"])
    timeframes_cmd.run_list_spot_timeframes(args=args, logger=logging.getLogger("test"))
    assert seen == ["deribit"]
    assert '"15m"' in capsys.readouterr().out
