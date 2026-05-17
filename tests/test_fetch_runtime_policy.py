"""Tests for fetch runtime policy helpers."""

from __future__ import annotations

import pytest

from application.services.fetch_runtime_policy import heartbeat_seconds, task_timeout_seconds


def test_task_timeout_seconds_zero_or_invalid_returns_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "0")
    assert task_timeout_seconds() is None
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "-5")
    assert task_timeout_seconds() is None
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "bad")
    assert task_timeout_seconds() is None


def test_task_timeout_seconds_positive_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEPTH_FETCH_TASK_TIMEOUT_S", "3.5")
    assert task_timeout_seconds() == 3.5


def test_heartbeat_seconds_default_and_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DEPTH_FETCH_HEARTBEAT_S", raising=False)
    assert heartbeat_seconds() == 30.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "5")
    assert heartbeat_seconds() == 5.0
    monkeypatch.setenv("DEPTH_FETCH_HEARTBEAT_S", "-1")
    assert heartbeat_seconds() == 30.0
