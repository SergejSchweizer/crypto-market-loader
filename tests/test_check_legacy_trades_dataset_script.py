"""Tests for legacy trades dataset migration checker script."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "check_legacy_trades_dataset.py"
    spec = importlib.util.spec_from_file_location("check_legacy_trades_dataset", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_script_returns_zero_when_no_legacy_path(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    module = _load_script_module()
    monkeypatch.setattr(sys, "argv", ["check_legacy_trades_dataset.py", "--lake-root", str(tmp_path)])
    assert module.main() == 0


def test_script_returns_two_when_legacy_path_exists(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    module = _load_script_module()
    (tmp_path / "dataset_type=trades").mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(sys, "argv", ["check_legacy_trades_dataset.py", "--lake-root", str(tmp_path)])
    assert module.main() == 2
