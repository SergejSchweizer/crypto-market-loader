"""Integration-like tests for medallion pipeline script config behavior."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_pipeline_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_medallion_pipeline.py"
    spec = importlib.util.spec_from_file_location("run_medallion_pipeline", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_build_steps_uses_configured_market_args_with_trades(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    main_path = tmp_path / "main.py"
    main_path.write_text("print('ok')\n", encoding="utf-8")
    config_path = tmp_path / "config.yaml"
    config_path.write_text("x: 1\n", encoding="utf-8")
    cfg = {
        "medallion-pipeline": {
            "execution_order": ["bronze", "silver", "gold"],
            "bronze": {
                "enabled": True,
                "command": "bronze-build",
                "cli_args": ["--market", "spot", "perp", "oi", "funding", "perp_trades"],
            },
            "silver": {"enabled": False, "command": "silver-build", "cli_args": []},
            "gold": {"enabled": False, "command": "gold-build", "cli_args": []},
        }
    }
    steps = module._build_steps(main_path=main_path, config_path=config_path, config_data=cfg)
    assert len(steps) == 1
    args = steps[0].args
    assert "--market" in args
    assert "perp_trades" in args


def test_log_path_from_config_prefers_explicit_log_file(tmp_path: Path) -> None:
    module = _load_pipeline_module()
    cfg = {"env": {"DEPTH_SYNC_LOG_FILE": str(tmp_path / "logs" / "pipeline.log")}}
    out = module._log_path_from_config(config_data=cfg, repo_root=tmp_path)
    assert out == (tmp_path / "logs" / "pipeline.log").resolve()
