"""Tests for Pydantic runtime config validation."""

from __future__ import annotations

import pytest

from application.services.config_validation import validate_runtime_config


def test_validate_runtime_config_accepts_minimal_required_sections() -> None:
    payload: dict[str, object] = {
        "global": {"no_json_output": False},
        "env": {"DEPTH_HTTP_TIMEOUT_S": 8},
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": ["deribit"],
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }
    validate_runtime_config(payload)


def test_validate_runtime_config_rejects_invalid_export_types() -> None:
    payload: dict[str, object] = {
        "global": {},
        "env": {},
        "export-descriptive-stats": {
            "lake_root": "lake/bronze",
            "output_csv": "docs/out.csv",
            "start_time": "2026-01-01T00:00:00+00:00",
            "end_time": "2026-01-31T00:00:00+00:00",
            "exchanges": "deribit",
            "symbols": ["BTC"],
            "timeframes": ["1m"],
            "instrument_types": ["spot"],
        },
    }
    with pytest.raises(ValueError, match="Invalid config.yaml schema"):
        validate_runtime_config(payload)
