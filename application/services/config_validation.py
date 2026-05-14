"""Pydantic-based validation helpers for runtime YAML config."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, ValidationError


class _LoaderConfigModel(BaseModel):
    """Loose schema for loader defaults."""

    model_config = ConfigDict(extra="allow")
    exchanges: list[str] | None = None
    market: list[str] | None = None
    symbols: list[str] | None = None


class _ExportDescriptiveStatsModel(BaseModel):
    """Schema for export-descriptive-stats config section."""

    model_config = ConfigDict(extra="allow")
    lake_root: str
    output_csv: str
    start_time: str
    end_time: str
    exchanges: list[str]
    symbols: list[str]
    timeframes: list[str]
    instrument_types: list[str]


class _RuntimeConfigModel(BaseModel):
    """Top-level runtime config schema."""

    model_config = ConfigDict(extra="allow")
    global_: dict[str, Any] = {}
    env: dict[str, Any]
    export_descriptive_stats: _ExportDescriptiveStatsModel
    bronze_build: _LoaderConfigModel | None = None


def validate_runtime_config(config: dict[str, object]) -> None:
    """Validate loaded config mapping with Pydantic.

    This enforces required sections and basic value types while allowing
    additional project-specific keys.
    """

    normalized: dict[str, object] = dict(config)
    if "global" in normalized:
        normalized["global_"] = normalized.pop("global")
    if "export-descriptive-stats" in normalized:
        normalized["export_descriptive_stats"] = normalized.pop("export-descriptive-stats")
    if "bronze-build" in normalized:
        normalized["bronze_build"] = normalized.pop("bronze-build")

    try:
        _RuntimeConfigModel.model_validate(normalized)
    except ValidationError as exc:
        raise ValueError(f"Invalid config.yaml schema: {exc}") from exc
