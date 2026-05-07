# Deribit Ingestion And Transformation Report

## Abstract
This project provides a reproducible, maintainable data pipeline for Deribit crypto markets. It implements typed bronze ingestion, monthly silver transformations, and versioned/provenanced gold datasets. The main result is an engineering baseline suitable for downstream quantitative research, with deterministic contracts, incremental operation, and explicit artifact lineage.

## Problem Statement
Early-stage market-data stacks often fail due to ad-hoc scripts, schema drift, and weak reproducibility. This repository addresses that by enforcing:
- explicit datatype contracts (`spot`, `perp`, `oi`, `funding`)
- modular service boundaries
- deterministic parquet output layouts
- test-backed command workflows

## System Summary

Pipeline:

```text
CLI -> application services -> exchange adapters -> normalized rows -> parquet layers
```

Layers:
- Bronze: source-shaped parquet partitions
- Silver: cleaned/derived monthly datasets with sidecars
- Gold: model-ready merged datasets with semantic versioning and hash provenance

## Data Contracts

### Bronze
- `spot`, `perp`: 1-minute OHLCV bars
- `oi`, `funding`: source event-time rows
- idempotent persistence via natural-key partition rewrites

### Silver
- Monthly parquet naming: `<SYMBOL>-<YYYY-MM>.parquet`
- Datasets:
  - `spot`, `perp`
  - `funding_observed`, `funding_1m_feature`
  - `oi_observed`, `oi_1m_feature`
- Optional monthly sidecars (`.json`, `.png`) with gold-style metadata

### Gold
Supported dataset variants:
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.perp_funding.m1`
- `gold.market.full.m1`
- `gold.hybrid.full_l2.m1`

Gold dataset feature matrix:

| Dataset ID | Inputs | Feature families |
|---|---|---|
| `gold.market.core.m1` | spot + perp | spot/perp OHLCV feature set |
| `gold.market.core_funding.m1` | spot + perp + funding_1m_feature | core OHLCV + funding state/timing features |
| `gold.market.perp_funding.m1` | perp + funding_1m_feature | perp OHLCV + funding features |
| `gold.market.full.m1` | spot + perp + oi_1m_feature + funding_1m_feature | core + OI + funding feature families |
| `gold.hybrid.full_l2.m1` | full market set + latest L2 gold parquet | full market features + prefixed L2 microstructure features (`l2_*`) |

Join policy for all gold datasets: canonical 1-minute time grid with left joins on
`timestamp_m1` + (`exchange`, `symbol`) across included source datasets. Missing source
minutes are preserved as null values.

Gold artifact policy:
- every gold build writes `data.parquet`, `manifest.json`, and `plot.png`
- for `gold.hybrid.full_l2.m1`, L2 validation mode is configurable:
  - `strict` (default): invalid L2 rows fail that symbol/dataset build
  - `lenient`: invalid L2 rows are dropped and audited in manifest fields
    (`l2_invalid_rows_found`, `l2_invalid_rows_dropped`)

Artifact identity fields:
- semantic contract version: `dataset_version`
- reproducibility hashes: `feature_set_hash`, `source_data_hash`, `git_commit_hash`, `build_id`

## Versioning Policy

Gold dataset versions follow semver.

- `major`: breaking contract changes
- `minor`: additive contract changes
- `patch`: same contract, data-source change

Auto-version mode (`--auto-version`) derives bumps by comparing current contract and source hashes to the latest manifest of the same `dataset_id` and symbol.

Manifest quality/provenance highlights:
- feature-level missing/null and numeric summary stats
- temporal coverage fields:
  - `expected_minutes_in_span`
  - `missing_minutes_in_span`
  - `observed_row_coverage_ratio`
- missing-value audit fields:
  - `missing_value_count_total`
  - `missing_value_count_by_column`

## Plotting Policy

Silver and gold plots share the same renderer style and are capped to **3000 evenly sampled points** across the full time series, preserving full-range representation while keeping rendering bounded.

## Reliability And Reproducibility Controls

- typed service interfaces and strict normalization
- command-driven deterministic workflows
- manifest-level provenance and source summaries
- quality gates: `pytest`, `ruff`, `mypy`
- mandatory runtime config file (`config.yaml`) with restricted permissions

## Current Limitations

- Deribit-only scope
- no trade-level stream ingestion
- no multi-exchange reconciliation

## Recommended Next Steps

1. Add continuous data quality checks (coverage/null/drift thresholds) per dataset variant.
2. Add retention/compaction jobs for long-horizon parquet maintenance.
3. Introduce contract migration tooling for future schema evolution.
4. Expand adapters to additional exchanges with unified symbol canonicalization.

## References
1. Deribit API documentation (market chart, settlements/open-interest, funding history).
2. Apache Arrow/Parquet documentation.
