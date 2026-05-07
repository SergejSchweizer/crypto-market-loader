# crypto-market-loader

Production-grade ingestion and transformation pipeline for Deribit market data with bronze/silver/gold parquet layers.

## Overview

Scope:
- Exchange: `deribit`
- Markets: `spot`, `perp`, `oi`, `funding`
- Symbols: `BTC`, `ETH`, `SOL`
- Storage: parquet lake

Design goals:
- deterministic and reproducible runs
- incremental ingestion and idempotent persistence
- strict schemas and typed service boundaries
- operationally safe CLI workflows

## Architecture

```text
CLI
  -> command modules (api/commands)
  -> application services (fetch, gapfill, storage, silver, gold)
  -> ingestion adapters (Deribit APIs)
  -> parquet lake + optional sidecar artifacts
```

Repository layout:

```text
api/
application/
ingestion/
docs/
tests/
README.md
REPORT.md
AGENTS.md
```

## Setup

Prerequisites:
- Python `3.11+`

Install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

Core libraries:
- `pyarrow` (parquet I/O)
- `polars` (silver/gold transforms)
- `duckdb` (SQL analytics on parquet)
- `pandas`, `numpy`

Quality tools:
- `pytest`, `ruff`, `mypy`

## Configuration

Runtime configuration is mandatory via `config.yaml`.

- `config.yaml` must be local/untracked and permission-restricted (`chmod 600 config.yaml`)
- `sample_config.yaml` is the tracked template
- CLI flags override config defaults

Create local config:

```bash
cp sample_config.yaml config.yaml
chmod 600 config.yaml
```

## Bronze Layer

Command:

```bash
python3 main.py bronze-build --exchange deribit --market spot perp oi funding --symbols BTC ETH SOL --save-parquet-lake --lake-root lake/bronze
```

Behavior:
- fixed ingestion cadence for OHLCV: `1m`
- `oi`/`funding` preserve source event timestamps
- incremental/tail-first operation with optional gap-fill checks

Bronze layout:

```text
dataset_type=spot|perp|oi|funding/
  exchange=<exchange>/instrument_type=<spot|perp>/symbol=<symbol>/timeframe=<interval>/month=<YYYY-MM>/date=<YYYY-MM-DD>/data.parquet
```

## Silver Layer

Command:

```bash
python3 main.py silver-build --bronze-root lake/bronze --silver-root lake/silver --exchange deribit --market spot perp oi funding --timeframe 1m
python3 main.py silver-build --bronze-root lake/bronze --silver-root lake/silver --exchange deribit --market spot perp oi funding --timeframe 1m --manifest --plot
```

Silver outputs (monthly):

```text
dataset_type=<spot|perp>/
  exchange=<exchange>/symbol=<symbol>/timeframe=1m/year=<YYYY>/month=<YYYY-MM>/<SYMBOL>-<YYYY-MM>.parquet

dataset_type=funding_observed/
  exchange=<exchange>/symbol=<symbol>/timeframe=8h/year=<YYYY>/month=<YYYY-MM>/<SYMBOL>-<YYYY-MM>.parquet

dataset_type=funding_1m_feature/
  exchange=<exchange>/symbol=<symbol>/timeframe=1m/year=<YYYY>/month=<YYYY-MM>/<SYMBOL>-<YYYY-MM>.parquet

dataset_type=oi_observed/
  exchange=<exchange>/symbol=<symbol>/timeframe=1m/year=<YYYY>/month=<YYYY-MM>/<SYMBOL>-<YYYY-MM>.parquet

dataset_type=oi_1m_feature/
  exchange=<exchange>/symbol=<symbol>/timeframe=1m/year=<YYYY>/month=<YYYY-MM>/<SYMBOL>-<YYYY-MM>.parquet
```

Silver sidecars:
- no aggregated `silver/reports/...` artifacts
- `--manifest` writes monthly `<SYMBOL>-<YYYY-MM>.json` next to parquet
- `--plot` writes monthly `<SYMBOL>-<YYYY-MM>.png` next to parquet
- sidecar manifests use gold-style metadata fields (`column_hash`, `feature_metadata`, provenance flags)

## Gold Layer

Command examples:

```bash
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --symbols BTC ETH SOL
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --dataset-id gold.market.full.m1 --dataset-version v1.0.0
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --dataset-id gold.market.full.m1 --auto-version --version-base v1.0.0
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --dataset-id gold.hybrid.full_l2.m1 --l2-validation-mode lenient
```

Default dataset behavior:
- if `--dataset-id` is omitted, gold attempts all supported dataset variants
- unavailable symbol/dataset combinations are skipped (warning logged)

Supported dataset IDs:
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.perp_funding.m1`
- `gold.market.full.m1`
- `gold.hybrid.full_l2.m1` (joins full market features with latest per-symbol L2 parquet from `dataset_id=gold.l2.micro.m1`)

Gold dataset feature matrix:

| Dataset ID | Includes | Feature families provided |
|---|---|---|
| `gold.market.core.m1` | `spot`, `perp` | `spot_open/high/low/close/volume`, `perp_open/high/low/close/volume` |
| `gold.market.core_funding.m1` | `spot`, `perp`, `funding_1m_feature` | core features + `funding_rate_last_known`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available` |
| `gold.market.perp_funding.m1` | `perp`, `funding_1m_feature` | perp OHLCV features + funding timing/state features |
| `gold.market.full.m1` | `spot`, `perp`, `oi_1m_feature`, `funding_1m_feature` | core + `oi_open_interest`, `oi_is_observed`, `oi_is_ffill`, `minutes_since_oi_observation`, `oi_observation_lag_sec` + funding features |
| `gold.hybrid.full_l2.m1` | `spot`, `perp`, `oi_1m_feature`, `funding_1m_feature`, latest L2 | full-market features + all L2 columns prefixed with `l2_` (for example `l2_snapshot_count`, `l2_coverage_ratio`) |

Gold artifacts:

```text
lake/gold/dataset_id=<dataset_id>/exchange=<exchange>/symbol=<symbol>/version=<dataset_version>/build_id=<featurehash>_<sourcehash>_<gitshort>/data.parquet
lake/gold/dataset_id=<dataset_id>/exchange=<exchange>/symbol=<symbol>/version=<dataset_version>/build_id=<featurehash>_<sourcehash>_<gitshort>/manifest.json
lake/gold/dataset_id=<dataset_id>/exchange=<exchange>/symbol=<symbol>/version=<dataset_version>/build_id=<featurehash>_<sourcehash>_<gitshort>/plot.png
```

- `.json` manifest is always generated for every gold dataset artifact
- `.png` plot is always generated for every gold dataset artifact
- `--manifest` and `--plot` flags are accepted for backward compatibility, but are no-ops for gold (generation is always on)
- plots and silver-reused gold plot style are capped to max `3000` evenly sampled points across full series
- gold datasets are built on a canonical 1-minute time grid, then source datasets are left-joined on (`timestamp_m1`, `exchange`, `symbol`)
- missing source minutes are preserved as null values (not dropped)
- for `gold.hybrid.full_l2.m1`, `--l2-validation-mode strict|lenient` controls imported L2 quality handling (`strict` fails on invalid rows, `lenient` drops invalid rows and records counts in manifest)
  - default mode is `strict`
  - in `strict`, failed symbols are skipped and no artifact directory is written for that failed build

## Gold Versioning

Manual mode:
- use `--dataset-version vMAJOR.MINOR.PATCH`

Auto mode:
- `--auto-version` + `--version-base vX.Y.Z`
- compares current contract against latest manifest for same `dataset_id` + `symbol`
- bump policy:
  - `major`: breaking contract change (removed/renamed fields, incompatible contract changes)
  - `minor`: additive contract changes
  - `patch`: same contract, source data changed
  - `none`: no contract/data change

Manifest provenance fields include:
- `dataset_id`, `dataset_version`
- `feature_set_hash`, `source_data_hash`, `git_commit_hash`, `build_id`
- source silver dataset summaries
- feature-level metadata and null/statistics profile
- temporal completeness metrics: `expected_minutes_in_span`, `missing_minutes_in_span`, `observed_row_coverage_ratio`
- missing-value audit metrics: `missing_value_count_total`, `missing_value_count_by_column` (plus per-feature `missing_values` in `feature_metadata`)
- hybrid L2 validation audit fields: `l2_validation_mode`, `l2_invalid_rows_found`, `l2_invalid_rows_dropped`

## Testing And Checks

Run all checks:

```bash
make check
```

Equivalent:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m ruff check .
.venv/bin/python -m mypy .
```

## Operations Notes

- single-instance runtime lock: `.run/crypto-market-loader.lock`
- default log path: `/volume1/Temp/logs` (override via `DEPTH_SYNC_LOG_DIR`)
- this repo currently targets Deribit only

## Limitations

- Deribit-only integration
- no exchange failover routing
- no trade-level ingestion in current scope

## Planned Improvements

- parquet compaction/retention routines
- multi-exchange adapters
- schema migration utilities
- richer observability dashboards
