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

- `config.yaml` is the canonical configuration file
- keep permissions restrictive (`chmod 600 config.yaml`)
- CLI flags override config defaults

Validate config permissions:

```bash
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
- optional lower bounds can be configured per exchange-symbol via
  `bronze-build.exchange_symbol_start_dates` entries like
  `deribit:BTC=2023-04-24`
- when `--save-parquet-lake` is enabled, Bronze guarantees sidecars for each `data.parquet`:
  - manifest: `data.json`
  - plot: `data.png`
  - missing sidecars on existing matching Bronze files are backfilled during the run

Dataset semantics (precise):
- `spot`:
  - Describes the underlying spot market traded on the exchange (cash market, no expiry/funding).
  - Rows are 1-minute OHLCV candles: `open/high/low/close`, traded `volume`, `quote_volume`, `trade_count`.
  - Timestamp semantics: `open_time`/`close_time` represent the closed 1-minute bar window in UTC.
  - Nuances:
    - Measures executed trades only (not order book liquidity).
    - Price can diverge from perpetual due to basis, inventory pressure, or temporary dislocations.
    - Best used as underlying price anchor in cross-market features.
- `perp`:
  - Describes perpetual futures market microstructure aggregated to 1-minute OHLCV bars.
  - Same candle fields as `spot`, but for perpetual instruments (levered derivatives).
  - Timestamp semantics: same 1-minute bar windowing as spot.
  - Nuances:
    - Includes derivatives-specific behavior (liquidation cascades, leverage effects, risk-premium/basis).
    - Volume/trade dynamics are not directly comparable to spot notional without normalization.
    - Preferred for short-horizon derivatives signal extraction.
- `oi` (open interest observations):
  - Describes outstanding open derivatives exposure (total open contracts/positions), not traded flow.
  - Rows are event-time observations with `open_interest` and `open_interest_value`.
  - Timestamp semantics: stored at source observation time (no synthetic resampling in Bronze).
  - Nuances:
    - Update cadence can be irregular; missing minutes do not imply zero OI.
    - OI can rise/fall without immediate price move; interpret jointly with perp return/volume.
    - In Silver, `oi_observed` and `oi_1m_feature` separate observed values from forward-filled state.
- `funding`:
  - Describes perpetual funding-rate transfer mechanism between long/short sides.
  - Rows contain observed `funding_rate`, `index_price`, `mark_price` at source event timestamps.
  - Timestamp semantics: event-time observations (Deribit native funding cadence), not minute bars.
  - Nuances:
    - Funding is discrete in time and should not be treated as per-minute realized PnL directly.
    - Positive/negative sign encodes payer side (market regime signal), but impact depends on holding period and leverage.
    - In Silver, `funding_observed` and `funding_1m_feature` expose last-known funding state with leakage-safe asof logic.

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
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --l2-root remote_l2_m1_features --exchange deribit --dataset-id gold.hybrid.full_l2.m1 --l2-validation-mode lenient
```

Default dataset behavior:
- if `--dataset-id` is omitted, gold attempts all supported dataset variants
- unavailable symbol/dataset combinations are skipped (warning logged)

Supported dataset IDs:
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.market.full.m1`
- `gold.hybrid.full_l2.m1` (joins full market features with latest per-symbol L2 parquet from `--l2-root`; default `remote_l2_m1_features`)

Gold dataset feature matrix:

| Dataset ID | Includes | Feature families provided |
|---|---|---|
| `gold.market.core.m1` | `spot`, `perp` | `spot_open/high/low/close/volume`, `perp_open/high/low/close/volume` |
| `gold.market.core_funding.m1` | `spot`, `perp`, `funding_1m_feature` | core features + `funding_rate_last_known`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available` |
| `gold.market.full.m1` | `spot`, `perp`, `oi_1m_feature`, `funding_1m_feature` | core + `oi_open_interest`, `oi_is_observed`, `oi_is_ffill`, `minutes_since_oi_observation`, `oi_observation_lag_sec` + funding features |
| `gold.hybrid.full_l2.m1` | `spot`, `perp`, `oi_1m_feature`, `funding_1m_feature`, latest L2 | full-market features + all L2 columns prefixed with `l2_` (for example `l2_snapshot_count`, `l2_coverage_ratio`) |

### High-Importance Inputs For Market-Neutral (Self-Financing) Strategies

Recommended primary dataset:
- `gold.market.full.m1` (or `gold.hybrid.full_l2.m1` when reliable L2 is available)

High-importance feature groups:
- Relative-value spread basis:
  `perp_close - spot_close` (and normalized variants such as ratio/z-score).
- Funding carry state:
  `funding_rate_last_known`, `minutes_since_funding`, `is_funding_observation_minute`, `funding_data_available`.
- Positioning/crowding pressure:
  `oi_open_interest`, `oi_is_observed`, `oi_is_ffill`, `minutes_since_oi_observation`, `oi_observation_lag_sec`.
- Liquidity/execution context:
  `spot_volume`, `perp_volume` (and, for hybrid dataset, `l2_snapshot_count`, `l2_coverage_ratio` and other `l2_` features).

Practical note:
- For self-financing market-neutral designs, build signals from cross-market relationships
  (spot vs perp spread/carry/positioning) rather than absolute directional price level.

Gold artifacts:

```text
lake/gold/dataset_id=<dataset_id>/dataset_type=gold_symbol_dataset/feature_set_version=<dataset_version>/exchange=<exchange>/symbol=<symbol>/<SYMBOL>_GOLD_<featurehash>_<sourcehash>.parquet
lake/gold/dataset_id=<dataset_id>/dataset_type=gold_symbol_dataset/feature_set_version=<dataset_version>/exchange=<exchange>/symbol=<symbol>/<SYMBOL>_GOLD_<featurehash>_<sourcehash>.json
lake/gold/dataset_id=<dataset_id>/dataset_type=gold_symbol_dataset/feature_set_version=<dataset_version>/exchange=<exchange>/symbol=<symbol>/<SYMBOL>_GOLD_<featurehash>_<sourcehash>.png
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
- shared log file path: `/volume1/Temp/logs/crypto-market-loader.log` (override via `DEPTH_SYNC_LOG_FILE`)
- log retention/rotation: daily rotation with 30-day retention; rotated files are date-suffixed (for example `crypto-market-loader.log.2026-05-07`)
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

## Scheduled Pipeline (Python Script)

Use the built-in Python orchestrator to run Bronze -> Silver -> Gold in one cron-safe command:

```bash
python3 scripts/run_medallion_pipeline.py --config config.yaml
```

What it does:
- executes only what is defined in `medallion-pipeline` config
- requires three layer sections: `medallion-pipeline.bronze`, `medallion-pipeline.silver`, `medallion-pipeline.gold`
- uses `medallion-pipeline.execution_order` plus each layer `command` and `cli_args`
- no pipeline step flags are hardcoded in the script
- stops immediately on first failed step (non-zero exit)
- uses a non-blocking lock at `.run/full-pipeline.lock` to prevent overlapping runs
- appends logs to the same shared log file used by all commands (`DEPTH_SYNC_LOG_FILE`)
- writes explicit runtime markers such as `ACTIVE_STEP=bronze|silver|gold`

Cron example (hourly):

```cron
5 * * * * /usr/bin/python3 /home/vcs/git/crypto-market-loader/scripts/run_medallion_pipeline.py --config /home/vcs/git/crypto-market-loader/config.yaml
```
