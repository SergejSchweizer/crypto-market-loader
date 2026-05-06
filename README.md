# crypto-market-loader

## 1. Why This Project Exists

`crypto-market-loader` ingests Deribit market data into reproducible research storage.

It is built for:
- deterministic backfills
- incremental delta updates
- strict schema consistency
- production-ready downstream modeling inputs

Scope in this repository:
- Exchange: `deribit`
- Datatypes: `spot`, `perp`, `oi`, `funding`
- Symbols: `BTC`, `ETH`, `SOL`
- Ingestion policy: `1m` bronze-build timeframe (funding remains native `8h`)
- Storage target: Parquet lake

## 2. End-to-End Flow

```text
CLI command
  -> application services (fetch, gapfill, storage, artifacts)
  -> ingestion adapters (Deribit endpoints)
  -> normalized typed rows
  -> sinks:
       A) Parquet partition lake
  -> optional sample CSV artifacts
```

## 3. Project Overview

### 3.1 Datatype Importance (Quant Modeling)

| Datatype | Market Domain | Importance |
|---|---|---|
| `perp` | Perpetual markets | `5/5` |
| `oi` | Perpetual markets | `4/5` |
| `funding` | Perpetual markets | `3/5` |
| `spot` | Spot markets | `2/5` |

### 3.2 Market Ownership

| Datatype | Belongs To |
|---|---|
| `spot` | Spot markets |
| `perp` | Perpetual markets |
| `oi` | Perpetual markets |
| `funding` | Perpetual markets |

## 4. Data Contracts By Datatype

### 4.1 `spot`
- Source endpoint: `public/get_tradingview_chart_data`
- Stored cadence: `1m`
- Parquet dataset: `dataset_type=spot`

- Timeframe handling: fixed `1m` cadence (no variable timeframe key in core contract)
- Core fields:
  - `open_price`, `high_price`, `low_price`, `close_price`: OHLC Preise pro 1m-Intervall
  - `volume`: gehandelte Basis-Menge im Intervall
  - `quote_volume`: gehandeltes Notional in Quote-Währung
  - `trade_count`: Anzahl Trades im Intervall

### 4.2 `perp`
- Source endpoint: `public/get_tradingview_chart_data`
- Stored cadence: `1m`
- Parquet dataset: `dataset_type=perp`

- Timeframe handling: fixed `1m` cadence (no variable timeframe key in core contract)
- Core fields:
  - `open_price`, `high_price`, `low_price`, `close_price`: OHLC Preise pro 1m-Intervall (Perpetual)
  - `volume`: gehandelte Kontrakt-/Basis-Menge im Intervall
  - `quote_volume`: gehandeltes Notional in Quote-Währung
  - `trade_count`: Anzahl Trades im Intervall

### 4.3 `oi`
- Source endpoint: `public/get_last_settlements_by_instrument`
- Stored cadence: raw event timestamp
- Bronze persistence: observed source rows only (no synthetic gap-fill rows at bronze layer)
- Parquet dataset: `dataset_type=oi`

- Timeframe handling: source event-time rows are preserved without feature-grid expansion
- Core fields:
  - `open_interest`: beobachteter Open-Interest-Wert des Quellpunkts
  - `open_interest_value`: Wertmaß von Open Interest laut Quelle (falls vorhanden)
- Semantics:
  - bronze stores observed rows only; synthetic interpolation is deferred to later medallion steps

### 4.4 `funding`
- Source endpoint: `public/get_funding_rate_history`
- Stored cadence: raw event timestamp
- Parquet dataset: `dataset_type=funding`

- Timeframe handling: source event-time rows are preserved without bucket aggregation
- Core fields:
  - `funding_rate`: Funding-Rate des nativen 8h-Fensters
  - `index_price`: Referenz-/Indexpreis zum Funding-Zeitpunkt
  - `mark_price`: Mark-Preis zum Funding-Zeitpunkt

## 5. Repository Architecture

```text
project/
|-- api/
|-- application/
|-- ingestion/
|-- docs/
|-- samples/
|-- tests/
|-- README.md
|-- REPORT.md
`-- AGENTS.md
```

Key modules:
- `application/services/fetch_service.py`: sequential fetch orchestration
- `application/services/storage_service.py`: parquet write orchestration
- `application/services/artifact_service.py`: sample CSV output
- `ingestion/lake.py`: partitioned parquet read/write/load
- `api/commands/loader.py`: main online ingestion entrypoint

## 6. Installation And Environment

### 6.1 Prerequisites
- Python `3.11+`

### 6.2 Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

If repository path changes, recreate `.venv` so entrypoint shebangs stay valid.

### 6.3 Core Dependencies
- `pyarrow`: parquet I/O
- `pandas`, `numpy`: dataframe and numerical operations
- `polars`: fast dataframe transformations for silver/gold workflows
- `duckdb`: SQL analytics over parquet datasets

### 6.4 Tooling Dependencies
- `ruff`: lint/style checks
- `mypy`: static typing checks
- `pytest`: tests

## 7. Running Bronze Build

### 7.1 Canonical Commands

Spot:

```bash
python3 main.py bronze-build --exchange deribit --market spot --symbols BTC ETH SOL
```

All datatypes:

```bash
python3 main.py bronze-build --exchange deribit --market spot perp oi funding --symbols BTC ETH SOL --save-parquet-lake --lake-root lake/bronze
```


### 7.2 Ingestion Policy
- `bronze-build` has no timeframe CLI parameter; scheduling uses fixed `1m`.
- Stored timestamp semantics:
  - `spot`, `perp`: interval-aligned OHLCV bars (`1m`)
  - `oi`, `funding`: raw source event timestamps (no bucket aggregation)

### 7.3 Delta Behavior
Default mode is delta-first:
- first run without history: full bootstrap
- next runs: continue from latest stored open time + one interval
- explicit CLI control:
  - `--tail-delta-only` forces tail-only mode (overrides config)
  - `--full-gap-fill` enables historical internal gap checks (overrides config)

### 7.4 Concurrency Behavior
- Fetch execution uses bounded concurrency (`DEPTH_FETCH_CONCURRENCY`).
- Time-range fetches are split into UTC day windows and processed in randomized order.
- Parquet partition writing remains parallelized.

## 8. Storage Design

### 8.1 Parquet Lake Layout

```text
dataset_type=spot/
  exchange=<exchange>/instrument_type=spot/symbol=<symbol>/timeframe=<interval>/month=<YYYY-MM>/date=<YYYY-MM-DD>/data.parquet

dataset_type=perp/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/month=<YYYY-MM>/date=<YYYY-MM-DD>/data.parquet

dataset_type=oi/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/month=<YYYY-MM>/date=<YYYY-MM-DD>/data.parquet

dataset_type=funding/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/month=<YYYY-MM>/date=<YYYY-MM-DD>/data.parquet
```

### 8.2 Architecture/Storage Tradeoffs
- Parquet lake: cheap historical storage, partition-friendly batch reads, reproducibility

## 9. Silver Transformation

### 9.1 Command

```bash
python3 main.py silver-build --bronze-root lake/bronze --silver-root lake/silver --exchange deribit --market spot perp oi funding --timeframe 1m
python3 main.py silver-build --bronze-root lake/bronze --silver-root lake/silver --exchange deribit --market spot perp oi funding --timeframe 1m --plot
```

### 9.2 Silver Output Layout (Monthly)

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

### 9.3 Silver Sidecars

Silver does not write `silver/reports/...` aggregated report artifacts.  
When `--manifest` is provided, silver writes per-month manifest sidecars next to each monthly parquet:

```text
.../<SYMBOL>-<YYYY-MM>.json
```

When `--plot` is provided, silver writes per-month plot sidecars:

```text
.../<SYMBOL>-<YYYY-MM>.png
```

Report fields include:
- `rows_in`, `rows_out`, `duplicates_removed`
- `invalid_ohlc_rows`, `null_price_rows`
- `min_timestamp`, `max_timestamp`
- `period_start`, `period_end`, `months_processed`
- `columns` (exact output column names for the reported dataset)

## 10. Datetime And Sampling

### 10.1 Datetime Semantics
- `open_time`, `close_time`, `event_time` are UTC timestamps
- for `oi` and `funding`, raw source event timestamps are preserved
- `oi_observed` stores only real observed OI points (no synthetic 1m rows)
- `oi_1m_feature` is a derived minute grid with `oi_is_observed` / `oi_is_ffill` and observation lag fields
- for OHLCV, interval alignment is preserved
- `ingested_at` is UTC write timestamp

### 10.2 Samples
- bronze-build CSV sampling behavior is separate from silver transformation

## 11. Gold Transformation

### 11.1 Command

```bash
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --symbols BTC ETH SOL
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --dataset-id gold.market.full.m1 --dataset-version v1.0.0
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --dataset-id gold.market.full.m1 --auto-version --version-base v1.0.0
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --symbols BTC ETH SOL --manifest
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --symbols BTC ETH SOL --plot
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --symbols BTC ETH SOL --manifest --plot
python3 main.py gold-build --silver-root lake/silver --gold-root lake/gold --exchange deribit --no-json-output
```

Default behavior: when `--dataset-id` is omitted, `gold-build` attempts all supported dataset variants and skips unavailable symbol/dataset combinations.

Automatic versioning:
- `--auto-version` enables semantic version bumping by comparing the current contract to the latest manifest for the same `dataset_id` + `symbol`.
- `--version-base` is used when no prior manifest exists (initial release).
- bump policy:
  - `major`: breaking contract change (removed/renamed columns, join policy/source dataset removals, incompatible column-order change)
  - `minor`: additive contract change (new columns/source datasets)
  - `patch`: same contract, changed source data
  - `none`: no contract/data change

### 11.2 Gold Output Contract

For each base-asset symbol (`BTC`, `ETH`, `SOL`), gold writes:

```text
lake/gold/<symbol>_<datasetid>_<datasetversion>_<featuresethash>_<sourcedatahash>_<gitshort>.parquet
lake/gold/<symbol>_<datasetid>_<datasetversion>_<featuresethash>_<sourcedatahash>_<gitshort>.json
lake/gold/<symbol>_<datasetid>_<datasetversion>_<featuresethash>_<sourcedatahash>_<gitshort>.png
```

The `.json` artifact is generated only when `--manifest` is provided.
The `.png` artifact is generated only when `--plot` is provided.

`featuresethash` is derived from dataset contract inputs (dataset id/version, columns, join policy).
`sourcedatahash` is derived from source silver dataset summaries.
`gitshort` is resolved from `git rev-parse HEAD` (first 8 chars, fallback: `nogit`).
The PNG plot is generated with the same basename and contains all numeric features as rows:
- left panel (80% width): feature line plot with metadata legend
- right panel (20% width): feature distribution histogram

Each symbol parquet is built by combining silver datasets:
- `spot` (`timeframe=1m`)
- `perp` (`timeframe=1m`)
- `oi_1m_feature` (`timeframe=1m`)
- `funding_1m_feature` (`timeframe=1m`)

The JSON metadata file contains dataset-level and feature-level metadata, including:
- dataset id/version + build provenance: `feature_set_hash`, `source_data_hash`, `git_commit_hash`, `build_id`
- build timestamp (UTC), row/column stats and timestamp bounds
- source silver dataset summaries (columns, row counts, source symbols)
- per-feature metadata (`dtype`, null counts, and numeric distribution stats such as mean/std/min/max)
- no filesystem paths are stored in the JSON

## 12. Testing And Quality Gates

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

## 13. Deployment And Operations

Current mode:
- local CLI execution
- single-instance lock file: `.run/crypto-market-loader.lock`

Logging:
- default log path: `/volume1/Temp/logs/`
- override via `DEPTH_SYNC_LOG_DIR`

Environment:
- optional command defaults can be set in `config.yaml` (CLI flags override YAML values)
- keep `config.yaml` untracked
- initialize `config.yaml` from tracked `sample_config.yaml` and set secrets locally

YAML config shape:

```yaml
global:
  no_json_output: false

env:
  DEPTH_HTTP_TIMEOUT_S: 8
  DEPTH_HTTP_MAX_RETRIES: 2
  DEPTH_HTTP_RETRY_BACKOFF_S: 0.5
  DEPTH_SYNC_LOG_DIR: /volume1/Temp/logs
  DEPTH_FETCH_CONCURRENCY: 6
  DEPTH_FETCH_TASK_TIMEOUT_S: 300

bronze-build:
  exchange: deribit
  market: [spot, perp, oi, funding]
  symbols: [BTC, ETH]
  save_parquet_lake: true
  lake_root: lake/bronze
  tail_delta_only: true

silver-build:
  bronze_root: lake/bronze
  silver_root: lake/silver
  market: [spot, perp, oi, funding]
  manifest: false
  plot: false

gold-build:
  silver_root: lake/silver
  gold_root: lake/gold
  symbols: [BTC, ETH]
  manifest: false
  plot: true
  no_json_output: false

export-descriptive-stats:
  start_time: "2026-01-01T00:00:00+00:00"
  end_time: "2026-01-31T23:59:59+00:00"
```

Use non-default config path:

```bash
python3 main.py --config /path/to/config.yaml bronze-build
```

Bootstrap local config:

```bash
cp sample_config.yaml config.yaml
chmod 600 config.yaml
```

Runtime configuration variables are provided in `config.yaml` under `env:` and injected into the process environment at CLI start.

## 13. Known Limitations
- Deribit-only integration
- no exchange failover routing
- no trade-level ingestion in current scope

## 14. Future Improvements
- scheduled parquet compaction and retention
- multi-exchange adapter expansion
- schema/version migration utilities
- richer ingestion observability (throughput/error dashboards)
