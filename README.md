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
- Datatypes: `spot`, `perp`, `oi_m1_feature`, `funding`
- Symbols: `BTC`, `ETH`, `SOL`
- Ingestion policy: `1m` loader timeframe (funding remains native `8h`)
- Storage targets: Parquet lake and TimescaleDB

## 2. End-to-End Flow

```text
CLI command
  -> application services (fetch, gapfill, storage, artifacts)
  -> ingestion adapters (Deribit endpoints)
  -> normalized typed rows
  -> sinks:
       A) Parquet partition lake
       B) TimescaleDB typed tables
  -> optional sample CSV + plot artifacts
```

## 3. Project Overview

### 3.1 Datatype Importance (Quant Modeling)

| Datatype | Market Domain | Importance |
|---|---|---|
| `perp` | Perpetual markets | `5/5` |
| `oi_m1_feature` | Perpetual markets | `4/5` |
| `funding` | Perpetual markets | `3/5` |
| `spot` | Spot markets | `2/5` |

### 3.2 Market Ownership

| Datatype | Belongs To |
|---|---|
| `spot` | Spot markets |
| `perp` | Perpetual markets |
| `oi_m1_feature` | Perpetual markets |
| `funding` | Perpetual markets |

## 4. Data Contracts By Datatype

### 4.1 `spot`
- Source endpoint: `public/get_tradingview_chart_data`
- Stored cadence: `1m`
- Parquet dataset: `dataset_type=spot`
- Timescale table: `spot`
- Core fields: `open, high, low, close, volume, quote_volume, trade_count`

### 4.2 `perp`
- Source endpoint: `public/get_tradingview_chart_data`
- Stored cadence: `1m`
- Parquet dataset: `dataset_type=perp`
- Timescale table: `perp`
- Core fields: `open, high, low, close, volume, quote_volume, trade_count`

### 4.3 `oi_m1_feature`
- Source endpoint: `public/get_last_settlements_by_instrument`
- Stored cadence: `1m` feature grid
- Parquet dataset: `dataset_type=oi_m1_feature`
- Timescale table: `open_interest`
- Core fields:
  - `open_interest`, `open_interest_value`
  - `oi_ffill`, `oi_is_observed`, `minutes_since_oi_observation`
- Semantics:
  - observed OI minute: `oi_is_observed=true`, `minutes_since_oi_observation=0`
  - synthetic minute: forward-filled value, `oi_is_observed=false`, counter increments
  - no backward-fill before first observed point

### 4.4 `funding`
- Source endpoint: `public/get_funding_rate_history`
- Stored cadence: native `8h` (not transformed to `1m`)
- Parquet dataset: `dataset_type=funding`
- Timescale table: `funding`
- Core fields: `funding_rate`, `index_price`, `mark_price`

## 5. Repository Architecture

```text
project/
|-- api/
|-- application/
|-- infra/
|-- ingestion/
|-- tests/
|-- README.md
|-- REPORT.md
`-- AGENTS.md
```

Key modules:
- `application/services/fetch_service.py`: sequential fetch orchestration
- `application/services/storage_service.py`: parquet + Timescale write orchestration
- `application/services/artifact_service.py`: sample CSV/plot output
- `ingestion/lake.py`: partitioned parquet read/write/load
- `infra/timescaledb/sink.py`: table DDL, upserts, watermarks
- `api/commands/loader.py`: main online ingestion entrypoint
- `api/commands/timescaledb_ingest.py`: offline parquet -> Timescale ingest

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
- `matplotlib`: optional plotting

### 6.4 Tooling Dependencies
- `ruff`: lint/style checks
- `mypy`: static typing checks
- `pytest`: tests

## 7. Running The Loader

### 7.1 Canonical Commands

Spot:

```bash
python3 main.py loader --exchange deribit --market spot --symbols BTC ETH SOL --timeframe 1m
```

All datatypes:

```bash
python3 main.py loader --exchange deribit --market spot perp oi funding --symbols BTC ETH SOL --timeframe 1m --save-parquet-lake --lake-root lake/bronze
```

All datatypes + Timescale:

```bash
python3 main.py loader --exchange deribit --market spot perp oi funding --symbols BTC ETH SOL --timeframe 1m --save-timescaledb --timescaledb-schema market_data
```

With plots:

```bash
python3 main.py loader --exchange deribit --market spot perp oi funding --symbols BTC ETH SOL --timeframe 1m --save-parquet-lake --plot
```

### 7.2 Ingestion Policy
- Input timeframe must be `1m`/`M1`
- Effective persisted cadence:
  - `spot`, `perp`, `oi_m1_feature`: `1m`
  - `funding`: `8h`

### 7.3 Delta Behavior
Default mode is delta-first:
- first run without history: full bootstrap
- next runs: continue from latest stored open time + one interval
- optional `--full-gap-fill` performs historical internal gap checks

### 7.4 Concurrency Behavior
- Fetch execution is sequential to reduce Deribit blocking risk
- Parquet partition writing remains parallelized

## 8. Storage Design

### 8.1 Parquet Lake Layout

```text
dataset_type=spot/
  exchange=<exchange>/instrument_type=spot/symbol=<symbol>/timeframe=<interval>/date=<YYYY-MM>/data.parquet

dataset_type=perp/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/date=<YYYY-MM>/data.parquet

dataset_type=oi_m1_feature/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/date=<YYYY-MM>/data.parquet

dataset_type=funding/
  exchange=<exchange>/instrument_type=perp/symbol=<symbol>/timeframe=<interval>/date=<YYYY-MM>/data.parquet
```

### 8.2 TimescaleDB Tables
- `spot`
- `perp`
- `open_interest`
- `funding`
- `ingest_watermarks`

### 8.3 Architecture/Storage Tradeoffs
- Parquet lake: cheap historical storage, partition-friendly batch reads, reproducibility
- TimescaleDB: indexed low-latency time-series queries, robust incremental upserts
- Combined model: keeps history-oriented storage independent from serving-oriented storage

## 9. Datetime, Sampling, And Plot Nuances

### 9.1 Datetime Semantics
- `open_time`, `close_time`, `event_time` are UTC timestamps
- interval alignment follows exchange-derived bucket boundaries
- `ingested_at` is UTC write timestamp

### 9.2 Samples And Plots
- CSV samples are random across full available series (`random_state=42`)
- if series length `< 10`, sampling uses replacement (duplicates are expected)
- plots are generated only when `--plot` is enabled
- each plot uses full available period and downsampling capped at `1000` points

## 10. Testing And Quality Gates

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

## 11. Deployment And Operations

Current mode:
- local CLI execution
- single-instance lock file: `.run/crypto-market-loader.lock`

Logging:
- default log path: `/volume1/Temp/logs/`
- override via `DEPTH_SYNC_LOG_DIR`

Environment:
- `.env` is auto-loaded locally
- keep `.env` untracked

Key variables:
- HTTP: `DEPTH_HTTP_TIMEOUT_S`, `DEPTH_HTTP_MAX_RETRIES`, `DEPTH_HTTP_RETRY_BACKOFF_S`
- Compatibility knobs: `DEPTH_FETCH_CONCURRENCY`, `LOADER_OHLCV_CONCURRENCY`, `LOADER_OI_CONCURRENCY`, `LOADER_FUNDING_CONCURRENCY`
- Timescale ingest workers: `TIMESCALE_INGEST_WORKERS`
- DB config: `TIMESCALEDB_HOST`, `TIMESCALEDB_PORT`, `TIMESCALEDB_USER`, `TIMESCALEDB_PASSWORD`, `TIMESCALEDB_DB`, `PGSSLMODE`

## 12. Known Limitations
- Deribit-only integration
- no exchange failover routing
- no trade-level ingestion in current scope

## 13. Future Improvements
- scheduled parquet compaction and retention
- multi-exchange adapter expansion
- schema/version migration utilities
- richer ingestion observability (throughput/error dashboards)
