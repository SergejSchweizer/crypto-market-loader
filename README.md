# crypto-market-loader

Production-grade cryptocurrency market data ingestion, normalization, feature engineering, and dataset generation framework for quantitative research and systematic trading.

---

# Table Of Contents

- [crypto-market-loader](#crypto-market-loader)
- [Table Of Contents](#table-of-contents)
- [1. Project Goals](#1-project-goals)
- [2. System Overview](#2-system-overview)
  - [2.1 Core Design Principles](#21-core-design-principles)
  - [2.2 Medallion Architecture](#22-medallion-architecture)
  - [2.3 Supported Data Domains](#23-supported-data-domains)
- [3. Repository Structure](#3-repository-structure)
- [4. Installation](#4-installation)
- [5. Pipeline Architecture](#5-pipeline-architecture)
  - [5.1 Bronze Layer](#51-bronze-layer)
  - [5.2 Silver Layer](#52-silver-layer)
  - [5.3 Gold Layer](#53-gold-layer)
- [6. Dataset Definitions](#6-dataset-definitions)
  - [6.1 Spot OHLCV](#61-spot-ohlcv)
  - [6.2 Perpetual OHLCV](#62-perpetual-ohlcv)
  - [6.3 Open Interest](#63-open-interest)
  - [6.4 Funding Rate](#64-funding-rate)
  - [6.5 Tick Trades](#65-tick-trades)
- [7. Quantitative Interpretation Of Features](#7-quantitative-interpretation-of-features)
  - [Price Features](#price-features)
  - [Volume Features](#volume-features)
  - [Trade Flow Features](#trade-flow-features)
  - [Funding Features](#funding-features)
  - [Open Interest Features](#open-interest-features)
  - [Cross-Market Features](#cross-market-features)
- [8. Gold Dataset Definitions](#8-gold-dataset-definitions)
  - [gold.market.option_trades.m1](#goldmarketoption_tradesm1)
  - [gold.market.perp_trades.m1](#goldmarketperp_tradesm1)
  - [gold.market.core.m1](#goldmarketcorem1)
  - [gold.market.core\_funding.m1](#goldmarketcore_fundingm1)
  - [gold.market.full.m1](#goldmarketfullm1)
  - [gold.hybrid.full\_l2.m1](#goldhybridfull_l2m1)
- [9. Recommended Additional Features](#9-recommended-additional-features)
- [10. Missing Datasets And Future Extensions](#10-missing-datasets-and-future-extensions)
  - [L2 Order Book Data](#l2-order-book-data)
  - [Liquidation Data](#liquidation-data)
  - [Trade-Level Data](#trade-level-data)
  - [Options Surface Data](#options-surface-data)
  - [Cross-Exchange Data](#cross-exchange-data)
- [11. Storage Layout](#11-storage-layout)
  - [Bronze Layout](#bronze-layout)
  - [Silver Layout](#silver-layout)
  - [Gold Layout](#gold-layout)
- [12. Example Commands](#12-example-commands)
  - [Full Medallion Pipeline (Bronze+Silver+Gold)](#full-medallion-pipeline-bronzesilvergold)
  - [Bronze Build](#bronze-build)
  - [Silver Build](#silver-build)
  - [Gold Build](#gold-build)
- [13. Quant Research Usage](#13-quant-research-usage)
  - [Regime Detection](#regime-detection)
  - [Market-Neutral Strategies](#market-neutral-strategies)
  - [Forecasting](#forecasting)
  - [Reinforcement Learning](#reinforcement-learning)
- [14. Engineering Standards](#14-engineering-standards)
- [15. Roadmap](#15-roadmap)

---

# 1. Project Goals

`crypto-market-loader` is designed as a reproducible market data platform for cryptocurrency quantitative research.

Primary goals:

- deterministic ingestion
- schema-stable parquet datasets
- reproducible feature engineering
- medallion architecture separation
- ML-ready dataset generation
- scalable historical backfills
- quantitative research workflows

The repository is intended for:

- systematic trading
- market-neutral research
- volatility forecasting
- HMM regime detection
- reinforcement learning
- feature engineering pipelines
- derivatives analytics

---

# 2. System Overview

## 2.1 Core Design Principles

The repository follows the engineering principles defined in `AGENTS.md`:

- maintainability
- modularity
- reproducibility
- deterministic processing
- idempotent ingestion
- explicit interfaces
- production-grade architecture

## 2.2 Medallion Architecture

```text
Exchange APIs
      |
      v
+----------------+
| Bronze Layer   |
| Raw normalized |
+----------------+
      |
      v
+----------------+
| Silver Layer   |
| Feature tables |
+----------------+
      |
      v
+----------------+
| Gold Layer     |
| ML datasets    |
+----------------+
```

## 2.3 Supported Data Domains

| Dataset | Description |
|---|---|
| Spot OHLCV | Physical spot market |
| Perpetual OHLCV | Leveraged perpetual futures |
| Funding | Long/short positioning pressure |
| Open Interest | Aggregate leveraged exposure |
| Tick Trades | Historical trade-by-trade prints (REST backfill) |
| Option Tick Trades | Historical option trade prints (REST backfill) |

Current exchange support:

- Deribit

Primary symbols:

- BTC
- ETH
- SOL

---

# 3. Repository Structure

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

| Directory | Responsibility |
|---|---|
| `api/` | CLI entrypoints |
| `application/` | Pipeline orchestration |
| `ingestion/` | Exchange connectors |
| `tests/` | Validation and regression tests |
| `docs/` | Figures and documentation |

---

# 4. Installation

```bash
uv sync --extra dev
```

Runtime configuration uses:

```text
config.yaml
```

Recommended permissions:

```bash
chmod 600 config.yaml
```

---

# 5. Pipeline Architecture

## 5.1 Bronze Layer

Bronze stores raw normalized exchange data.

Properties:

- append-oriented
- deterministic
- audit-friendly
- minimal transformations
- preserves source fidelity

Bronze stores:

- OHLCV candles
- funding events
- open interest observations
- tick trades (historical REST backfill)
- option tick trades (historical REST backfill)

## 5.2 Silver Layer

Silver transforms raw records into engineered feature datasets.

Responsibilities:

- rolling statistics
- volatility features
- funding transformations
- OI transformations
- trade-tick to 1m aggregation
- canonical resampling
- forward filling
- feature manifests

## 5.3 Gold Layer

Gold produces final modeling datasets.

Responsibilities:

- canonical 1-minute alignment
- joining feature families
- versioned datasets
- plot generation
- manifests/provenance

---

# 6. Dataset Definitions

## 6.1 Spot OHLCV

Represents the underlying physical market.

Typical fields:

| Field | Meaning |
|---|---|
| open | First traded price |
| high | Highest traded price |
| low | Lowest traded price |
| close | Last traded price |
| volume | Base asset turnover |

Quantitative importance:

- baseline market direction
- volatility estimation
- trend structure
- lead/lag modeling
- spot/perp basis analysis

## 6.2 Perpetual OHLCV

Represents leveraged perpetual futures trading.

Important because perpetuals often lead spot markets during:

- liquidations
- leverage expansions
- speculative squeezes
- volatility events

Potential feature groups:

| Feature | Interpretation |
|---|---|
| perp returns | Leveraged directional pressure |
| perp volume | Speculative participation |
| basis vs spot | Carry and leverage state |
| volatility | Market stress |

## 6.3 Open Interest

Open Interest measures total leveraged exposure.

Important conceptual distinction:

| Concept | Meaning |
|---|---|
| observed OI | Native exchange observation |
| OI 1m feature | Forward-filled modeling feature |

Quantitative interpretation:

| Price | OI | Meaning |
|---|---|
| Up | Up | New longs entering |
| Down | Up | New shorts entering |
| Up | Down | Short covering |
| Down | Down | Long liquidation |

OI is extremely important for:

- leverage regime detection
- squeeze prediction
- volatility forecasting
- systemic stress estimation

## 6.4 Funding Rate

Funding transfers capital between longs and shorts.

Interpretation:

| Funding State | Market Meaning |
|---|---|
| Positive funding | Long crowding |
| Negative funding | Short crowding |
| Neutral funding | Balanced positioning |

Funding is highly valuable for:

- carry strategies
- market-neutral trading
- crowding analysis
- mean reversion systems
- regime detection

## 6.5 Tick Trades

Tick trades represent per-execution market prints.

Silver builds `perp_trades_1m_feature` from tick data and derives:

| Feature | Meaning |
|---|---|
| open/high/low/close | Minute-level trade-price path |
| volume / quote_volume | Executed flow intensity |
| trade_count | Activity/participation |
| buy/sell volume + counts | Directional aggressor pressure proxy |
| buy_volume_share | Buy-side flow dominance |

For options, Silver builds the analogous `option_trades_1m_feature` from `option_trades_observed`.

---

# 7. Quantitative Interpretation Of Features

## Price Features

Describe:

- trend
- momentum
- volatility clustering
- regime shifts

## Volume Features

Describe:

- participation intensity
- speculative activity
- stress conditions
- liquidity conditions

## Trade Flow Features

Describe:

- execution-level pressure
- buy/sell imbalance
- participation bursts
- short-horizon microstructure regime shifts

## Funding Features

Describe:

- directional crowding
- leverage imbalance
- carry state
- sentiment extremes

## Open Interest Features

Describe:

- leverage expansion
- leverage unwinds
- liquidation risk
- structural market stress

## Cross-Market Features

Most powerful features usually come from interactions:

| Combination | Interpretation |
|---|---|
| spot/perp spread | Futures premium |
| funding + OI | Crowded leverage |
| OI + volatility | Fragile market state |
| volume + funding | Speculative frenzy |

---

# 8. Gold Dataset Definitions

## gold.market.perp_trades.m1

Contains:

- trades (tick-to-1m flow features)

Use cases:

- flow-only modeling
- execution pressure analysis
- trade-activity regime signals

## gold.market.option_trades.m1

Contains:

- option trades (tick-to-1m flow features)

Use cases:

- option flow regime modeling
- options activity pressure analysis
- option/perp flow comparison studies

## gold.market.core.m1

Contains:

- spot features
- perpetual features

Use cases:

- forecasting
- volatility models
- regime detection

## gold.market.core_funding.m1

Adds:

- funding features

Use cases:

- carry modeling
- crowding analysis
- market-neutral systems

## gold.market.full.m1

Adds:

- open interest
- funding
- trades (tick-to-1m flow features)
- option trades (tick-to-1m flow features)
- full derivatives state

Use cases:

- advanced ML
- systemic risk modeling
- leverage-state analysis
- flow-aware leverage-state modeling

## gold.hybrid.full_l2.m1

Extends gold datasets with L2 order book features.
Includes spot/perp/funding/open-interest/perp-trades-derived 1m features plus L2.

Potential L2 features:

| Feature | Meaning |
|---|---|
| bid/ask imbalance | Liquidity pressure |
| spread | Market quality |
| order flow imbalance | Aggressive flow |
| microprice | Near-term directional bias |

---

# 9. Recommended Additional Features

Strong future feature candidates:

| Feature | Importance |
|---|---|
| rolling z-scores | Regime normalization |
| realized volatility | Risk estimation |
| EWMA statistics | Adaptive state |
| entropy measures | Market disorder |
| rolling correlations | Dependency structure |
| volatility-of-volatility | Stress estimation |
| basis z-score | Relative-value modeling |
| rolling hedge ratios | Market-neutral trading |

Recommended regime features:

- HMM probabilities
- volatility state labels
- liquidity regime labels
- market stress indicators

---

# 10. Missing Datasets And Future Extensions

## L2 Order Book Data

Highest-priority extension.

Enables:

- microstructure modeling
- execution research
- liquidity imbalance features

## Liquidation Data

Important for crypto markets.

Captures:

- forced flows
- liquidation cascades
- leverage flushes

## Trade-Level Data

Enables:

- signed volume
- order flow imbalance
- VPIN-style metrics

## Options Surface Data

Provides:

- implied volatility
- skew
- term structure
- volatility expectations

## Cross-Exchange Data

Currently missing but highly valuable:

- Binance vs Deribit spreads
- fragmented liquidity indicators
- cross-exchange funding divergence

---

# 11. Storage Layout

## Bronze Layout

```text
dataset_type=spot|perp|oi|funding|perp_trades|option_trades/
  exchange=<exchange>/
  instrument_type=<spot|perp>/
  symbol=<symbol>/
  timeframe=<interval|tick>/
  year=<YYYY>/
  month=<YYYY-MM>/
  date=<YYYY-MM-DD>/
  data.parquet
```

## Silver Layout

```text
dataset_type=<dataset>/
  exchange=<exchange>/
  symbol=<symbol>/
  timeframe=<interval>/
  year=<YYYY>/
  month=<YYYY-MM>/
  <SYMBOL>-<YYYY-MM>.parquet
```

## Gold Layout

```text
lake/gold/
  dataset_id=<dataset_id>/
  feature_set_version=<version>/
  exchange=<exchange>/
  symbol=<symbol>/
```

---

# 12. Example Commands

## Full Medallion Pipeline (Bronze+Silver+Gold)

```bash
uv run python scripts/run_medallion_pipeline.py --config config.yaml
```

This script runs all three layers in sequence (`bronze-build` -> `silver-build` -> `gold-build`)
using `medallion-pipeline` settings from `config.yaml`. It also enforces a non-blocking single-run
lock via `.run/full-pipeline.lock` and writes a shared append-only pipeline log.

## Bronze Build

```bash
uv run python main.py bronze-build \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades \
  --symbols BTC ETH SOL
```

Trade datasets can use independent symbol defaults and overrides:

- `--symbols` applies to `spot`, `perp`, `oi`, `funding`
- `--perp-trade-symbols` applies to `perp_trades` (default: `BTC ETH SOL`)
- `--option-trade-symbols` applies to `option_trades` (default: `BTC ETH SOL`)

### Bronze Resume Checkpoint

`bronze-build` writes a restart checkpoint at:

```text
.run/checkpoints/bronze-build.json
```

Behavior:

- Completed tasks are recorded incrementally during the run.
- If a run fails or is interrupted, the next run with the same effective plan resumes by skipping completed tasks.
- If all tasks complete successfully, the checkpoint is deleted automatically.

Manual reset:

```bash
rm -f .run/checkpoints/bronze-build.json
```

### Perp Trades Dataset Migration Note

Perpetual trade ticks now use `dataset_type=perp_trades` (not `dataset_type=trades`).

- New Bronze writes go to `dataset_type=perp_trades`.
- Silver `perp_trades` discovery/processing expects `dataset_type=perp_trades`.

If you have historical Bronze data in the legacy path, migrate or backfill it before running Silver:

```bash
# example legacy -> canonical path rename
mv lake/bronze/dataset_type=trades lake/bronze/dataset_type=perp_trades
```

## Silver Build

```bash
uv run python main.py silver-build \
  --bronze-root lake/bronze \
  --silver-root lake/silver \
  --exchange deribit \
  --market spot perp oi funding perp_trades option_trades \
  --timeframe 1m
```

## Gold Build

```bash
uv run python main.py gold-build \
  --silver-root lake/silver \
  --gold-root lake/gold \
  --exchange deribit \
  --dataset-id gold.market.full.m1
```

Weitere Gold-Dataset-IDs:

- `gold.market.perp_trades.m1` (perp-trade-flow Features only)
- `gold.market.option_trades.m1` (nur option-trade-flow Features)
- `gold.market.core.m1`
- `gold.market.core_funding.m1`
- `gold.hybrid.full_l2.m1`

## Quality Checks

```bash
uv run ruff check .
uv run mypy .
uv run ty check .
uv run lint-imports --config .importlinter
uv run python scripts/validate_config_with_pydantic.py --config config.yaml
uv run pytest
```

`pytest` includes coverage reporting for `application`, `ingestion`, and `api` via
`pyproject.toml` defaults. The same test+coverage command is enforced in `.pre-commit-config.yaml`.
Architecture import boundaries are validated with `import-linter` using `.importlinter`.
Runtime configuration schema is validated with Pydantic via `scripts/validate_config_with_pydantic.py`.

---

# 13. Quant Research Usage

## Regime Detection

Useful for:

- Gaussian HMMs
- Markov-switching models
- volatility state estimation

Most important features:

- perp returns
- OI changes
- funding
- realized volatility

## Market-Neutral Strategies

Important features:

- basis spreads
- funding carry
- leverage state
- hedge ratios

## Forecasting

Potential targets:

- realized volatility
- regime transitions
- volatility expansions
- return direction

## Reinforcement Learning

Gold datasets provide:

- deterministic replay
- aligned feature grids
- reproducible state spaces

---

# 14. Engineering Standards

The repository follows the engineering rules defined in `AGENTS.md`.

Important principles:

- typed code
- modular design
- reproducibility
- scalable storage
- deterministic outputs
- documentation consistency

Recommended tooling:

- pytest
- ruff
- mypy
- ty
- pyright

---

# 15. Roadmap

Recommended future directions:

| Priority | Area |
|---|---|
| High | Full L2 ingestion |
| High | Multi-exchange support |
| High | Liquidation datasets |
| High | Cross-exchange basis features |
| Medium | Options surface ingestion |
| Medium | TimescaleDB integration |
| Medium | MLFlow lineage tracking |
| Medium | Streaming ingestion |
