# Multi-Exchange Candle Ingestion Baseline for BTC/ETH (Binance + Deribit)

## Abstract
This report documents the first implementation stage of a modular crypto data ingestion framework designed for research and production workflows. The immediate objective is to establish a reproducible baseline pipeline to fetch BTC/ETH market candles, including prices and traded volume, through public REST endpoints across multiple exchanges. We implement typed exchange adapters for Binance and Deribit and expose the workflow through a CLI command that supports configurable exchange, market type, symbols, interval, and request depth. The current dataset sources are Binance kline data and Deribit tradingview chart data queried on demand; each run returns recent OHLCV observations for specified instruments. Findings at this stage are engineering-focused: the pipeline successfully normalizes heterogeneous exchange payloads into deterministic records, validates timeframe constraints per exchange, supports paginated downloads for long lookbacks, can generate local price/volume plots for quick run-level verification, and can persist downloaded candles to partitioned parquet lake storage for downstream ingestion. The contribution of this stage is a maintainable ingestion foundation that can be extended to L2 order books, funding rates, and database-backed full/backfill synchronization modes in later iterations.

## Introduction
Crypto market research systems require reliable and repeatable ingestion layers before advanced modeling can be trusted.

Many prototype systems fail because they tightly couple data pulls, transformation logic, and ad hoc scripts.

We propose a modular ingestion baseline with strict typing, explicit interfaces, and command-line reproducibility.

This stage contributes: (1) a production-oriented spot ingestion module, (2) a reusable CLI workflow, and (3) test coverage for parsing and input validation.

## Literature Review
Initial stage. Full quantitative literature synthesis will be added once modeling and evaluation components are implemented.

## Dataset
- Source: Binance public REST API (`/api/v3/klines`) and Deribit public REST API (`/api/v2/public/get_tradingview_chart_data`).
- Sample period: rolling recent candles requested at runtime.
- Number of observations: user-defined via `--limit`.
- Variables: open, high, low, close, base volume, quote volume, trade count.
- Cleaning methodology: typed parsing and schema normalization.
- Train/test split: not applicable in baseline ingestion-only stage.

## Methodology
- Retrieve raw candle arrays from exchange-specific public endpoints.
- Map ordered fields into strongly typed `SpotCandle` records.
- Convert timestamps from epoch milliseconds to UTC datetime.
- Enforce request constraints (`limit > 0`) and paginate where exchange limits apply.

## Results
Results tables and figures are not yet included because this stage is ingestion infrastructure only.

## Discussion
The baseline demonstrates that multi-exchange candle data can be ingested through a deterministic, testable interface. Current limitations include candles-only scope (no L2/funding yet) and lack of persistence.

## Conclusion
Step 1 establishes a maintainable multi-exchange ingestion foundation for BTC/ETH candle data and enables immediate extension toward L2, funding, and database synchronization workflows.

## Appendix
None for this stage.
