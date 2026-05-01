"""TimescaleDB sink for OHLCV, open-interest, and funding data."""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.funding import FundingPoint
from ingestion.lake import candle_record, funding_record, open_interest_record
from ingestion.open_interest import OpenInterestPoint
from ingestion.spot import Market, SpotCandle

OhlcvTableName = "ohlcv"
OpenInterestTableName = "open_interest"
FundingTableName = "funding"
WatermarkTableName = "ingest_watermarks"
_SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_PARQUET_BATCH_SIZE = 10_000
DEFAULT_DB_WRITE_BATCH_SIZE = 5_000
DEFAULT_TIMESCALE_INGEST_WORKERS = 1
PartitionFilter = Callable[[dict[str, str]], bool]
IngestProgressCallback = Callable[[dict[str, str]], None]
SeriesKey = tuple[str, str, str, str]


def _validate_sql_identifier(value: str, kind: str = "identifier") -> str:
    """Validate SQL identifier against strict safe pattern."""

    if not _SQL_IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid SQL {kind}: '{value}'")
    return value


def _db_settings() -> dict[str, Any]:
    """Read TimescaleDB connection settings from environment."""

    password = os.getenv("TIMESCALEDB_PASSWORD")
    if password is None or password == "":
        raise RuntimeError(
            "TIMESCALEDB_PASSWORD is required. Set it in your local environment or .env (not committed)."
        )

    return {
        "host": os.getenv("TIMESCALEDB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TIMESCALEDB_PORT", "54321")),
        "user": os.getenv("TIMESCALEDB_USER", "crypto"),
        "password": password,
        "dbname": os.getenv("TIMESCALEDB_DB", "crypto"),
        "sslmode": os.getenv("PGSSLMODE", "disable"),
    }


def _positive_env_int(name: str, default: int) -> int:
    """Read positive integer env var with fallback."""

    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _json_safe(value: object) -> object:
    """Convert nested objects into JSON-serializable primitives."""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return value


def _partition_from_path(path: Path) -> dict[str, str]:
    """Extract hive-style partition key/value pairs from path segments."""

    partition: dict[str, str] = {}
    for segment in path.parts:
        if "=" in segment:
            key, value = segment.split("=", 1)
            partition[key] = value
    return partition


def _iter_parquet_rows(
    lake_root: str,
    glob_pattern: str,
    allow_partition: PartitionFilter,
    batch_size: int,
) -> Iterator[dict[str, object]]:
    """Yield parquet rows for matching files while honoring partition filters."""

    import pyarrow.parquet as pq

    for data_file in sorted(Path(lake_root).glob(glob_pattern)):
        if not allow_partition(_partition_from_path(data_file)):
            continue
        parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
        for batch in parquet_file.iter_batches(batch_size=batch_size):  # type: ignore[no-untyped-call]
            yield from batch.to_pylist()


def _iter_matching_parquet_files(
    lake_root: str,
    glob_pattern: str,
    allow_partition: PartitionFilter,
) -> list[Path]:
    """Return sorted parquet file paths matching glob and partition filter."""

    matching_files: list[Path] = []
    for data_file in sorted(Path(lake_root).glob(glob_pattern)):
        if allow_partition(_partition_from_path(data_file)):
            matching_files.append(data_file)
    return matching_files


def _parse_month_partition(value: str) -> tuple[int, int] | None:
    """Parse ``YYYY-MM`` partition value into ``(year, month)`` tuple."""

    parts = value.split("-")
    if len(parts) != 2:
        return None
    year_raw, month_raw = parts
    if not (year_raw.isdigit() and month_raw.isdigit()):
        return None
    year = int(year_raw)
    month = int(month_raw)
    if month < 1 or month > 12:
        return None
    return year, month


def _filter_files_by_watermark_month(
    files: list[Path],
    watermark_by_series: dict[SeriesKey, datetime],
) -> list[Path]:
    """Drop parquet partitions strictly older than per-series watermark month."""

    filtered: list[Path] = []
    for data_file in files:
        partition = _partition_from_path(data_file)
        series_key: SeriesKey = (
            partition.get("exchange", ""),
            partition.get("instrument_type", ""),
            partition.get("symbol", ""),
            partition.get("timeframe", ""),
        )
        watermark = watermark_by_series.get(series_key)
        if watermark is None:
            filtered.append(data_file)
            continue

        partition_month = _parse_month_partition(partition.get("date", ""))
        if partition_month is None:
            filtered.append(data_file)
            continue

        watermark_month = (watermark.year, watermark.month)
        if partition_month < watermark_month:
            continue
        filtered.append(data_file)
    return filtered


def _build_ohlcv_db_row(row: dict[str, object]) -> dict[str, object]:
    """Map one parquet OHLCV row into DB upsert payload format."""

    return {
        "exchange": row.get("exchange"),
        "symbol": row.get("symbol"),
        "instrument_type": row.get("instrument_type"),
        "timeframe": row.get("timeframe"),
        "open_time": row.get("open_time"),
        "close_time": row.get("close_time"),
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "volume": row.get("volume"),
        "quote_volume": row.get("quote_volume"),
        "trade_count": row.get("trade_count"),
        "schema_version": row.get("schema_version", "v1"),
        "dataset_type": row.get("dataset_type", "ohlcv"),
        "event_time": row.get("event_time", row.get("open_time")),
        "ingested_at": row.get("ingested_at"),
        "run_id": row.get("run_id", "unknown"),
        "source_endpoint": row.get("source_endpoint", "unknown"),
        "extra": row.get("extra"),
    }


def _build_open_interest_db_row(row: dict[str, object]) -> dict[str, object]:
    """Map one parquet open-interest row into DB upsert payload format."""

    return {
        "exchange": row.get("exchange"),
        "symbol": row.get("symbol"),
        "instrument_type": row.get("instrument_type"),
        "timeframe": row.get("timeframe"),
        "open_time": row.get("open_time"),
        "close_time": row.get("close_time"),
        "open_interest": row.get("open_interest"),
        "open_interest_value": row.get("open_interest_value", 0.0),
        "oi_ffill": row.get("oi_ffill", row.get("open_interest")),
        "oi_is_observed": row.get("oi_is_observed", True),
        "minutes_since_oi_observation": row.get("minutes_since_oi_observation", 0),
        "schema_version": row.get("schema_version", "v1"),
        "dataset_type": row.get("dataset_type", "oi_m1_feature"),
        "event_time": row.get("event_time", row.get("open_time")),
        "ingested_at": row.get("ingested_at"),
        "run_id": row.get("run_id", "unknown"),
        "source_endpoint": row.get("source_endpoint", "unknown"),
    }


def _build_funding_db_row(row: dict[str, object]) -> dict[str, object]:
    """Map one parquet funding row into DB upsert payload format."""

    return {
        "exchange": row.get("exchange"),
        "symbol": row.get("symbol"),
        "instrument_type": row.get("instrument_type"),
        "timeframe": row.get("timeframe"),
        "open_time": row.get("open_time"),
        "close_time": row.get("close_time"),
        "funding_rate": row.get("funding_rate"),
        "index_price": row.get("index_price", 0.0),
        "mark_price": row.get("mark_price", 0.0),
        "schema_version": row.get("schema_version", "v1"),
        "dataset_type": row.get("dataset_type", "funding"),
        "event_time": row.get("event_time", row.get("open_time")),
        "ingested_at": row.get("ingested_at"),
        "run_id": row.get("run_id", "unknown"),
        "source_endpoint": row.get("source_endpoint", "unknown"),
    }


def _upsert_rows_in_batches(
    row_iter: Iterator[dict[str, object]],
    write_batch_size: int,
    upsert_fn: Callable[[Any, str, list[dict[str, object]]], int],
    conn: Any,
    schema: str,
) -> int:
    """Accumulate rows in bounded batches and flush them via provided upsert function."""

    total_count = 0
    buffer: list[dict[str, object]] = []
    for row in row_iter:
        buffer.append(row)
        if len(buffer) >= write_batch_size:
            total_count += upsert_fn(conn, schema, buffer)
            buffer = []
    if buffer:
        total_count += upsert_fn(conn, schema, buffer)
    return total_count


def _upsert_rows_from_parquet_files(
    *,
    settings: dict[str, Any],
    schema: str,
    parquet_files: list[Path],
    batch_size: int,
    write_batch_size: int,
    row_builder: Callable[[dict[str, object]], dict[str, object]],
    upsert_fn: Callable[[Any, str, list[dict[str, object]]], int],
    max_open_time_by_series: dict[SeriesKey, datetime],
    progress_callback: IngestProgressCallback | None = None,
) -> tuple[int, int, int, dict[SeriesKey, datetime]]:
    """Read parquet files and upsert rows in bounded batches using one DB connection."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet ingestion. Install project dependencies.") from exc

    scanned_rows = 0
    skipped_rows = 0
    latest_open_time_by_series: dict[SeriesKey, datetime] = {}

    def _row_iter() -> Iterator[dict[str, object]]:
        nonlocal scanned_rows, skipped_rows
        for data_file in parquet_files:
            partition = _partition_from_path(data_file)
            if progress_callback is not None:
                progress_callback(
                    {
                        "dataset": partition.get("dataset_type", "unknown"),
                        "exchange": partition.get("exchange", "unknown"),
                        "instrument_type": partition.get("instrument_type", "unknown"),
                        "symbol": partition.get("symbol", "unknown"),
                        "timeframe": partition.get("timeframe", "unknown"),
                        "time_range": partition.get("date", "unknown"),
                    }
                )
            parquet_file = pq.ParquetFile(data_file)  # type: ignore[no-untyped-call]
            for arrow_batch in parquet_file.iter_batches(batch_size=batch_size):  # type: ignore[no-untyped-call]
                for row in arrow_batch.to_pylist():
                    scanned_rows += 1
                    db_row = row_builder(row)
                    series_key = (
                        str(db_row.get("exchange", "")),
                        str(db_row.get("instrument_type", "")),
                        str(db_row.get("symbol", "")),
                        str(db_row.get("timeframe", "")),
                    )
                    last_open_time = max_open_time_by_series.get(series_key)
                    open_time = db_row.get("open_time")
                    if isinstance(open_time, datetime) and last_open_time is not None and open_time <= last_open_time:
                        skipped_rows += 1
                        continue
                    if isinstance(open_time, datetime):
                        previous = latest_open_time_by_series.get(series_key)
                        if previous is None or open_time > previous:
                            latest_open_time_by_series[series_key] = open_time
                    yield db_row

    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            ingested_rows = _upsert_rows_in_batches(
                row_iter=_row_iter(),
                write_batch_size=write_batch_size,
                upsert_fn=upsert_fn,
                conn=conn,
                schema=schema,
            )
    return ingested_rows, scanned_rows, skipped_rows, latest_open_time_by_series


def _create_schema_and_tables(conn: Any, schema: str) -> None:
    """Create schema/tables/hypertables/indexes when missing."""

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {safe_schema};")
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{OhlcvTableName} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                quote_volume DOUBLE PRECISION,
                trade_count BIGINT NOT NULL,
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                extra JSONB,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cur.execute(
            f"""
            SELECT create_hypertable(
                '{safe_schema}.{OhlcvTableName}',
                'open_time',
                if_not_exists => TRUE
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{OhlcvTableName}_symbol_time
            ON {safe_schema}.{OhlcvTableName} (exchange, symbol, timeframe, open_time DESC);
            """
        )

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{OpenInterestTableName} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                open_interest DOUBLE PRECISION NOT NULL,
                open_interest_value DOUBLE PRECISION NOT NULL,
                oi_ffill DOUBLE PRECISION NOT NULL,
                oi_is_observed BOOLEAN NOT NULL,
                minutes_since_oi_observation BIGINT NOT NULL,
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {safe_schema}.{OpenInterestTableName}
            ADD COLUMN IF NOT EXISTS oi_ffill DOUBLE PRECISION NOT NULL DEFAULT 0.0;
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {safe_schema}.{OpenInterestTableName}
            ADD COLUMN IF NOT EXISTS oi_is_observed BOOLEAN NOT NULL DEFAULT TRUE;
            """
        )
        cur.execute(
            f"""
            ALTER TABLE {safe_schema}.{OpenInterestTableName}
            ADD COLUMN IF NOT EXISTS minutes_since_oi_observation BIGINT NOT NULL DEFAULT 0;
            """
        )
        cur.execute(
            f"""
            SELECT create_hypertable(
                '{safe_schema}.{OpenInterestTableName}',
                'open_time',
                if_not_exists => TRUE
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{OpenInterestTableName}_symbol_time
            ON {safe_schema}.{OpenInterestTableName} (exchange, symbol, timeframe, open_time DESC);
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{FundingTableName} (
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open_time TIMESTAMPTZ NOT NULL,
                close_time TIMESTAMPTZ NOT NULL,
                funding_rate DOUBLE PRECISION NOT NULL,
                index_price DOUBLE PRECISION NOT NULL,
                mark_price DOUBLE PRECISION NOT NULL,
                schema_version TEXT NOT NULL,
                dataset_type TEXT NOT NULL,
                event_time TIMESTAMPTZ NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL,
                run_id TEXT NOT NULL,
                source_endpoint TEXT NOT NULL,
                PRIMARY KEY (exchange, instrument_type, symbol, timeframe, open_time)
            );
            """
        )
        cur.execute(
            f"""
            SELECT create_hypertable(
                '{safe_schema}.{FundingTableName}',
                'open_time',
                if_not_exists => TRUE
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{FundingTableName}_symbol_time
            ON {safe_schema}.{FundingTableName} (exchange, symbol, timeframe, open_time DESC);
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {safe_schema}.{WatermarkTableName} (
                dataset_type TEXT NOT NULL,
                exchange TEXT NOT NULL,
                instrument_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                last_open_time TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (dataset_type, exchange, instrument_type, symbol, timeframe)
            );
            """
        )


def _load_latest_open_time_by_key(conn: Any, schema: str, dataset_type: str) -> dict[SeriesKey, datetime]:
    """Load latest ingested open_time from watermark table by dataset/series."""

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    safe_table_name = _validate_sql_identifier(value=WatermarkTableName, kind="table")
    sql = f"""
        SELECT exchange, instrument_type, symbol, timeframe, last_open_time AS max_open_time
        FROM {safe_schema}.{safe_table_name}
        WHERE dataset_type = '{dataset_type}'
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    except Exception:  # noqa: BLE001
        return {}

    result: dict[SeriesKey, datetime] = {}
    for row in rows:
        if len(row) != 5:
            continue
        exchange, instrument_type, symbol, timeframe, max_open_time = row
        if not isinstance(max_open_time, datetime):
            continue
        result[(str(exchange), str(instrument_type), str(symbol), str(timeframe))] = max_open_time
    return result


def _upsert_ingest_watermarks(
    conn: Any,
    schema: str,
    dataset_type: str,
    watermark_by_series: dict[SeriesKey, datetime],
) -> None:
    """Persist watermark state for a dataset."""

    if not watermark_by_series:
        return
    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    safe_table_name = _validate_sql_identifier(value=WatermarkTableName, kind="table")
    sql = f"""
        INSERT INTO {safe_schema}.{safe_table_name} (
            dataset_type, exchange, instrument_type, symbol, timeframe, last_open_time, updated_at
        ) VALUES (
            %(dataset_type)s, %(exchange)s, %(instrument_type)s, %(symbol)s, %(timeframe)s,
            %(last_open_time)s, %(updated_at)s
        )
        ON CONFLICT (dataset_type, exchange, instrument_type, symbol, timeframe)
        DO UPDATE SET
            last_open_time = EXCLUDED.last_open_time,
            updated_at = EXCLUDED.updated_at;
    """
    updated_at = datetime.now(UTC)
    payload = [
        {
            "dataset_type": dataset_type,
            "exchange": exchange,
            "instrument_type": instrument_type,
            "symbol": symbol,
            "timeframe": timeframe,
            "last_open_time": last_open_time,
            "updated_at": updated_at,
        }
        for (exchange, instrument_type, symbol, timeframe), last_open_time in watermark_by_series.items()
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, payload)


def _upsert_ohlcv(conn: Any, schema: str, rows: list[dict[str, object]]) -> int:
    """Upsert OHLCV rows into TimescaleDB."""

    if not rows:
        return 0

    try:
        from psycopg.types.json import Json
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    sql = f"""
        INSERT INTO {safe_schema}.{OhlcvTableName} (
            exchange, symbol, instrument_type, timeframe, open_time, close_time,
            open, high, low, close, volume, quote_volume, trade_count,
            schema_version, dataset_type, event_time, ingested_at, run_id,
            source_endpoint, extra
        ) VALUES (
            %(exchange)s, %(symbol)s, %(instrument_type)s, %(timeframe)s, %(open_time)s, %(close_time)s,
            %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(quote_volume)s, %(trade_count)s,
            %(schema_version)s, %(dataset_type)s, %(event_time)s, %(ingested_at)s, %(run_id)s,
            %(source_endpoint)s, %(extra)s
        )
        ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            quote_volume = EXCLUDED.quote_volume,
            trade_count = EXCLUDED.trade_count,
            schema_version = EXCLUDED.schema_version,
            dataset_type = EXCLUDED.dataset_type,
            event_time = EXCLUDED.event_time,
            ingested_at = EXCLUDED.ingested_at,
            run_id = EXCLUDED.run_id,
            source_endpoint = EXCLUDED.source_endpoint,
            extra = EXCLUDED.extra;
    """
    payload = [{**row, "extra": Json(_json_safe(row.get("extra")))} for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, payload)
    return len(payload)


def _upsert_open_interest(conn: Any, schema: str, rows: list[dict[str, object]]) -> int:
    """Upsert open-interest rows into TimescaleDB."""

    if not rows:
        return 0

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    sql = f"""
        INSERT INTO {safe_schema}.{OpenInterestTableName} (
            exchange, symbol, instrument_type, timeframe, open_time, close_time,
            open_interest, open_interest_value, oi_ffill, oi_is_observed, minutes_since_oi_observation,
            schema_version, dataset_type, event_time, ingested_at, run_id, source_endpoint
        ) VALUES (
            %(exchange)s, %(symbol)s, %(instrument_type)s, %(timeframe)s, %(open_time)s, %(close_time)s,
            %(open_interest)s, %(open_interest_value)s, %(oi_ffill)s, %(oi_is_observed)s,
            %(minutes_since_oi_observation)s,
            %(schema_version)s, %(dataset_type)s, %(event_time)s, %(ingested_at)s, %(run_id)s, %(source_endpoint)s
        )
        ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            open_interest = EXCLUDED.open_interest,
            open_interest_value = EXCLUDED.open_interest_value,
            oi_ffill = EXCLUDED.oi_ffill,
            oi_is_observed = EXCLUDED.oi_is_observed,
            minutes_since_oi_observation = EXCLUDED.minutes_since_oi_observation,
            schema_version = EXCLUDED.schema_version,
            dataset_type = EXCLUDED.dataset_type,
            event_time = EXCLUDED.event_time,
            ingested_at = EXCLUDED.ingested_at,
            run_id = EXCLUDED.run_id,
            source_endpoint = EXCLUDED.source_endpoint;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def _upsert_funding(conn: Any, schema: str, rows: list[dict[str, object]]) -> int:
    """Upsert funding rows into TimescaleDB."""

    if not rows:
        return 0

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    sql = f"""
        INSERT INTO {safe_schema}.{FundingTableName} (
            exchange, symbol, instrument_type, timeframe, open_time, close_time,
            funding_rate, index_price, mark_price,
            schema_version, dataset_type, event_time, ingested_at, run_id, source_endpoint
        ) VALUES (
            %(exchange)s, %(symbol)s, %(instrument_type)s, %(timeframe)s, %(open_time)s, %(close_time)s,
            %(funding_rate)s, %(index_price)s, %(mark_price)s,
            %(schema_version)s, %(dataset_type)s, %(event_time)s, %(ingested_at)s, %(run_id)s, %(source_endpoint)s
        )
        ON CONFLICT (exchange, instrument_type, symbol, timeframe, open_time)
        DO UPDATE SET
            close_time = EXCLUDED.close_time,
            funding_rate = EXCLUDED.funding_rate,
            index_price = EXCLUDED.index_price,
            mark_price = EXCLUDED.mark_price,
            schema_version = EXCLUDED.schema_version,
            dataset_type = EXCLUDED.dataset_type,
            event_time = EXCLUDED.event_time,
            ingested_at = EXCLUDED.ingested_at,
            run_id = EXCLUDED.run_id,
            source_endpoint = EXCLUDED.source_endpoint;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


def save_market_data_to_timescaledb(
    candles_for_storage: dict[Market, dict[str, dict[str, list[SpotCandle]]]],
    open_interest_for_storage: dict[Market, dict[str, dict[str, list[OpenInterestPoint]]]],
    funding_for_storage: dict[Market, dict[str, dict[str, list[FundingPoint]]]] | None = None,
    schema: str = "market_data",
    create_schema: bool = True,
) -> dict[str, int | str]:
    """Persist fetched market data into TimescaleDB."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    ingested_at = datetime.now(UTC)

    ohlcv_rows: list[dict[str, object]] = []
    for market, candle_by_exchange in candles_for_storage.items():
        for candle_by_symbol in candle_by_exchange.values():
            for candles in candle_by_symbol.values():
                ohlcv_rows.extend(
                    [
                        candle_record(candle=item, market=market, run_id=run_id, ingested_at=ingested_at)
                        for item in candles
                    ]
                )

    oi_rows: list[dict[str, object]] = []
    for market, oi_by_exchange in open_interest_for_storage.items():
        for oi_by_symbol in oi_by_exchange.values():
            for items in oi_by_symbol.values():
                oi_rows.extend(
                    [
                        open_interest_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                        for item in items
                    ]
                )

    funding_rows: list[dict[str, object]] = []
    for market, funding_by_exchange in (funding_for_storage or {}).items():
        for funding_by_symbol in funding_by_exchange.values():
            for funding_items in funding_by_symbol.values():
                funding_rows.extend(
                    [
                        funding_record(item=item, market=market, run_id=run_id, ingested_at=ingested_at)
                        for item in funding_items
                    ]
                )

    settings = _db_settings()
    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            if create_schema:
                _create_schema_and_tables(conn=conn, schema=safe_schema)
            ohlcv_count = _upsert_ohlcv(conn=conn, schema=safe_schema, rows=ohlcv_rows)
            oi_count = _upsert_open_interest(conn=conn, schema=safe_schema, rows=oi_rows)
            funding_count = _upsert_funding(conn=conn, schema=safe_schema, rows=funding_rows)

    return {
        "schema": safe_schema,
        "ohlcv_rows": ohlcv_count,
        "oi_rows": oi_count,
        "funding_rows": funding_count,
    }


def save_parquet_lake_to_timescaledb(
    lake_root: str,
    schema: str = "market_data",
    create_schema: bool = True,
    exchanges: list[str] | None = None,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
    instrument_types: list[str] | None = None,
    progress_callback: IngestProgressCallback | None = None,
) -> dict[str, int | str]:
    """Ingest existing parquet lake rows into TimescaleDB without fetching from exchanges."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("psycopg is required for TimescaleDB output. Install project dependencies.") from exc
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet ingestion. Install project dependencies.") from exc

    safe_schema = _validate_sql_identifier(value=schema, kind="schema")
    parquet_batch_size = _positive_env_int("L2_TSDB_PARQUET_BATCH_SIZE", DEFAULT_PARQUET_BATCH_SIZE)
    write_batch_size = _positive_env_int("L2_TSDB_WRITE_BATCH_SIZE", DEFAULT_DB_WRITE_BATCH_SIZE)
    ingest_workers = _positive_env_int("TIMESCALE_INGEST_WORKERS", DEFAULT_TIMESCALE_INGEST_WORKERS)
    exchange_filter = {item.lower() for item in exchanges} if exchanges else None
    symbol_filter = {item.upper() for item in symbols} if symbols else None
    timeframe_filter = {item.lower() for item in timeframes} if timeframes else None
    instrument_filter = {item.lower() for item in instrument_types} if instrument_types else None

    def _allow(partition: dict[str, str]) -> bool:
        exchange_value = partition.get("exchange", "")
        instrument_value = partition.get("instrument_type", "")
        symbol_value = partition.get("symbol", "")
        timeframe_value = partition.get("timeframe", "")
        if exchange_filter is not None and exchange_value.lower() not in exchange_filter:
            return False
        if instrument_filter is not None and instrument_value.lower() not in instrument_filter:
            return False
        if symbol_filter is not None and symbol_value.upper() not in symbol_filter:
            return False
        if timeframe_filter is not None and timeframe_value.lower() not in timeframe_filter:
            return False
        return True

    all_ohlcv_files = sorted(
        [
            *_iter_matching_parquet_files(
                lake_root=lake_root,
                glob_pattern="dataset_type=spot/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet",
                allow_partition=_allow,
            ),
            *_iter_matching_parquet_files(
                lake_root=lake_root,
                glob_pattern="dataset_type=perp/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet",
                allow_partition=_allow,
            ),
        ]
    )
    all_oi_files = _iter_matching_parquet_files(
        lake_root=lake_root,
        glob_pattern="dataset_type=oi_m1_feature/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet",
        allow_partition=_allow,
    )
    all_funding_files = _iter_matching_parquet_files(
        lake_root=lake_root,
        glob_pattern="dataset_type=funding/exchange=*/instrument_type=*/symbol=*/timeframe=*/date=*/data.parquet",
        allow_partition=_allow,
    )

    settings = _db_settings()
    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            if create_schema:
                _create_schema_and_tables(conn=conn, schema=safe_schema)
            spot_latest_by_key = _load_latest_open_time_by_key(conn=conn, schema=safe_schema, dataset_type="spot")
            perp_latest_by_key = _load_latest_open_time_by_key(conn=conn, schema=safe_schema, dataset_type="perp")
            ohlcv_latest_by_key: dict[SeriesKey, datetime] = {}
            for series_key, value in {
                **spot_latest_by_key,
                **perp_latest_by_key,
            }.items():
                previous = ohlcv_latest_by_key.get(series_key)
                if previous is None or value > previous:
                    ohlcv_latest_by_key[series_key] = value
            oi_latest_by_key = _load_latest_open_time_by_key(
                conn=conn,
                schema=safe_schema,
                dataset_type="oi_m1_feature",
            )
            funding_latest_by_key = _load_latest_open_time_by_key(
                conn=conn,
                schema=safe_schema,
                dataset_type="funding",
            )

    ohlcv_files = _filter_files_by_watermark_month(all_ohlcv_files, ohlcv_latest_by_key)
    oi_files = _filter_files_by_watermark_month(all_oi_files, oi_latest_by_key)
    funding_files = _filter_files_by_watermark_month(all_funding_files, funding_latest_by_key)

    if ingest_workers <= 1:
        ohlcv_count, ohlcv_scanned, ohlcv_skipped, ohlcv_watermarks = _upsert_rows_from_parquet_files(
            settings=settings,
            schema=safe_schema,
            parquet_files=ohlcv_files,
            batch_size=parquet_batch_size,
            write_batch_size=write_batch_size,
            row_builder=_build_ohlcv_db_row,
            upsert_fn=_upsert_ohlcv,
            max_open_time_by_series=ohlcv_latest_by_key,
            progress_callback=progress_callback,
        )
        oi_count, oi_scanned, oi_skipped, oi_watermarks = _upsert_rows_from_parquet_files(
            settings=settings,
            schema=safe_schema,
            parquet_files=oi_files,
            batch_size=parquet_batch_size,
            write_batch_size=write_batch_size,
            row_builder=_build_open_interest_db_row,
            upsert_fn=_upsert_open_interest,
            max_open_time_by_series=oi_latest_by_key,
            progress_callback=progress_callback,
        )
        funding_count, funding_scanned, funding_skipped, funding_watermarks = _upsert_rows_from_parquet_files(
            settings=settings,
            schema=safe_schema,
            parquet_files=funding_files,
            batch_size=parquet_batch_size,
            write_batch_size=write_batch_size,
            row_builder=_build_funding_db_row,
            upsert_fn=_upsert_funding,
            max_open_time_by_series=funding_latest_by_key,
            progress_callback=progress_callback,
        )
    else:
        with ThreadPoolExecutor(max_workers=min(ingest_workers, 3)) as pool:
            ohlcv_future = pool.submit(
                _upsert_rows_from_parquet_files,
                settings=settings,
                schema=safe_schema,
                parquet_files=ohlcv_files,
                batch_size=parquet_batch_size,
                write_batch_size=write_batch_size,
                row_builder=_build_ohlcv_db_row,
                upsert_fn=_upsert_ohlcv,
                max_open_time_by_series=ohlcv_latest_by_key,
                progress_callback=progress_callback,
            )
            oi_future = pool.submit(
                _upsert_rows_from_parquet_files,
                settings=settings,
                schema=safe_schema,
                parquet_files=oi_files,
                batch_size=parquet_batch_size,
                write_batch_size=write_batch_size,
                row_builder=_build_open_interest_db_row,
                upsert_fn=_upsert_open_interest,
                max_open_time_by_series=oi_latest_by_key,
                progress_callback=progress_callback,
            )
            funding_future = pool.submit(
                _upsert_rows_from_parquet_files,
                settings=settings,
                schema=safe_schema,
                parquet_files=funding_files,
                batch_size=parquet_batch_size,
                write_batch_size=write_batch_size,
                row_builder=_build_funding_db_row,
                upsert_fn=_upsert_funding,
                max_open_time_by_series=funding_latest_by_key,
                progress_callback=progress_callback,
            )
            ohlcv_count, ohlcv_scanned, ohlcv_skipped, ohlcv_watermarks = ohlcv_future.result()
            oi_count, oi_scanned, oi_skipped, oi_watermarks = oi_future.result()
            funding_count, funding_scanned, funding_skipped, funding_watermarks = funding_future.result()

    with psycopg.connect(**settings) as conn:
        with conn.transaction():
            _upsert_ingest_watermarks(
                conn=conn,
                schema=safe_schema,
                dataset_type="spot",
                watermark_by_series=ohlcv_watermarks,
            )
            _upsert_ingest_watermarks(
                conn=conn,
                schema=safe_schema,
                dataset_type="perp",
                watermark_by_series=ohlcv_watermarks,
            )
            _upsert_ingest_watermarks(
                conn=conn,
                schema=safe_schema,
                dataset_type="oi_m1_feature",
                watermark_by_series=oi_watermarks,
            )
            _upsert_ingest_watermarks(
                conn=conn,
                schema=safe_schema,
                dataset_type="funding",
                watermark_by_series=funding_watermarks,
            )

    return {
        "schema": safe_schema,
        "ohlcv_rows": ohlcv_count,
        "oi_rows": oi_count,
        "funding_rows": funding_count,
        "ohlcv_files": len(ohlcv_files),
        "oi_files": len(oi_files),
        "funding_files": len(funding_files),
        "ohlcv_scanned_rows": ohlcv_scanned,
        "oi_scanned_rows": oi_scanned,
        "funding_scanned_rows": funding_scanned,
        "ohlcv_skipped_rows": ohlcv_skipped,
        "oi_skipped_rows": oi_skipped,
        "funding_skipped_rows": funding_skipped,
    }
