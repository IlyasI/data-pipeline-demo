"""
Data loading module.

Loads extracted data into warehouse backends with support for multiple
strategies and verification. Backends include:

- DuckDB (local development, zero-setup)
- BigQuery (streaming inserts and batch load via GCS)
- PostgreSQL (upsert with ON CONFLICT resolution)
- Parquet files (for data lake / archive patterns)

Each backend implements the WarehouseBackend protocol, making it easy
to swap backends without changing pipeline logic. Load operations are
idempotent: re-running a load replaces the data rather than duplicating it.

Usage:
    python -m pipeline.load
    python -m pipeline.load --warehouse bigquery --data-dir /path/to/data
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LoadResult:
    """Immutable result of loading a single source into the warehouse.

    Attributes:
        source_name: Logical name of the data source.
        table_name: Fully qualified table name (schema.table).
        row_count: Number of rows loaded.
        duration_seconds: Wall-clock time for the load.
        success: Whether the load completed without errors.
        error: Error message if load failed.
        checksum: Optional row-content checksum for verification.
    """

    source_name: str
    table_name: str
    row_count: int
    duration_seconds: float
    success: bool
    error: str | None = None
    checksum: str | None = None


@dataclass
class LoadVerification:
    """Result of post-load verification checks.

    Attributes:
        table_name: Table that was verified.
        expected_rows: Number of rows loaded.
        actual_rows: Number of rows found in the table.
        rows_match: Whether expected and actual match.
        checksum_match: Whether content checksums match (if available).
        message: Human-readable verification summary.
    """

    table_name: str
    expected_rows: int
    actual_rows: int
    rows_match: bool
    checksum_match: bool | None = None
    message: str = ""


# ---------------------------------------------------------------------------
# Backend protocol
# ---------------------------------------------------------------------------


class WarehouseBackend(Protocol):
    """Protocol for warehouse backends.

    Any backend that implements these methods can be used by the loader.
    This allows swapping DuckDB for BigQuery or PostgreSQL without changing
    the pipeline logic.
    """

    def create_schema(self, schema_name: str) -> None: ...
    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None: ...
    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int: ...
    def row_count(self, schema: str, table: str) -> int: ...
    def close(self) -> None: ...


# ---------------------------------------------------------------------------
# DuckDB backend
# ---------------------------------------------------------------------------


class DuckDBBackend:
    """DuckDB warehouse backend for local development.

    Uses an in-process DuckDB database, which requires zero infrastructure
    setup. Data persists to a local file for re-runs. Loads are idempotent:
    each load clears existing data before inserting.

    Args:
        db_path: Path to the DuckDB database file.
    """

    def __init__(self, db_path: str = "data/pipeline.duckdb") -> None:
        try:
            import duckdb
        except ImportError:
            raise ImportError(
                "duckdb is required for local development. Install with: pip install duckdb"
            )

        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(db_path)
        logger.info("Connected to DuckDB at %s", db_path)

    def create_schema(self, schema_name: str) -> None:
        """Create a schema if it does not exist."""
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.debug("Ensured schema exists: %s", schema_name)

    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None:
        """Create a table if it does not exist, with a _loaded_at metadata column."""
        col_defs = ", ".join(f"{c['name']} {c['type']}" for c in columns)
        col_defs += ", _loaded_at TIMESTAMP DEFAULT current_timestamp"

        fqn = f"{schema}.{table}"
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {fqn} ({col_defs})")
        logger.debug("Ensured table exists: %s", fqn)

    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
        """Load rows into a table. Clears existing data for idempotent reload."""
        if not rows:
            return 0

        fqn = f"{schema}.{table}"
        loaded_at = datetime.now(timezone.utc).isoformat()

        # Clear existing data for idempotent reload
        self._conn.execute(f"DELETE FROM {fqn}")

        columns = list(rows[0].keys())
        placeholders = ", ".join(["?"] * (len(columns) + 1))
        col_names = ", ".join(columns) + ", _loaded_at"
        insert_sql = f"INSERT INTO {fqn} ({col_names}) VALUES ({placeholders})"

        for row in rows:
            values = [row[col] for col in columns] + [loaded_at]
            self._conn.execute(insert_sql, values)

        logger.info("Loaded %d rows into %s", len(rows), fqn)
        return len(rows)

    def row_count(self, schema: str, table: str) -> int:
        """Return the number of rows in a table."""
        fqn = f"{schema}.{table}"
        result = self._conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
        logger.debug("Closed DuckDB connection")


# ---------------------------------------------------------------------------
# BigQuery backend
# ---------------------------------------------------------------------------


class BigQueryBackend:
    """BigQuery warehouse backend for production workloads.

    Supports two loading strategies:
    - Streaming inserts: Low-latency, row-by-row (best for small batches)
    - Batch load via GCS: High-throughput (best for large datasets)

    The batch strategy stages data as newline-delimited JSON in GCS,
    then uses a BigQuery load job for atomicity and performance.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset name.
        location: BigQuery dataset location (default: US).
        use_streaming: Use streaming inserts instead of batch load.
    """

    def __init__(
        self,
        project: str,
        dataset: str,
        location: str = "US",
        use_streaming: bool = False,
    ) -> None:
        try:
            from google.cloud import bigquery
        except ImportError:
            raise ImportError(
                "google-cloud-bigquery is required. "
                "Install with: pip install google-cloud-bigquery"
            )

        self._client = bigquery.Client(project=project, location=location)
        self._dataset = dataset
        self._use_streaming = use_streaming
        self._bq = bigquery

        logger.info("Connected to BigQuery: %s.%s (location=%s)", project, dataset, location)

    def create_schema(self, schema_name: str) -> None:
        """Create dataset if it does not exist (BigQuery datasets = schemas)."""
        dataset_ref = self._client.dataset(self._dataset)
        try:
            self._client.get_dataset(dataset_ref)
        except Exception:
            dataset = self._bq.Dataset(dataset_ref)
            dataset.location = self._client.location
            self._client.create_dataset(dataset, exists_ok=True)
            logger.info("Created BigQuery dataset: %s", self._dataset)

    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None:
        """Create a BigQuery table if it does not exist."""
        type_mapping = {
            "INTEGER": "INT64",
            "VARCHAR": "STRING",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
            "FLOAT": "FLOAT64",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOL",
        }

        bq_fields = []
        for col in columns:
            bq_type = type_mapping.get(col["type"].upper(), "STRING")
            bq_fields.append(self._bq.SchemaField(col["name"], bq_type))

        # Add metadata column
        bq_fields.append(self._bq.SchemaField("_loaded_at", "TIMESTAMP"))

        table_ref = self._client.dataset(self._dataset).table(table)
        bq_table = self._bq.Table(table_ref, schema=bq_fields)

        # Partition by _loaded_at for efficient querying
        bq_table.time_partitioning = self._bq.TimePartitioning(
            type_=self._bq.TimePartitioningType.DAY,
            field="_loaded_at",
        )

        self._client.create_table(bq_table, exists_ok=True)
        logger.debug("Ensured BigQuery table exists: %s.%s", self._dataset, table)

    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
        """Load rows into BigQuery using streaming inserts or batch load."""
        if not rows:
            return 0

        loaded_at = datetime.now(timezone.utc).isoformat()
        enriched_rows = [{**row, "_loaded_at": loaded_at} for row in rows]

        if self._use_streaming:
            return self._streaming_insert(table, enriched_rows)
        return self._batch_load(table, enriched_rows)

    def _streaming_insert(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Insert rows via BigQuery streaming API."""
        table_ref = self._client.dataset(self._dataset).table(table)

        # Streaming insert in batches of 500 (BigQuery limit is 10,000)
        batch_size = 500
        total_errors = 0

        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            errors = self._client.insert_rows_json(table_ref, batch)
            if errors:
                total_errors += len(errors)
                logger.error("Streaming insert errors: %s", errors[:5])

        if total_errors > 0:
            logger.warning("Streaming insert completed with %d errors", total_errors)

        return len(rows) - total_errors

    def _batch_load(self, table: str, rows: list[dict[str, Any]]) -> int:
        """Load rows via BigQuery batch load job (NDJSON)."""
        table_ref = self._client.dataset(self._dataset).table(table)

        job_config = self._bq.LoadJobConfig(
            source_format=self._bq.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=self._bq.WriteDisposition.WRITE_TRUNCATE,
        )

        # Convert to NDJSON bytes
        ndjson = "\n".join(json.dumps(row, default=str) for row in rows)
        import io

        data = io.BytesIO(ndjson.encode("utf-8"))

        job = self._client.load_table_from_file(
            data,
            table_ref,
            job_config=job_config,
        )
        job.result()  # Wait for completion

        logger.info("BigQuery batch load completed: %d rows", len(rows))
        return len(rows)

    def row_count(self, schema: str, table: str) -> int:
        """Return the number of rows in a table."""
        query = f"SELECT COUNT(*) as cnt FROM `{self._dataset}.{table}`"
        result = self._client.query(query).result()
        for row in result:
            return row.cnt
        return 0

    def close(self) -> None:
        """Close the BigQuery client."""
        self._client.close()
        logger.debug("Closed BigQuery client")


# ---------------------------------------------------------------------------
# PostgreSQL backend
# ---------------------------------------------------------------------------


class PostgreSQLBackend:
    """PostgreSQL warehouse backend with upsert support.

    Uses ON CONFLICT for upsert operations, allowing idempotent loads
    without full table replacement. Supports configurable conflict
    resolution (update or ignore).

    Args:
        connection_params: PostgreSQL connection parameters.
        conflict_action: What to do on primary key conflict
            ("update" or "nothing").
    """

    def __init__(
        self,
        connection_params: dict[str, Any],
        conflict_action: str = "update",
    ) -> None:
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL. "
                "Install with: pip install psycopg2-binary"
            )

        self._conn = psycopg2.connect(**connection_params)
        self._conn.autocommit = False
        self._conflict_action = conflict_action
        logger.info("Connected to PostgreSQL: %s", connection_params.get("host", "localhost"))

    def create_schema(self, schema_name: str) -> None:
        """Create a PostgreSQL schema if it does not exist."""
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        self._conn.commit()

    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None:
        """Create a PostgreSQL table if it does not exist."""
        type_mapping = {
            "INTEGER": "INTEGER",
            "VARCHAR": "TEXT",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMPTZ",
            "FLOAT": "DOUBLE PRECISION",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
        }

        col_defs = ", ".join(
            f"{c['name']} {type_mapping.get(c['type'].upper(), 'TEXT')}" for c in columns
        )
        col_defs += ", _loaded_at TIMESTAMPTZ DEFAULT NOW()"

        fqn = f"{schema}.{table}"
        with self._conn.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {fqn} ({col_defs})")
        self._conn.commit()

    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
        """Load rows with upsert (ON CONFLICT) support.

        If conflict_action is "update", conflicting rows are updated
        with the new values. If "nothing", conflicts are silently skipped.
        """
        if not rows:
            return 0

        fqn = f"{schema}.{table}"
        loaded_at = datetime.now(timezone.utc).isoformat()

        columns = list(rows[0].keys())
        col_names = ", ".join(columns) + ", _loaded_at"
        placeholders = ", ".join(["%s"] * (len(columns) + 1))

        # Build upsert SQL
        if self._conflict_action == "update" and columns:
            # Assumes first column is the primary key
            pk = columns[0]
            update_cols = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns[1:])
            update_cols += ", _loaded_at = EXCLUDED._loaded_at"
            sql = (
                f"INSERT INTO {fqn} ({col_names}) VALUES ({placeholders}) "
                f"ON CONFLICT ({pk}) DO UPDATE SET {update_cols}"
            )
        else:
            sql = (
                f"INSERT INTO {fqn} ({col_names}) VALUES ({placeholders}) "
                f"ON CONFLICT DO NOTHING"
            )

        with self._conn.cursor() as cur:
            for row in rows:
                values = [row.get(col) for col in columns] + [loaded_at]
                cur.execute(sql, values)

        self._conn.commit()
        logger.info("Loaded %d rows into %s (upsert: %s)", len(rows), fqn, self._conflict_action)
        return len(rows)

    def row_count(self, schema: str, table: str) -> int:
        """Return the number of rows in a table."""
        fqn = f"{schema}.{table}"
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {fqn}")
            result = cur.fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        """Close the PostgreSQL connection."""
        self._conn.close()
        logger.debug("Closed PostgreSQL connection")


# ---------------------------------------------------------------------------
# Parquet writer
# ---------------------------------------------------------------------------


class ParquetWriter:
    """Write data as Parquet files for data lake / archive patterns.

    Creates partitioned Parquet files with metadata. Useful for feeding
    downstream analytics tools (Spark, Presto, Athena) or as a
    long-term archive format.

    Args:
        output_dir: Directory to write Parquet files.
        compression: Parquet compression codec (snappy, gzip, zstd).
    """

    def __init__(
        self,
        output_dir: Path,
        compression: str = "snappy",
    ) -> None:
        self.output_dir = output_dir
        self.compression = compression

    def write(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        partition_col: str | None = None,
    ) -> LoadResult:
        """Write rows to a Parquet file.

        Args:
            table_name: Logical table name (used as filename).
            rows: Records to write.
            partition_col: Optional column to partition by.

        Returns:
            LoadResult with write metadata.
        """
        start_time = time.monotonic()

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            return LoadResult(
                source_name=table_name,
                table_name=table_name,
                row_count=0,
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error="pyarrow is required. Install with: pip install pyarrow",
            )

        if not rows:
            return LoadResult(
                source_name=table_name,
                table_name=table_name,
                row_count=0,
                duration_seconds=time.monotonic() - start_time,
                success=True,
            )

        self.output_dir.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(rows)

        output_path = self.output_dir / f"{table_name}.parquet"

        if partition_col and partition_col in table.column_names:
            pq.write_to_dataset(
                table,
                root_path=str(self.output_dir / table_name),
                partition_cols=[partition_col],
                compression=self.compression,
            )
        else:
            pq.write_table(table, output_path, compression=self.compression)

        duration = time.monotonic() - start_time
        logger.info(
            "Wrote %d rows to %s (%.2fs, compression=%s)",
            len(rows),
            output_path,
            duration,
            self.compression,
        )

        return LoadResult(
            source_name=table_name,
            table_name=str(output_path),
            row_count=len(rows),
            duration_seconds=duration,
            success=True,
        )


# ---------------------------------------------------------------------------
# Load verification
# ---------------------------------------------------------------------------


def verify_load(
    backend: WarehouseBackend,
    schema: str,
    table: str,
    expected_rows: int,
) -> LoadVerification:
    """Verify that a load completed correctly.

    Checks that the actual row count in the warehouse matches the
    expected row count from the load operation.

    Args:
        backend: Warehouse backend to check.
        schema: Schema name.
        table: Table name.
        expected_rows: Number of rows that were loaded.

    Returns:
        LoadVerification with the verification results.
    """
    actual_rows = backend.row_count(schema, table)
    rows_match = actual_rows == expected_rows

    message = (
        f"Load verification for {schema}.{table}: "
        f"expected {expected_rows} rows, found {actual_rows}"
    )

    if not rows_match:
        logger.warning(message)
    else:
        logger.info(message)

    return LoadVerification(
        table_name=f"{schema}.{table}",
        expected_rows=expected_rows,
        actual_rows=actual_rows,
        rows_match=rows_match,
        message=message,
    )


# ---------------------------------------------------------------------------
# Column type mappings
# ---------------------------------------------------------------------------


SOURCE_SCHEMAS: dict[str, list[dict[str, str]]] = {
    "orders": [
        {"name": "id", "type": "INTEGER"},
        {"name": "user_id", "type": "INTEGER"},
        {"name": "order_date", "type": "DATE"},
        {"name": "status", "type": "VARCHAR"},
    ],
    "customers": [
        {"name": "id", "type": "INTEGER"},
        {"name": "first_name", "type": "VARCHAR"},
        {"name": "last_name", "type": "VARCHAR"},
    ],
    "payments": [
        {"name": "id", "type": "INTEGER"},
        {"name": "order_id", "type": "INTEGER"},
        {"name": "payment_method", "type": "VARCHAR"},
        {"name": "amount", "type": "INTEGER"},
    ],
}


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------


def _read_csv(file_path: Path) -> list[dict[str, str]]:
    """Read a CSV file and return rows as list of dicts."""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    logger.debug("Read %d rows from %s", len(rows), file_path)
    return rows


def _cast_row(row: dict[str, str], schema: list[dict[str, str]]) -> dict[str, Any]:
    """Cast string values from CSV to appropriate Python types.

    Args:
        row: Raw string values from CSV reader.
        schema: Column definitions with type information.

    Returns:
        Row with values cast to correct types.
    """
    type_map = {col["name"]: col["type"] for col in schema}
    result: dict[str, Any] = {}

    for col_name, value in row.items():
        col_type = type_map.get(col_name, "VARCHAR")

        if value == "" or value is None:
            result[col_name] = None
        elif col_type == "INTEGER":
            result[col_name] = int(value)
        elif col_type in ("FLOAT", "NUMERIC"):
            result[col_name] = float(value)
        else:
            result[col_name] = value

    return result


def get_backend(warehouse: str = "duckdb", **kwargs: Any) -> WarehouseBackend:
    """Factory to create warehouse backend.

    Args:
        warehouse: Backend type ("duckdb", "bigquery", or "postgresql").
        **kwargs: Additional backend-specific configuration.

    Returns:
        Configured warehouse backend instance.
    """
    if warehouse == "duckdb":
        db_path = kwargs.get("db_path", "data/pipeline.duckdb")
        return DuckDBBackend(db_path=db_path)
    elif warehouse == "bigquery":
        return BigQueryBackend(
            project=kwargs.get("project", os.getenv("GCP_PROJECT", "")),
            dataset=kwargs.get("dataset", os.getenv("BQ_DATASET", "pipeline_demo")),
            location=kwargs.get("location", os.getenv("BQ_LOCATION", "US")),
            use_streaming=kwargs.get("use_streaming", False),
        )
    elif warehouse == "postgresql":
        return PostgreSQLBackend(
            connection_params=kwargs.get(
                "connection_params",
                {
                    "host": os.getenv("PG_HOST", "localhost"),
                    "port": int(os.getenv("PG_PORT", "5432")),
                    "dbname": os.getenv("PG_DATABASE", "pipeline"),
                    "user": os.getenv("PG_USER", "pipeline"),
                    "password": os.getenv("PG_PASSWORD", ""),
                },
            ),
            conflict_action=kwargs.get("conflict_action", "update"),
        )
    else:
        raise ValueError(f"Unknown warehouse backend: {warehouse}")


def load_source(
    source_name: str,
    file_path: Path,
    backend: WarehouseBackend,
    schema_name: str = "raw",
) -> LoadResult:
    """Load a single source file into the warehouse.

    Args:
        source_name: Logical name of the source.
        file_path: Path to the CSV file.
        backend: Warehouse backend to load into.
        schema_name: Target schema name.

    Returns:
        LoadResult with metadata about the load.
    """
    start_time = time.monotonic()

    try:
        if not file_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        source_schema = SOURCE_SCHEMAS.get(source_name)
        if source_schema is None:
            raise ValueError(f"No schema defined for source: {source_name}")

        # Create schema and table
        backend.create_schema(schema_name)
        backend.create_table(schema_name, source_name, source_schema)

        # Read and cast data
        raw_rows = _read_csv(file_path)
        typed_rows = [_cast_row(row, source_schema) for row in raw_rows]

        # Load
        loaded_count = backend.load_rows(schema_name, source_name, typed_rows)

        # Verify
        verification = verify_load(backend, schema_name, source_name, loaded_count)
        if not verification.rows_match:
            logger.warning(
                "Row count mismatch for %s: loaded %d, found %d",
                source_name,
                loaded_count,
                verification.actual_rows,
            )

        duration = time.monotonic() - start_time
        logger.info(
            "Loaded %s: %d rows into %s.%s in %.2fs",
            source_name,
            loaded_count,
            schema_name,
            source_name,
            duration,
        )

        return LoadResult(
            source_name=source_name,
            table_name=f"{schema_name}.{source_name}",
            row_count=loaded_count,
            duration_seconds=duration,
            success=True,
        )

    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.error("Failed to load %s: %s", source_name, str(exc))
        return LoadResult(
            source_name=source_name,
            table_name=f"{schema_name}.{source_name}",
            row_count=0,
            duration_seconds=duration,
            success=False,
            error=str(exc),
        )


def load_all(
    data_dir: Path | None = None,
    warehouse: str = "duckdb",
) -> list[LoadResult]:
    """Load all extracted sources into the warehouse.

    Args:
        data_dir: Directory containing extracted CSV files. Defaults to data/raw.
        warehouse: Backend type to use.

    Returns:
        List of LoadResult objects.
    """
    if data_dir is None:
        data_dir = Path("data/raw")

    backend = get_backend(warehouse)
    results: list[LoadResult] = []

    try:
        for source_name in SOURCE_SCHEMAS:
            file_path = data_dir / f"{source_name}.csv"
            result = load_source(source_name, file_path, backend)
            results.append(result)
    finally:
        backend.close()

    succeeded = sum(1 for r in results if r.success)
    total_rows = sum(r.row_count for r in results)
    logger.info(
        "Load complete: %d/%d succeeded, %d total rows",
        succeeded,
        len(results),
        total_rows,
    )

    return results


def main() -> None:
    """CLI entrypoint for loading."""
    import argparse

    parser = argparse.ArgumentParser(description="Load extracted data into warehouse")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing extracted CSV files (default: data/raw)",
    )
    parser.add_argument(
        "--warehouse",
        choices=["duckdb", "bigquery", "postgresql"],
        default=os.getenv("PIPELINE_WAREHOUSE", "duckdb"),
        help="Warehouse backend (default: duckdb)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    results = load_all(data_dir=args.data_dir, warehouse=args.warehouse)
    failed = [r for r in results if not r.success]
    if failed:
        raise SystemExit(f"Load failed for: {', '.join(r.source_name for r in failed)}")


if __name__ == "__main__":
    main()
