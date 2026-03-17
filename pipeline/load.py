"""
Data loading module.

Loads extracted CSV files into the warehouse (DuckDB for local dev, BigQuery
for production). Handles schema creation, append-only loading with metadata
columns, and idempotent reloads.

Usage:
    python -m pipeline.load
    python -m pipeline.load --warehouse bigquery --data-dir /path/to/data
"""

from __future__ import annotations

import csv
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of loading a single source into the warehouse."""

    source_name: str
    table_name: str
    row_count: int
    duration_seconds: float
    success: bool
    error: str | None = None


class WarehouseBackend(Protocol):
    """Protocol for warehouse backends (DuckDB, BigQuery, etc.)."""

    def create_schema(self, schema_name: str) -> None: ...
    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None: ...
    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int: ...
    def row_count(self, schema: str, table: str) -> int: ...
    def close(self) -> None: ...


class DuckDBBackend:
    """DuckDB warehouse backend for local development.

    Uses an in-process DuckDB database, which requires zero infrastructure
    setup. Data persists to a local file for re-runs.
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
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.debug("Ensured schema exists: %s", schema_name)

    def create_table(self, schema: str, table: str, columns: list[dict[str, str]]) -> None:
        col_defs = ", ".join(f"{c['name']} {c['type']}" for c in columns)
        # Add metadata columns
        col_defs += ", _loaded_at TIMESTAMP DEFAULT current_timestamp"

        fqn = f"{schema}.{table}"
        self._conn.execute(f"CREATE TABLE IF NOT EXISTS {fqn} ({col_defs})")
        logger.debug("Ensured table exists: %s", fqn)

    def load_rows(self, schema: str, table: str, rows: list[dict[str, Any]]) -> int:
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
        fqn = f"{schema}.{table}"
        result = self._conn.execute(f"SELECT COUNT(*) FROM {fqn}").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        self._conn.close()
        logger.debug("Closed DuckDB connection")


# Column type mappings per source
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
        elif col_type == "FLOAT" or col_type == "NUMERIC":
            result[col_name] = float(value)
        else:
            result[col_name] = value

    return result


def get_backend(warehouse: str = "duckdb", **kwargs: Any) -> WarehouseBackend:
    """Factory to create warehouse backend.

    Args:
        warehouse: Backend type ("duckdb" or "bigquery").
        **kwargs: Additional backend-specific configuration.

    Returns:
        Configured warehouse backend instance.
    """
    if warehouse == "duckdb":
        db_path = kwargs.get("db_path", "data/pipeline.duckdb")
        return DuckDBBackend(db_path=db_path)
    elif warehouse == "bigquery":
        raise NotImplementedError(
            "BigQuery backend not implemented in demo. "
            "In production, this would use google-cloud-bigquery to load via "
            "WRITE_APPEND with a _loaded_at partition column."
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
        actual_count = backend.row_count(schema_name, source_name)
        if actual_count != loaded_count:
            logger.warning(
                "Row count mismatch for %s: loaded %d, found %d",
                source_name,
                loaded_count,
                actual_count,
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
        choices=["duckdb", "bigquery"],
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
