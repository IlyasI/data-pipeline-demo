"""
Data extraction module.

Downloads sample e-commerce data from the dbt jaffle-shop dataset.
Handles retries, validation, and idempotent downloads.

Usage:
    python -m pipeline.extract
    python -m pipeline.extract --output-dir /path/to/data
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)

# dbt's canonical sample data
SOURCES: dict[str, str] = {
    "orders": (
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv"
    ),
    "customers": (
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv"
    ),
    "payments": (
        "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv"
    ),
}

# Expected schemas for validation
EXPECTED_COLUMNS: dict[str, list[str]] = {
    "orders": ["id", "user_id", "order_date", "status"],
    "customers": ["id", "first_name", "last_name"],
    "payments": ["id", "order_id", "payment_method", "amount"],
}


@dataclass
class ExtractionResult:
    """Result of a single source extraction."""

    source_name: str
    file_path: Path
    row_count: int
    byte_count: int
    checksum: str
    duration_seconds: float
    success: bool
    error: str | None = None
    columns: list[str] = field(default_factory=list)


def _download_with_retry(
    url: str,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    timeout: int = 30,
) -> str:
    """Download a URL with exponential backoff retry.

    Args:
        url: URL to download.
        max_retries: Maximum number of retry attempts.
        backoff_factor: Multiplier for exponential backoff.
        timeout: Request timeout in seconds.

    Returns:
        Response text content.

    Raises:
        requests.RequestException: If all retries are exhausted.
    """
    last_exception: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            logger.debug("Downloading %s (attempt %d/%d)", url, attempt + 1, max_retries + 1)
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text

        except requests.RequestException as exc:
            last_exception = exc
            if attempt < max_retries:
                wait_time = backoff_factor * (2**attempt)
                logger.warning(
                    "Download failed (attempt %d/%d): %s. Retrying in %.1fs...",
                    attempt + 1,
                    max_retries + 1,
                    str(exc),
                    wait_time,
                )
                time.sleep(wait_time)
            else:
                logger.error("Download failed after %d attempts: %s", max_retries + 1, str(exc))

    raise last_exception  # type: ignore[misc]


def _validate_csv(content: str, source_name: str) -> tuple[list[dict[str, str]], list[str]]:
    """Validate CSV content against expected schema.

    Args:
        content: Raw CSV string.
        source_name: Name of the source for schema lookup.

    Returns:
        Tuple of (parsed rows, column names).

    Raises:
        ValueError: If schema validation fails.
    """
    reader = csv.DictReader(io.StringIO(content))
    columns = reader.fieldnames or []

    expected = EXPECTED_COLUMNS.get(source_name, [])
    missing_columns = set(expected) - set(columns)
    if missing_columns:
        raise ValueError(
            f"Schema validation failed for {source_name}: "
            f"missing columns {missing_columns}. Got: {columns}"
        )

    rows = list(reader)

    if not rows:
        raise ValueError(f"No data rows found in {source_name}")

    logger.info(
        "Validated %s: %d rows, %d columns (%s)",
        source_name,
        len(rows),
        len(columns),
        ", ".join(columns),
    )
    return rows, list(columns)


def _compute_checksum(content: str) -> str:
    """Compute SHA-256 checksum of content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def extract_source(
    source_name: str,
    url: str,
    output_dir: Path,
    max_retries: int = 3,
) -> ExtractionResult:
    """Extract a single data source.

    Downloads the CSV, validates the schema, writes to disk, and returns
    metadata about the extraction.

    Args:
        source_name: Logical name of the source (e.g., "orders").
        url: URL to download from.
        output_dir: Directory to write the CSV file to.
        max_retries: Number of retry attempts for download.

    Returns:
        ExtractionResult with metadata about the extraction.
    """
    start_time = time.monotonic()
    output_path = output_dir / f"{source_name}.csv"

    try:
        # Download
        content = _download_with_retry(url, max_retries=max_retries)

        # Validate
        rows, columns = _validate_csv(content, source_name)

        # Checksum for idempotency tracking
        checksum = _compute_checksum(content)

        # Check if file already exists with same checksum
        if output_path.exists():
            existing_checksum = _compute_checksum(output_path.read_text())
            if existing_checksum == checksum:
                logger.info("Skipping %s: file unchanged (checksum %s)", source_name, checksum)
                return ExtractionResult(
                    source_name=source_name,
                    file_path=output_path,
                    row_count=len(rows),
                    byte_count=len(content.encode("utf-8")),
                    checksum=checksum,
                    duration_seconds=time.monotonic() - start_time,
                    success=True,
                    columns=columns,
                )

        # Write
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content)

        duration = time.monotonic() - start_time
        logger.info(
            "Extracted %s: %d rows, %d bytes, checksum=%s, %.2fs",
            source_name,
            len(rows),
            len(content.encode("utf-8")),
            checksum,
            duration,
        )

        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=len(rows),
            byte_count=len(content.encode("utf-8")),
            checksum=checksum,
            duration_seconds=duration,
            success=True,
            columns=columns,
        )

    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.error("Failed to extract %s: %s", source_name, str(exc))
        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=0,
            byte_count=0,
            checksum="",
            duration_seconds=duration,
            success=False,
            error=str(exc),
        )


def extract_all(
    output_dir: Path | None = None,
    sources: dict[str, str] | None = None,
) -> list[ExtractionResult]:
    """Extract all configured data sources.

    Args:
        output_dir: Directory to write CSV files. Defaults to ./data/raw/.
        sources: Override source URLs. Defaults to SOURCES.

    Returns:
        List of ExtractionResult objects.
    """
    if output_dir is None:
        output_dir = Path("data/raw")
    if sources is None:
        sources = SOURCES

    logger.info("Starting extraction of %d sources to %s", len(sources), output_dir)
    results: list[ExtractionResult] = []

    for name, url in sources.items():
        result = extract_source(name, url, output_dir)
        results.append(result)

    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)
    total_rows = sum(r.row_count for r in results)
    total_duration = sum(r.duration_seconds for r in results)

    logger.info(
        "Extraction complete: %d/%d succeeded, %d total rows, %.2fs total",
        succeeded,
        len(results),
        total_rows,
        total_duration,
    )

    if failed > 0:
        failed_sources = [r.source_name for r in results if not r.success]
        logger.error("Failed sources: %s", ", ".join(failed_sources))

    return results


def main() -> None:
    """CLI entrypoint for extraction."""
    import argparse

    parser = argparse.ArgumentParser(description="Extract pipeline source data")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory to write extracted CSV files (default: data/raw)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    results = extract_all(output_dir=args.output_dir)
    failed = [r for r in results if not r.success]
    if failed:
        raise SystemExit(f"Extraction failed for: {', '.join(r.source_name for r in failed)}")


if __name__ == "__main__":
    main()
