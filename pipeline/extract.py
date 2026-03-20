"""
Data extraction module.

Provides a generic extractor framework with concrete implementations for:
- REST APIs (paginated, rate-limited, with exponential backoff)
- Databases (PostgreSQL, BigQuery)
- Files (CSV, JSON, Parquet)

Each extractor returns an ExtractionResult with metadata for downstream
tracking and monitoring. The framework is designed around composition:
extractors share retry and logging behavior through the base class,
while each subclass handles source-specific logic.

Usage:
    # REST API extraction
    extractor = RestApiExtractor(
        base_url="https://api.example.com",
        endpoints={"users": "/v1/users"},
    )
    results = extractor.extract_all(output_dir=Path("data/raw"))

    # Simple CSV download (backward-compatible)
    results = extract_all(output_dir=Path("data/raw"))
"""

from __future__ import annotations

import abc
import csv
import hashlib
import io
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractionResult:
    """Immutable result of a single source extraction.

    Attributes:
        source_name: Logical name of the data source.
        file_path: Where the extracted data was written.
        row_count: Number of rows extracted.
        byte_count: Size of the extracted data in bytes.
        checksum: SHA-256 checksum (first 16 hex chars) for deduplication.
        duration_seconds: Wall-clock time for the extraction.
        success: Whether the extraction completed without errors.
        error: Error message if extraction failed.
        columns: Column names found in the extracted data.
        extracted_at: UTC timestamp when extraction completed.
    """

    source_name: str
    file_path: Path
    row_count: int
    byte_count: int
    checksum: str
    duration_seconds: float
    success: bool
    error: str | None = None
    columns: list[str] = field(default_factory=list)
    extracted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class RateLimitConfig:
    """Configuration for API rate limiting.

    Attributes:
        requests_per_second: Maximum requests per second.
        burst_size: Maximum burst of requests before throttling.
        retry_after_header: HTTP header to check for retry-after hints.
    """

    requests_per_second: float = 10.0
    burst_size: int = 20
    retry_after_header: str = "Retry-After"


# ---------------------------------------------------------------------------
# Base extractor
# ---------------------------------------------------------------------------


class BaseExtractor(abc.ABC):
    """Abstract base class for all data extractors.

    Provides shared infrastructure: retry logic with exponential backoff,
    structured logging, checksum computation, and result tracking. Subclasses
    implement _extract_source() for source-specific logic.

    Args:
        max_retries: Maximum retry attempts per source.
        backoff_factor: Multiplier for exponential backoff between retries.
        timeout: Default timeout in seconds for I/O operations.
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
    ) -> None:
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout

    @abc.abstractmethod
    def _extract_source(
        self,
        source_name: str,
        output_dir: Path,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract a single data source. Implemented by subclasses."""

    def extract_with_retry(
        self,
        source_name: str,
        output_dir: Path,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract a source with exponential backoff retry.

        Each failed attempt waits backoff_factor * 2^attempt seconds before
        retrying. The final attempt's error is preserved in the result.

        Args:
            source_name: Logical name of the source.
            output_dir: Directory to write extracted data.
            **kwargs: Passed through to _extract_source.

        Returns:
            ExtractionResult with success=True on any successful attempt,
            or success=False with the last error after all retries exhausted.
        """
        last_error: str | None = None

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                wait_time = self.backoff_factor * (2 ** (attempt - 1))
                logger.warning(
                    "Retrying %s (attempt %d/%d) after %.1fs...",
                    source_name,
                    attempt + 1,
                    self.max_retries + 1,
                    wait_time,
                )
                time.sleep(wait_time)

            try:
                result = self._extract_source(source_name, output_dir, **kwargs)
                if result.success:
                    return result
                last_error = result.error
            except Exception as exc:
                last_error = str(exc)
                logger.error(
                    "Extract %s failed (attempt %d/%d): %s",
                    source_name,
                    attempt + 1,
                    self.max_retries + 1,
                    last_error,
                )

        return ExtractionResult(
            source_name=source_name,
            file_path=output_dir / f"{source_name}.csv",
            row_count=0,
            byte_count=0,
            checksum="",
            duration_seconds=0.0,
            success=False,
            error=f"Failed after {self.max_retries + 1} attempts: {last_error}",
        )

    @staticmethod
    def compute_checksum(content: str | bytes) -> str:
        """Compute a truncated SHA-256 checksum.

        Args:
            content: String or bytes to hash.

        Returns:
            First 16 hex characters of the SHA-256 digest.
        """
        if isinstance(content, str):
            content = content.encode("utf-8")
        return hashlib.sha256(content).hexdigest()[:16]


# ---------------------------------------------------------------------------
# REST API extractor
# ---------------------------------------------------------------------------


class RestApiExtractor(BaseExtractor):
    """Extract data from paginated REST APIs.

    Handles pagination (offset, cursor, and link-header styles),
    rate limiting, authentication, and response parsing. Uses a
    requests.Session with connection pooling for efficiency.

    Args:
        base_url: API base URL (e.g., "https://api.example.com").
        endpoints: Mapping of source_name -> endpoint path.
        auth_token: Optional Bearer token for authentication.
        rate_limit: Rate limiting configuration.
        page_size: Number of records per page.
        max_retries: Retry attempts per request.
        backoff_factor: Exponential backoff multiplier.
        timeout: Request timeout in seconds.

    Example:
        >>> extractor = RestApiExtractor(
        ...     base_url="https://api.example.com",
        ...     endpoints={"users": "/v1/users", "events": "/v1/events"},
        ...     auth_token="secret",
        ...     rate_limit=RateLimitConfig(requests_per_second=5),
        ... )
        >>> results = extractor.extract_all(Path("data/raw"))
    """

    def __init__(
        self,
        base_url: str,
        endpoints: dict[str, str],
        auth_token: str | None = None,
        rate_limit: RateLimitConfig | None = None,
        page_size: int = 100,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
    ) -> None:
        super().__init__(max_retries=max_retries, backoff_factor=backoff_factor, timeout=timeout)
        self.base_url = base_url.rstrip("/")
        self.endpoints = endpoints
        self.rate_limit = rate_limit or RateLimitConfig()
        self.page_size = page_size

        # Build a session with connection pooling and transport-level retries
        self._session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=10,
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        if auth_token:
            self._session.headers["Authorization"] = f"Bearer {auth_token}"

        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        """Enforce rate limiting between requests."""
        if self.rate_limit.requests_per_second <= 0:
            return

        min_interval = 1.0 / self.rate_limit.requests_per_second
        elapsed = time.monotonic() - self._last_request_time

        if elapsed < min_interval:
            sleep_time = min_interval - elapsed
            logger.debug("Rate limiting: sleeping %.3fs", sleep_time)
            time.sleep(sleep_time)

        self._last_request_time = time.monotonic()

    def _fetch_page(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Fetch a single page with rate limiting and error handling.

        Respects Retry-After headers from 429 responses. Raises on
        non-recoverable HTTP errors.

        Args:
            url: Full URL to fetch.
            params: Query parameters.

        Returns:
            Response object.

        Raises:
            requests.HTTPError: On non-recoverable HTTP errors.
        """
        self._throttle()

        response = self._session.get(url, params=params, timeout=self.timeout)

        # Handle rate limiting with Retry-After header
        if response.status_code == 429:
            retry_after = response.headers.get(self.rate_limit.retry_after_header, "60")
            wait_seconds = int(retry_after)
            logger.warning("Rate limited. Waiting %ds (Retry-After header).", wait_seconds)
            time.sleep(wait_seconds)
            return self._fetch_page(url, params)

        response.raise_for_status()
        return response

    def _paginate(
        self,
        endpoint: str,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of records from a paginated endpoint.

        Supports offset-based pagination. Each yield is a list of records
        (dicts) from one page. Stops when the API returns fewer records
        than the page size.

        Args:
            endpoint: API endpoint path (appended to base_url).

        Yields:
            List of record dicts for each page.
        """
        url = f"{self.base_url}{endpoint}"
        offset = 0

        while True:
            params = {"limit": self.page_size, "offset": offset}
            logger.debug("Fetching page: %s offset=%d", endpoint, offset)

            response = self._fetch_page(url, params=params)
            data = response.json()

            # Handle common response shapes
            records: list[dict[str, Any]]
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # Try common keys: "data", "results", "items", "records"
                for key in ("data", "results", "items", "records"):
                    if key in data and isinstance(data[key], list):
                        records = data[key]
                        break
                else:
                    records = [data]
            else:
                break

            if not records:
                break

            yield records

            if len(records) < self.page_size:
                break  # Last page

            offset += len(records)

    def _extract_source(
        self,
        source_name: str,
        output_dir: Path,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract a single API endpoint to a JSON file.

        Paginates through the endpoint, collects all records, computes
        a checksum for idempotency, and writes to disk.
        """
        start_time = time.monotonic()
        endpoint = self.endpoints.get(source_name)

        if endpoint is None:
            return ExtractionResult(
                source_name=source_name,
                file_path=output_dir / f"{source_name}.json",
                row_count=0,
                byte_count=0,
                checksum="",
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error=f"No endpoint configured for source: {source_name}",
            )

        all_records: list[dict[str, Any]] = []
        for page in self._paginate(endpoint):
            all_records.extend(page)

        content = json.dumps(all_records, indent=2, default=str)
        checksum = self.compute_checksum(content)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{source_name}.json"

        # Skip write if content unchanged
        if output_path.exists():
            existing_checksum = self.compute_checksum(output_path.read_text())
            if existing_checksum == checksum:
                logger.info("Skipping %s: content unchanged (checksum %s)", source_name, checksum)
                return ExtractionResult(
                    source_name=source_name,
                    file_path=output_path,
                    row_count=len(all_records),
                    byte_count=len(content.encode("utf-8")),
                    checksum=checksum,
                    duration_seconds=time.monotonic() - start_time,
                    success=True,
                    columns=list(all_records[0].keys()) if all_records else [],
                )

        output_path.write_text(content)
        duration = time.monotonic() - start_time

        logger.info(
            "Extracted %s: %d records, %d bytes, checksum=%s, %.2fs",
            source_name,
            len(all_records),
            len(content.encode("utf-8")),
            checksum,
            duration,
        )

        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=len(all_records),
            byte_count=len(content.encode("utf-8")),
            checksum=checksum,
            duration_seconds=duration,
            success=True,
            columns=list(all_records[0].keys()) if all_records else [],
        )

    def extract_all(self, output_dir: Path) -> list[ExtractionResult]:
        """Extract all configured endpoints.

        Args:
            output_dir: Directory to write extracted files.

        Returns:
            List of ExtractionResult objects, one per endpoint.
        """
        results: list[ExtractionResult] = []
        for source_name in self.endpoints:
            result = self.extract_with_retry(source_name, output_dir)
            results.append(result)
        return results


# ---------------------------------------------------------------------------
# Database extractor
# ---------------------------------------------------------------------------


class DatabaseExtractor(BaseExtractor):
    """Extract data from databases (PostgreSQL or BigQuery).

    Uses chunked fetching to handle large tables without loading everything
    into memory. Supports custom SQL queries for extraction.

    This class defines the interface. In production, you would inject a
    real database connection. The extract method writes results as CSV
    for downstream compatibility.

    Args:
        connection_params: Database connection parameters.
        queries: Mapping of source_name -> SQL query.
        chunk_size: Number of rows to fetch per chunk.
        max_retries: Retry attempts per query.
        backoff_factor: Exponential backoff multiplier.
        timeout: Query timeout in seconds.

    Example:
        >>> extractor = DatabaseExtractor(
        ...     connection_params={"host": "localhost", "dbname": "analytics"},
        ...     queries={
        ...         "users": "SELECT * FROM users WHERE updated_at > :since",
        ...         "events": "SELECT * FROM events WHERE date > :since",
        ...     },
        ... )
    """

    def __init__(
        self,
        connection_params: dict[str, Any],
        queries: dict[str, str],
        chunk_size: int = 10_000,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 300,
    ) -> None:
        super().__init__(max_retries=max_retries, backoff_factor=backoff_factor, timeout=timeout)
        self.connection_params = connection_params
        self.queries = queries
        self.chunk_size = chunk_size
        self._connection: Any = None

    def _get_connection(self) -> Any:
        """Get or create a database connection.

        Returns the existing connection if still alive, otherwise creates
        a new one. Supports PostgreSQL (via psycopg2) and BigQuery
        (via google-cloud-bigquery).

        Returns:
            Database connection object.

        Raises:
            ImportError: If the required driver is not installed.
            ConnectionError: If the connection cannot be established.
        """
        if self._connection is not None:
            return self._connection

        db_type = self.connection_params.get("type", "postgresql")

        if db_type == "postgresql":
            try:
                import psycopg2
            except ImportError:
                raise ImportError("psycopg2 is required for PostgreSQL. Install with: pip install psycopg2-binary")

            self._connection = psycopg2.connect(
                host=self.connection_params.get("host", "localhost"),
                port=self.connection_params.get("port", 5432),
                dbname=self.connection_params.get("dbname", ""),
                user=self.connection_params.get("user", ""),
                password=self.connection_params.get("password", ""),
            )

        elif db_type == "bigquery":
            try:
                from google.cloud import bigquery
            except ImportError:
                raise ImportError(
                    "google-cloud-bigquery is required for BigQuery. "
                    "Install with: pip install google-cloud-bigquery"
                )

            self._connection = bigquery.Client(
                project=self.connection_params.get("project"),
            )

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        return self._connection

    def _extract_source(
        self,
        source_name: str,
        output_dir: Path,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract a single query result to CSV.

        Executes the configured SQL query, streams results in chunks,
        and writes to a CSV file.
        """
        start_time = time.monotonic()
        query = self.queries.get(source_name)

        if query is None:
            return ExtractionResult(
                source_name=source_name,
                file_path=output_dir / f"{source_name}.csv",
                row_count=0,
                byte_count=0,
                checksum="",
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error=f"No query configured for source: {source_name}",
            )

        conn = self._get_connection()
        db_type = self.connection_params.get("type", "postgresql")

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{source_name}.csv"

        row_count = 0
        columns: list[str] = []

        if db_type == "bigquery":
            # BigQuery: use the client API
            query_job = conn.query(query)
            results = query_job.result()
            columns = [field.name for field in results.schema]

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                for row in results:
                    writer.writerow(dict(row))
                    row_count += 1
        else:
            # PostgreSQL: use server-side cursor for chunked fetching
            cursor = conn.cursor(name=f"extract_{source_name}")
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

                while True:
                    chunk = cursor.fetchmany(self.chunk_size)
                    if not chunk:
                        break
                    for row_tuple in chunk:
                        row_dict = dict(zip(columns, row_tuple))
                        writer.writerow(row_dict)
                        row_count += 1

            cursor.close()

        content = output_path.read_text()
        checksum = self.compute_checksum(content)
        duration = time.monotonic() - start_time

        logger.info(
            "Extracted %s: %d rows from %s, checksum=%s, %.2fs",
            source_name,
            row_count,
            db_type,
            checksum,
            duration,
        )

        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=row_count,
            byte_count=len(content.encode("utf-8")),
            checksum=checksum,
            duration_seconds=duration,
            success=True,
            columns=columns,
        )

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None


# ---------------------------------------------------------------------------
# File extractor
# ---------------------------------------------------------------------------


class FileExtractor(BaseExtractor):
    """Extract data from local or remote files (CSV, JSON, Parquet).

    Reads files from a source directory, validates structure, computes
    checksums, and copies to the output directory. Supports CSV, JSON,
    and Parquet formats with automatic detection based on file extension.

    Args:
        source_paths: Mapping of source_name -> file path or URL.
        max_retries: Retry attempts per file.
        backoff_factor: Exponential backoff multiplier.
        timeout: Timeout for remote file downloads.

    Example:
        >>> extractor = FileExtractor(
        ...     source_paths={
        ...         "users": Path("/data/exports/users.csv"),
        ...         "events": Path("/data/exports/events.parquet"),
        ...     },
        ... )
    """

    def __init__(
        self,
        source_paths: dict[str, Path | str],
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
    ) -> None:
        super().__init__(max_retries=max_retries, backoff_factor=backoff_factor, timeout=timeout)
        self.source_paths = source_paths

    def _extract_source(
        self,
        source_name: str,
        output_dir: Path,
        **kwargs: Any,
    ) -> ExtractionResult:
        """Extract a single file source."""
        start_time = time.monotonic()
        source = self.source_paths.get(source_name)

        if source is None:
            return ExtractionResult(
                source_name=source_name,
                file_path=output_dir / source_name,
                row_count=0,
                byte_count=0,
                checksum="",
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error=f"No path configured for source: {source_name}",
            )

        source_path = Path(source)
        suffix = source_path.suffix.lower()

        if suffix == ".csv":
            return self._extract_csv(source_name, source_path, output_dir, start_time)
        elif suffix == ".json":
            return self._extract_json(source_name, source_path, output_dir, start_time)
        elif suffix == ".parquet":
            return self._extract_parquet(source_name, source_path, output_dir, start_time)
        else:
            return ExtractionResult(
                source_name=source_name,
                file_path=output_dir / source_path.name,
                row_count=0,
                byte_count=0,
                checksum="",
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error=f"Unsupported file format: {suffix}",
            )

    def _extract_csv(
        self,
        source_name: str,
        source_path: Path,
        output_dir: Path,
        start_time: float,
    ) -> ExtractionResult:
        """Extract a CSV file."""
        content = source_path.read_text(encoding="utf-8")
        reader = csv.DictReader(io.StringIO(content))
        columns = list(reader.fieldnames or [])
        rows = list(reader)

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{source_name}.csv"

        checksum = self.compute_checksum(content)

        # Skip if unchanged
        if output_path.exists():
            existing = self.compute_checksum(output_path.read_text())
            if existing == checksum:
                logger.info("Skipping %s: unchanged", source_name)
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

        output_path.write_text(content)

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

    def _extract_json(
        self,
        source_name: str,
        source_path: Path,
        output_dir: Path,
        start_time: float,
    ) -> ExtractionResult:
        """Extract a JSON file (expects array of objects)."""
        content = source_path.read_text(encoding="utf-8")
        data = json.loads(content)
        records = data if isinstance(data, list) else [data]
        columns = list(records[0].keys()) if records else []

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{source_name}.json"
        checksum = self.compute_checksum(content)

        output_path.write_text(content)

        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=len(records),
            byte_count=len(content.encode("utf-8")),
            checksum=checksum,
            duration_seconds=time.monotonic() - start_time,
            success=True,
            columns=columns,
        )

    def _extract_parquet(
        self,
        source_name: str,
        source_path: Path,
        output_dir: Path,
        start_time: float,
    ) -> ExtractionResult:
        """Extract a Parquet file."""
        try:
            import pyarrow.parquet as pq
        except ImportError:
            return ExtractionResult(
                source_name=source_name,
                file_path=output_dir / f"{source_name}.parquet",
                row_count=0,
                byte_count=0,
                checksum="",
                duration_seconds=time.monotonic() - start_time,
                success=False,
                error="pyarrow is required for Parquet. Install with: pip install pyarrow",
            )

        table = pq.read_table(source_path)
        columns = table.column_names
        row_count = table.num_rows

        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"{source_name}.parquet"

        pq.write_table(table, output_path)

        content_bytes = output_path.read_bytes()
        checksum = self.compute_checksum(content_bytes)

        return ExtractionResult(
            source_name=source_name,
            file_path=output_path,
            row_count=row_count,
            byte_count=len(content_bytes),
            checksum=checksum,
            duration_seconds=time.monotonic() - start_time,
            success=True,
            columns=list(columns),
        )


# ---------------------------------------------------------------------------
# Convenience functions (backward-compatible with v1)
# ---------------------------------------------------------------------------


# dbt's canonical sample data
SOURCES: dict[str, str] = {
    "orders": "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_orders.csv",
    "customers": "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_customers.csv",
    "payments": "https://raw.githubusercontent.com/dbt-labs/jaffle_shop/main/seeds/raw_payments.csv",
}

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "orders": ["id", "user_id", "order_date", "status"],
    "customers": ["id", "first_name", "last_name"],
    "payments": ["id", "order_id", "payment_method", "amount"],
}


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
        content = _download_with_retry(url, max_retries=max_retries)
        rows, columns = _validate_csv(content, source_name)
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
        "--verbose",
        "-v",
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
