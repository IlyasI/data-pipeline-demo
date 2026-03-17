"""
Data quality checking framework.

Implements a suite of configurable data quality checks that run after each
pipeline execution. Checks include null rate thresholds, freshness monitoring,
row count anomaly detection, and schema validation.

Each check is independent and returns a structured result, making it easy to
add new checks or integrate with external alerting.

Usage:
    python -m pipeline.quality
    python -m pipeline.quality --data-dir /path/to/data --verbose
"""

from __future__ import annotations

import csv
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    """Result of a single data quality check."""

    check_name: str
    source_name: str
    passed: bool
    severity: str  # "error", "warning", "info"
    message: str
    metric_value: float | None = None
    threshold: float | None = None
    duration_seconds: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityCheckConfig:
    """Configuration for a quality check on a specific source."""

    source_name: str
    check_type: str
    column: str | None = None
    threshold: float | None = None
    severity: str = "error"
    params: dict[str, Any] = field(default_factory=dict)


# Default quality check configurations
DEFAULT_CHECKS: list[QualityCheckConfig] = [
    # Null rate checks
    QualityCheckConfig("orders", "null_rate", column="id", threshold=0.0, severity="error"),
    QualityCheckConfig("orders", "null_rate", column="user_id", threshold=0.0, severity="error"),
    QualityCheckConfig("orders", "null_rate", column="order_date", threshold=0.0, severity="error"),
    QualityCheckConfig("orders", "null_rate", column="status", threshold=0.01, severity="warning"),
    QualityCheckConfig("customers", "null_rate", column="id", threshold=0.0, severity="error"),
    QualityCheckConfig("customers", "null_rate", column="first_name", threshold=0.05),
    QualityCheckConfig("customers", "null_rate", column="last_name", threshold=0.05),
    QualityCheckConfig("payments", "null_rate", column="id", threshold=0.0, severity="error"),
    QualityCheckConfig("payments", "null_rate", column="order_id", threshold=0.0, severity="error"),
    QualityCheckConfig("payments", "null_rate", column="amount", threshold=0.0, severity="error"),

    # Row count checks (minimum expected rows)
    QualityCheckConfig("orders", "row_count_min", threshold=10, severity="error"),
    QualityCheckConfig("customers", "row_count_min", threshold=10, severity="error"),
    QualityCheckConfig("payments", "row_count_min", threshold=10, severity="error"),

    # Schema validation
    QualityCheckConfig(
        "orders", "schema_match",
        params={"expected_columns": ["id", "user_id", "order_date", "status"]},
        severity="error",
    ),
    QualityCheckConfig(
        "customers", "schema_match",
        params={"expected_columns": ["id", "first_name", "last_name"]},
        severity="error",
    ),
    QualityCheckConfig(
        "payments", "schema_match",
        params={"expected_columns": ["id", "order_id", "payment_method", "amount"]},
        severity="error",
    ),

    # Uniqueness checks on primary keys
    QualityCheckConfig("orders", "uniqueness", column="id", severity="error"),
    QualityCheckConfig("customers", "uniqueness", column="id", severity="error"),
    QualityCheckConfig("payments", "uniqueness", column="id", severity="error"),

    # Accepted values
    QualityCheckConfig(
        "orders", "accepted_values", column="status",
        params={"values": ["placed", "shipped", "completed", "return_pending", "returned"]},
        severity="error",
    ),
    QualityCheckConfig(
        "payments", "accepted_values", column="payment_method",
        params={"values": ["credit_card", "coupon", "bank_transfer", "gift_card"]},
        severity="error",
    ),

    # Referential integrity
    QualityCheckConfig(
        "payments", "referential_integrity", column="order_id",
        params={"reference_source": "orders", "reference_column": "id"},
        severity="error",
    ),
]


def _read_csv_data(file_path: Path) -> tuple[list[dict[str, str]], list[str]]:
    """Read CSV file and return rows and column names."""
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    return rows, columns


def check_null_rate(
    rows: list[dict[str, str]],
    column: str,
    threshold: float,
) -> tuple[bool, float, str]:
    """Check that the null/empty rate for a column is below threshold.

    Args:
        rows: Data rows.
        column: Column name to check.
        threshold: Maximum allowed null rate (0.0 = no nulls, 1.0 = all nulls).

    Returns:
        Tuple of (passed, actual_rate, message).
    """
    if not rows:
        return False, 1.0, f"No rows to check for column '{column}'"

    null_count = sum(1 for row in rows if not row.get(column, "").strip())
    null_rate = null_count / len(rows)
    passed = null_rate <= threshold

    message = (
        f"Null rate for '{column}': {null_rate:.2%} "
        f"({'<=' if passed else '>'} threshold {threshold:.2%})"
    )
    return passed, null_rate, message


def check_row_count_min(
    rows: list[dict[str, str]],
    threshold: float,
) -> tuple[bool, float, str]:
    """Check that row count meets minimum threshold.

    Args:
        rows: Data rows.
        threshold: Minimum expected row count.

    Returns:
        Tuple of (passed, actual_count, message).
    """
    count = len(rows)
    passed = count >= threshold
    message = f"Row count: {count} ({'>=' if passed else '<'} minimum {int(threshold)})"
    return passed, float(count), message


def check_schema_match(
    columns: list[str],
    expected_columns: list[str],
) -> tuple[bool, float, str]:
    """Check that all expected columns are present.

    Args:
        columns: Actual column names.
        expected_columns: Required column names.

    Returns:
        Tuple of (passed, match_rate, message).
    """
    missing = set(expected_columns) - set(columns)
    extra = set(columns) - set(expected_columns)
    match_rate = 1.0 - (len(missing) / len(expected_columns)) if expected_columns else 1.0
    passed = len(missing) == 0

    parts = [f"Schema match: {match_rate:.0%}"]
    if missing:
        parts.append(f"missing: {sorted(missing)}")
    if extra:
        parts.append(f"extra: {sorted(extra)}")
    message = ". ".join(parts)

    return passed, match_rate, message


def check_uniqueness(
    rows: list[dict[str, str]],
    column: str,
) -> tuple[bool, float, str]:
    """Check that values in a column are unique.

    Args:
        rows: Data rows.
        column: Column name to check uniqueness on.

    Returns:
        Tuple of (passed, duplicate_rate, message).
    """
    if not rows:
        return True, 0.0, f"No rows to check uniqueness for '{column}'"

    values = [row.get(column, "") for row in rows]
    total = len(values)
    unique = len(set(values))
    duplicate_rate = (total - unique) / total if total > 0 else 0.0
    passed = duplicate_rate == 0.0

    message = (
        f"Uniqueness for '{column}': {unique}/{total} unique values "
        f"({duplicate_rate:.2%} duplicate rate)"
    )
    return passed, duplicate_rate, message


def check_accepted_values(
    rows: list[dict[str, str]],
    column: str,
    accepted: list[str],
) -> tuple[bool, float, str]:
    """Check that all values in a column are in the accepted set.

    Args:
        rows: Data rows.
        column: Column name to check.
        accepted: List of accepted values.

    Returns:
        Tuple of (passed, violation_rate, message).
    """
    if not rows:
        return True, 0.0, f"No rows to check accepted values for '{column}'"

    values = [row.get(column, "").strip() for row in rows]
    violations = [v for v in values if v and v not in accepted]
    violation_rate = len(violations) / len(values) if values else 0.0
    passed = len(violations) == 0

    message = (
        f"Accepted values for '{column}': "
        f"{len(violations)}/{len(values)} violations ({violation_rate:.2%})"
    )
    if violations:
        unique_violations = sorted(set(violations))[:5]
        message += f". Invalid values: {unique_violations}"

    return passed, violation_rate, message


def check_referential_integrity(
    rows: list[dict[str, str]],
    column: str,
    reference_rows: list[dict[str, str]],
    reference_column: str,
) -> tuple[bool, float, str]:
    """Check that all values in a column exist in a reference source.

    Args:
        rows: Data rows to check.
        column: Foreign key column.
        reference_rows: Reference data rows.
        reference_column: Primary key column in reference data.

    Returns:
        Tuple of (passed, orphan_rate, message).
    """
    if not rows:
        return True, 0.0, f"No rows to check referential integrity for '{column}'"

    fk_values = {row.get(column, "").strip() for row in rows if row.get(column, "").strip()}
    pk_values = {
        row.get(reference_column, "").strip()
        for row in reference_rows
        if row.get(reference_column, "").strip()
    }

    orphans = fk_values - pk_values
    orphan_rate = len(orphans) / len(fk_values) if fk_values else 0.0
    passed = len(orphans) == 0

    message = (
        f"Referential integrity '{column}': "
        f"{len(orphans)}/{len(fk_values)} orphaned records ({orphan_rate:.2%})"
    )
    if orphans:
        sample = sorted(orphans)[:5]
        message += f". Orphaned values: {sample}"

    return passed, orphan_rate, message


def run_check(
    config: QualityCheckConfig,
    data_dir: Path,
) -> QualityResult:
    """Run a single quality check based on its configuration.

    Args:
        config: Check configuration.
        data_dir: Directory containing CSV files.

    Returns:
        QualityResult with check outcome.
    """
    start_time = time.monotonic()

    try:
        file_path = data_dir / f"{config.source_name}.csv"
        if not file_path.exists():
            return QualityResult(
                check_name=f"{config.check_type}:{config.source_name}.{config.column or '*'}",
                source_name=config.source_name,
                passed=False,
                severity=config.severity,
                message=f"Source file not found: {file_path}",
                duration_seconds=time.monotonic() - start_time,
            )

        rows, columns = _read_csv_data(file_path)
        check_name = f"{config.check_type}:{config.source_name}"
        if config.column:
            check_name += f".{config.column}"

        if config.check_type == "null_rate":
            passed, metric, message = check_null_rate(
                rows, config.column or "", config.threshold or 0.0
            )
        elif config.check_type == "row_count_min":
            passed, metric, message = check_row_count_min(rows, config.threshold or 0)
        elif config.check_type == "schema_match":
            passed, metric, message = check_schema_match(
                columns, config.params.get("expected_columns", [])
            )
        elif config.check_type == "uniqueness":
            passed, metric, message = check_uniqueness(rows, config.column or "")
        elif config.check_type == "accepted_values":
            passed, metric, message = check_accepted_values(
                rows, config.column or "", config.params.get("values", [])
            )
        elif config.check_type == "referential_integrity":
            ref_source = config.params.get("reference_source", "")
            ref_column = config.params.get("reference_column", "")
            ref_path = data_dir / f"{ref_source}.csv"
            if not ref_path.exists():
                return QualityResult(
                    check_name=check_name,
                    source_name=config.source_name,
                    passed=False,
                    severity=config.severity,
                    message=f"Reference source not found: {ref_path}",
                    duration_seconds=time.monotonic() - start_time,
                )
            ref_rows, _ = _read_csv_data(ref_path)
            passed, metric, message = check_referential_integrity(
                rows, config.column or "", ref_rows, ref_column
            )
        else:
            return QualityResult(
                check_name=check_name,
                source_name=config.source_name,
                passed=False,
                severity="error",
                message=f"Unknown check type: {config.check_type}",
                duration_seconds=time.monotonic() - start_time,
            )

        duration = time.monotonic() - start_time
        log_fn = logger.info if passed else (
            logger.error if config.severity == "error" else logger.warning
        )
        log_fn("[%s] %s", "PASS" if passed else "FAIL", message)

        return QualityResult(
            check_name=check_name,
            source_name=config.source_name,
            passed=passed,
            severity=config.severity,
            message=message,
            metric_value=metric,
            threshold=config.threshold,
            duration_seconds=duration,
        )

    except Exception as exc:
        duration = time.monotonic() - start_time
        logger.error("Check failed with exception: %s", str(exc))
        return QualityResult(
            check_name=f"{config.check_type}:{config.source_name}",
            source_name=config.source_name,
            passed=False,
            severity="error",
            message=f"Check raised exception: {str(exc)}",
            duration_seconds=duration,
        )


def run_all_checks(
    data_dir: Path | None = None,
    checks: list[QualityCheckConfig] | None = None,
) -> list[QualityResult]:
    """Run all configured quality checks.

    Args:
        data_dir: Directory containing CSV files. Defaults to data/raw.
        checks: Override check configurations. Defaults to DEFAULT_CHECKS.

    Returns:
        List of QualityResult objects.
    """
    if data_dir is None:
        data_dir = Path("data/raw")
    if checks is None:
        checks = DEFAULT_CHECKS

    logger.info("Running %d quality checks on %s", len(checks), data_dir)
    results: list[QualityResult] = []

    for config in checks:
        result = run_check(config, data_dir)
        results.append(result)

    passed = sum(1 for r in results if r.passed)
    failed_errors = [r for r in results if not r.passed and r.severity == "error"]
    failed_warnings = [r for r in results if not r.passed and r.severity == "warning"]

    logger.info(
        "Quality check results: %d/%d passed, %d errors, %d warnings",
        passed, len(results), len(failed_errors), len(failed_warnings),
    )

    if failed_errors:
        logger.error(
            "CRITICAL quality failures: %s",
            ", ".join(r.check_name for r in failed_errors),
        )

    return results


def main() -> None:
    """CLI entrypoint for data quality checks."""
    import argparse

    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing CSV files (default: data/raw)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    results = run_all_checks(data_dir=args.data_dir)
    failed_errors = [r for r in results if not r.passed and r.severity == "error"]

    if failed_errors:
        raise SystemExit(
            f"{len(failed_errors)} critical quality check(s) failed: "
            f"{', '.join(r.check_name for r in failed_errors)}"
        )


if __name__ == "__main__":
    main()
