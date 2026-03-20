"""
Data transformation module.

Provides a framework for in-Python transformations that complement dbt's
SQL-based transforms. Useful for:
- Pre-load data validation with Pydantic models
- Type casting and null handling before warehouse ingestion
- SCD Type 2 history tracking for slowly changing dimensions
- Data quality checks that are easier to express in Python than SQL

Design philosophy: Keep business logic in dbt (SQL). Use Python transforms
only for structural operations (validation, casting, deduplication) that
happen before data reaches the warehouse.

Usage:
    from pipeline.transform import TransformPipeline, validate_records

    # Validate raw records against a schema
    valid, invalid = validate_records(raw_records, OrderRecord)

    # Run a full transform pipeline
    pipeline = TransformPipeline(steps=[
        DeduplicateStep(key_columns=["id"]),
        CastTypesStep(type_map={"amount": "float", "id": "int"}),
        NullHandlerStep(defaults={"status": "unknown"}),
    ])
    result = pipeline.run(records)
"""

from __future__ import annotations

import abc
import hashlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any, TypeVar

from pydantic import BaseModel, Field, ValidationError, field_validator

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic validation models
# ---------------------------------------------------------------------------


class OrderRecord(BaseModel):
    """Validated order record.

    Enforces types and constraints on raw order data before it enters
    the warehouse. Invalid records are caught at validation time rather
    than causing load failures or silent data corruption.
    """

    id: int = Field(gt=0, description="Primary key, must be positive")
    user_id: int = Field(gt=0, description="Foreign key to customers")
    order_date: date = Field(description="Date the order was placed")
    status: str = Field(
        min_length=1,
        description="Order status",
    )

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        allowed = {"placed", "shipped", "completed", "return_pending", "returned"}
        if v.lower().strip() not in allowed:
            raise ValueError(f"Invalid status '{v}'. Must be one of: {sorted(allowed)}")
        return v.lower().strip()


class CustomerRecord(BaseModel):
    """Validated customer record."""

    id: int = Field(gt=0, description="Primary key, must be positive")
    first_name: str = Field(min_length=1, description="Customer first name")
    last_name: str = Field(min_length=1, description="Customer last name")

    @field_validator("first_name", "last_name")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()


class PaymentRecord(BaseModel):
    """Validated payment record."""

    id: int = Field(gt=0, description="Primary key, must be positive")
    order_id: int = Field(gt=0, description="Foreign key to orders")
    payment_method: str = Field(min_length=1, description="Payment method used")
    amount: int = Field(ge=0, description="Payment amount in cents")

    @field_validator("payment_method")
    @classmethod
    def validate_payment_method(cls, v: str) -> str:
        allowed = {"credit_card", "coupon", "bank_transfer", "gift_card"}
        if v.lower().strip() not in allowed:
            raise ValueError(f"Invalid payment method '{v}'. Must be one of: {sorted(allowed)}")
        return v.lower().strip()


ModelT = TypeVar("ModelT", bound=BaseModel)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Result of validating a batch of records against a Pydantic model.

    Attributes:
        valid: Records that passed validation.
        invalid: Tuples of (record, error_message) for failed records.
        total: Total records processed.
        valid_count: Number of valid records.
        invalid_count: Number of invalid records.
        validation_rate: Fraction of records that passed (0.0 to 1.0).
    """

    valid: list[dict[str, Any]]
    invalid: list[tuple[dict[str, Any], str]]

    @property
    def total(self) -> int:
        return len(self.valid) + len(self.invalid)

    @property
    def valid_count(self) -> int:
        return len(self.valid)

    @property
    def invalid_count(self) -> int:
        return len(self.invalid)

    @property
    def validation_rate(self) -> float:
        return self.valid_count / self.total if self.total > 0 else 0.0


def validate_records(
    records: list[dict[str, Any]],
    model: type[ModelT],
) -> ValidationResult:
    """Validate a list of records against a Pydantic model.

    Each record is validated independently. Valid records are returned
    with their validated (and potentially coerced) values. Invalid
    records are collected with their error messages for logging or
    dead-letter queue handling.

    Args:
        records: Raw records to validate.
        model: Pydantic model class to validate against.

    Returns:
        ValidationResult with valid and invalid record lists.

    Example:
        >>> records = [{"id": "1", "user_id": "2", "order_date": "2024-01-01", "status": "placed"}]
        >>> result = validate_records(records, OrderRecord)
        >>> result.valid_count
        1
    """
    valid: list[dict[str, Any]] = []
    invalid: list[tuple[dict[str, Any], str]] = []

    for record in records:
        try:
            validated = model.model_validate(record)
            valid.append(validated.model_dump())
        except ValidationError as exc:
            error_msg = "; ".join(
                f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                for e in exc.errors()
            )
            invalid.append((record, error_msg))
            logger.debug("Validation failed for record %s: %s", record, error_msg)

    logger.info(
        "Validation complete: %d/%d valid (%.1f%%), %d invalid",
        len(valid),
        len(valid) + len(invalid),
        100 * len(valid) / max(len(valid) + len(invalid), 1),
        len(invalid),
    )

    return ValidationResult(valid=valid, invalid=invalid)


# ---------------------------------------------------------------------------
# Transform steps
# ---------------------------------------------------------------------------


class TransformStep(abc.ABC):
    """Abstract base class for a single transformation step.

    Each step takes a list of records (dicts), applies a transformation,
    and returns the modified list. Steps are composable via TransformPipeline.
    """

    @abc.abstractmethod
    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply this transformation to a list of records.

        Args:
            records: Input records.

        Returns:
            Transformed records.
        """

    @property
    def name(self) -> str:
        """Human-readable step name for logging."""
        return self.__class__.__name__


class DeduplicateStep(TransformStep):
    """Remove duplicate records based on key columns.

    When duplicates are found, keeps the last occurrence (assuming
    later records are more recent). For explicit ordering, sort the
    records before deduplication.

    Args:
        key_columns: Columns that define uniqueness.
        keep: Which duplicate to keep ("first" or "last").
    """

    def __init__(self, key_columns: list[str], keep: str = "last") -> None:
        self.key_columns = key_columns
        self.keep = keep

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not records:
            return records

        seen: dict[tuple[Any, ...], int] = {}
        for idx, record in enumerate(records):
            key = tuple(record.get(col) for col in self.key_columns)
            if self.keep == "last":
                seen[key] = idx
            elif key not in seen:
                seen[key] = idx

        deduplicated = [records[idx] for idx in sorted(seen.values())]

        removed = len(records) - len(deduplicated)
        if removed > 0:
            logger.info(
                "Deduplication: removed %d duplicates (%d -> %d rows, key=%s)",
                removed,
                len(records),
                len(deduplicated),
                self.key_columns,
            )

        return deduplicated


class CastTypesStep(TransformStep):
    """Cast record values to specified types.

    Handles common type conversions: str->int, str->float, str->date,
    str->datetime, str->bool, str->Decimal. Returns None for values
    that cannot be cast.

    Args:
        type_map: Mapping of column_name -> target type name.
            Supported types: "int", "float", "str", "bool", "date",
            "datetime", "decimal".
    """

    TYPE_CASTERS: dict[str, type | None] = {
        "int": int,
        "float": float,
        "str": str,
        "bool": None,  # handled specially
        "date": None,  # handled specially
        "datetime": None,  # handled specially
        "decimal": Decimal,
    }

    def __init__(self, type_map: dict[str, str]) -> None:
        self.type_map = type_map

    def _cast_value(self, value: Any, target_type: str) -> Any:
        """Cast a single value to the target type.

        Args:
            value: Raw value to cast.
            target_type: Name of the target type.

        Returns:
            Cast value, or None if casting fails.
        """
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None

        try:
            if target_type == "bool":
                if isinstance(value, bool):
                    return value
                return str(value).lower().strip() in ("true", "1", "yes", "t", "y")

            if target_type == "date":
                if isinstance(value, date):
                    return value
                return date.fromisoformat(str(value).strip())

            if target_type == "datetime":
                if isinstance(value, datetime):
                    return value
                return datetime.fromisoformat(str(value).strip())

            caster = self.TYPE_CASTERS.get(target_type)
            if caster is not None:
                return caster(value)

            return value

        except (ValueError, TypeError) as exc:
            logger.debug("Cast failed for value %r -> %s: %s", value, target_type, exc)
            return None

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        cast_failures = 0

        for record in records:
            new_record = dict(record)
            for col, target_type in self.type_map.items():
                if col in new_record:
                    original = new_record[col]
                    new_record[col] = self._cast_value(original, target_type)
                    if new_record[col] is None and original is not None and str(original).strip():
                        cast_failures += 1
            result.append(new_record)

        if cast_failures > 0:
            logger.warning("Type casting: %d values could not be cast", cast_failures)

        return result


class NullHandlerStep(TransformStep):
    """Handle null/missing values with configurable defaults.

    For each column in the defaults mapping, replaces None, empty string,
    and whitespace-only values with the specified default.

    Args:
        defaults: Mapping of column_name -> default value.
        fill_strategy: How to fill nulls. "default" uses the defaults map.
            "drop" removes records with any null in the specified columns.
    """

    def __init__(
        self,
        defaults: dict[str, Any] | None = None,
        fill_strategy: str = "default",
    ) -> None:
        self.defaults = defaults or {}
        self.fill_strategy = fill_strategy

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if self.fill_strategy == "drop":
            columns = set(self.defaults.keys()) if self.defaults else set()
            original_count = len(records)
            result = [
                r
                for r in records
                if all(
                    r.get(col) is not None and str(r.get(col, "")).strip() != ""
                    for col in columns
                )
            ]
            dropped = original_count - len(result)
            if dropped > 0:
                logger.info("Null handler: dropped %d records with nulls", dropped)
            return result

        result: list[dict[str, Any]] = []
        fill_count = 0

        for record in records:
            new_record = dict(record)
            for col, default in self.defaults.items():
                value = new_record.get(col)
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    new_record[col] = default
                    fill_count += 1
            result.append(new_record)

        if fill_count > 0:
            logger.info("Null handler: filled %d null values with defaults", fill_count)

        return result


class FilterStep(TransformStep):
    """Filter records based on a predicate function.

    Args:
        predicate: Function that takes a record dict and returns True to keep.
        description: Human-readable description of the filter for logging.
    """

    def __init__(
        self,
        predicate: Any,  # Callable[[dict[str, Any]], bool]
        description: str = "custom filter",
    ) -> None:
        self._predicate = predicate
        self._description = description

    @property
    def name(self) -> str:
        return f"FilterStep({self._description})"

    def apply(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        original_count = len(records)
        result = [r for r in records if self._predicate(r)]
        filtered = original_count - len(result)
        if filtered > 0:
            logger.info(
                "Filter '%s': removed %d records (%d -> %d)",
                self._description,
                filtered,
                original_count,
                len(result),
            )
        return result


# ---------------------------------------------------------------------------
# SCD Type 2
# ---------------------------------------------------------------------------


class SCDType2Handler:
    """Slowly Changing Dimension Type 2 handler.

    Implements SCD Type 2 logic: when a tracked attribute changes, the
    existing record is "closed" (effective_to is set) and a new record
    is "opened" with the updated values. This preserves the full history
    of changes.

    Args:
        key_column: The business key (natural key) column.
        tracked_columns: Columns whose changes trigger a new version.
        effective_from_col: Column name for the start of the validity period.
        effective_to_col: Column name for the end of the validity period.
        is_current_col: Column name for the current record flag.
        hash_col: Column name for the change-detection hash.

    Example:
        >>> handler = SCDType2Handler(
        ...     key_column="customer_id",
        ...     tracked_columns=["name", "email", "tier"],
        ... )
        >>> updated = handler.apply(existing_records, incoming_records)
    """

    def __init__(
        self,
        key_column: str,
        tracked_columns: list[str],
        effective_from_col: str = "_effective_from",
        effective_to_col: str = "_effective_to",
        is_current_col: str = "_is_current",
        hash_col: str = "_row_hash",
    ) -> None:
        self.key_column = key_column
        self.tracked_columns = tracked_columns
        self.effective_from_col = effective_from_col
        self.effective_to_col = effective_to_col
        self.is_current_col = is_current_col
        self.hash_col = hash_col

    def _compute_row_hash(self, record: dict[str, Any]) -> str:
        """Compute a hash of the tracked columns for change detection."""
        values = "|".join(str(record.get(col, "")) for col in sorted(self.tracked_columns))
        return hashlib.md5(values.encode("utf-8")).hexdigest()

    def apply(
        self,
        existing: list[dict[str, Any]],
        incoming: list[dict[str, Any]],
        as_of: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Apply SCD Type 2 logic to merge incoming records with existing.

        Records are matched on the key_column. For each incoming record:
        - If it's new (key not in existing), insert with is_current=True.
        - If the tracked columns changed, close the old version and open new.
        - If nothing changed, keep the existing record as-is.

        Args:
            existing: Current dimension records (with SCD metadata columns).
            incoming: New records to merge in.
            as_of: Timestamp for the change. Defaults to now (UTC).

        Returns:
            Complete set of records with SCD Type 2 metadata applied.
        """
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        # Index current records by business key
        current_by_key: dict[Any, dict[str, Any]] = {}
        for record in existing:
            if record.get(self.is_current_col, False):
                key = record[self.key_column]
                current_by_key[key] = record

        result: list[dict[str, Any]] = []

        # Keep all historical (non-current) records unchanged
        for record in existing:
            if not record.get(self.is_current_col, False):
                result.append(record)

        new_count = 0
        changed_count = 0
        unchanged_count = 0

        processed_keys: set[Any] = set()

        for incoming_record in incoming:
            key = incoming_record[self.key_column]
            processed_keys.add(key)
            incoming_hash = self._compute_row_hash(incoming_record)

            current = current_by_key.get(key)

            if current is None:
                # New record: insert as current
                new_record = dict(incoming_record)
                new_record[self.effective_from_col] = as_of.isoformat()
                new_record[self.effective_to_col] = None
                new_record[self.is_current_col] = True
                new_record[self.hash_col] = incoming_hash
                result.append(new_record)
                new_count += 1

            elif self._compute_row_hash(current) != incoming_hash:
                # Changed: close old version, open new version
                closed = dict(current)
                closed[self.effective_to_col] = as_of.isoformat()
                closed[self.is_current_col] = False
                result.append(closed)

                new_version = dict(incoming_record)
                new_version[self.effective_from_col] = as_of.isoformat()
                new_version[self.effective_to_col] = None
                new_version[self.is_current_col] = True
                new_version[self.hash_col] = incoming_hash
                result.append(new_version)
                changed_count += 1

            else:
                # Unchanged: keep existing current record
                result.append(current)
                unchanged_count += 1

        # Keep current records for keys not in the incoming batch
        for key, record in current_by_key.items():
            if key not in processed_keys:
                result.append(record)

        logger.info(
            "SCD Type 2: %d new, %d changed, %d unchanged (total: %d records)",
            new_count,
            changed_count,
            unchanged_count,
            len(result),
        )

        return result


# ---------------------------------------------------------------------------
# Data quality checks (Python-side, pre-load)
# ---------------------------------------------------------------------------


@dataclass
class QualityCheckResult:
    """Result of a single data quality check.

    Attributes:
        check_name: Name of the check.
        passed: Whether the check passed.
        metric_value: The measured metric value.
        threshold: The threshold the metric was compared against.
        message: Human-readable description of the result.
    """

    check_name: str
    passed: bool
    metric_value: float
    threshold: float
    message: str


def check_completeness(
    records: list[dict[str, Any]],
    required_columns: list[str],
    threshold: float = 0.95,
) -> QualityCheckResult:
    """Check that required columns have non-null values above a threshold.

    Measures the fraction of records where ALL required columns are non-null,
    and compares against the threshold.

    Args:
        records: Records to check.
        required_columns: Columns that should not be null.
        threshold: Minimum acceptable completeness rate (0.0 to 1.0).

    Returns:
        QualityCheckResult with the completeness rate.
    """
    if not records:
        return QualityCheckResult(
            check_name="completeness",
            passed=False,
            metric_value=0.0,
            threshold=threshold,
            message="No records to check",
        )

    complete = sum(
        1
        for r in records
        if all(
            r.get(col) is not None and str(r.get(col, "")).strip() != ""
            for col in required_columns
        )
    )
    rate = complete / len(records)
    passed = rate >= threshold

    return QualityCheckResult(
        check_name="completeness",
        passed=passed,
        metric_value=rate,
        threshold=threshold,
        message=f"Completeness: {rate:.1%} ({complete}/{len(records)} complete for {required_columns})",
    )


def check_uniqueness(
    records: list[dict[str, Any]],
    key_columns: list[str],
) -> QualityCheckResult:
    """Check that records are unique on the given key columns.

    Args:
        records: Records to check.
        key_columns: Columns that should form a unique key.

    Returns:
        QualityCheckResult with the duplicate rate.
    """
    if not records:
        return QualityCheckResult(
            check_name="uniqueness",
            passed=True,
            metric_value=0.0,
            threshold=0.0,
            message="No records to check",
        )

    keys = [tuple(r.get(col) for col in key_columns) for r in records]
    unique_count = len(set(keys))
    duplicate_rate = (len(keys) - unique_count) / len(keys)
    passed = duplicate_rate == 0.0

    return QualityCheckResult(
        check_name="uniqueness",
        passed=passed,
        metric_value=duplicate_rate,
        threshold=0.0,
        message=f"Uniqueness on {key_columns}: {unique_count}/{len(keys)} unique ({duplicate_rate:.2%} duplicate)",
    )


def check_freshness(
    records: list[dict[str, Any]],
    date_column: str,
    max_age_days: int = 7,
    reference_date: date | None = None,
) -> QualityCheckResult:
    """Check that the most recent record is within the expected freshness.

    Args:
        records: Records to check.
        date_column: Column containing a date or datetime.
        max_age_days: Maximum acceptable age in days.
        reference_date: Date to compare against. Defaults to today.

    Returns:
        QualityCheckResult with the actual age in days.
    """
    if reference_date is None:
        reference_date = date.today()

    if not records:
        return QualityCheckResult(
            check_name="freshness",
            passed=False,
            metric_value=float("inf"),
            threshold=float(max_age_days),
            message="No records to check freshness",
        )

    dates: list[date] = []
    for r in records:
        val = r.get(date_column)
        if val is not None:
            if isinstance(val, datetime):
                dates.append(val.date())
            elif isinstance(val, date):
                dates.append(val)
            elif isinstance(val, str) and val.strip():
                try:
                    dates.append(date.fromisoformat(val.strip()))
                except ValueError:
                    pass

    if not dates:
        return QualityCheckResult(
            check_name="freshness",
            passed=False,
            metric_value=float("inf"),
            threshold=float(max_age_days),
            message=f"No valid dates found in column '{date_column}'",
        )

    most_recent = max(dates)
    age_days = (reference_date - most_recent).days
    passed = age_days <= max_age_days

    return QualityCheckResult(
        check_name="freshness",
        passed=passed,
        metric_value=float(age_days),
        threshold=float(max_age_days),
        message=f"Freshness: most recent date is {most_recent} ({age_days} days old, max {max_age_days})",
    )


# ---------------------------------------------------------------------------
# Transform pipeline
# ---------------------------------------------------------------------------


@dataclass
class TransformResult:
    """Result of running a full transform pipeline.

    Attributes:
        records: Transformed records.
        input_count: Number of records before transformation.
        output_count: Number of records after transformation.
        step_results: Per-step metadata (step name, input/output counts).
        quality_checks: Results of any quality checks run.
    """

    records: list[dict[str, Any]]
    input_count: int
    output_count: int
    step_results: list[dict[str, Any]] = field(default_factory=list)
    quality_checks: list[QualityCheckResult] = field(default_factory=list)


class TransformPipeline:
    """Composable pipeline of transformation steps.

    Steps are applied sequentially. The pipeline tracks row counts at
    each step for debugging and monitoring. Optionally runs quality
    checks after all steps complete.

    Args:
        steps: Ordered list of transformation steps.
        quality_checks_fn: Optional function to run quality checks on
            the final output.

    Example:
        >>> pipeline = TransformPipeline(steps=[
        ...     DeduplicateStep(key_columns=["id"]),
        ...     CastTypesStep(type_map={"amount": "int", "date": "date"}),
        ...     NullHandlerStep(defaults={"status": "unknown"}),
        ... ])
        >>> result = pipeline.run(raw_records)
        >>> print(f"{result.input_count} -> {result.output_count} records")
    """

    def __init__(
        self,
        steps: list[TransformStep],
        quality_checks_fn: Any | None = None,  # Callable or None
    ) -> None:
        self.steps = steps
        self.quality_checks_fn = quality_checks_fn

    def run(self, records: list[dict[str, Any]]) -> TransformResult:
        """Execute all transformation steps in sequence.

        Args:
            records: Input records.

        Returns:
            TransformResult with transformed records and metadata.
        """
        input_count = len(records)
        current = records
        step_results: list[dict[str, Any]] = []

        logger.info("Transform pipeline starting: %d records, %d steps", input_count, len(self.steps))

        for step in self.steps:
            before_count = len(current)
            current = step.apply(current)
            after_count = len(current)

            step_results.append({
                "step": step.name,
                "input_count": before_count,
                "output_count": after_count,
                "delta": after_count - before_count,
            })

            logger.info(
                "Step '%s': %d -> %d records (delta: %+d)",
                step.name,
                before_count,
                after_count,
                after_count - before_count,
            )

        quality_checks: list[QualityCheckResult] = []
        if self.quality_checks_fn is not None:
            quality_checks = self.quality_checks_fn(current)
            for check in quality_checks:
                log_fn = logger.info if check.passed else logger.warning
                log_fn("Quality check '%s': %s", check.check_name, check.message)

        logger.info(
            "Transform pipeline complete: %d -> %d records",
            input_count,
            len(current),
        )

        return TransformResult(
            records=current,
            input_count=input_count,
            output_count=len(current),
            step_results=step_results,
            quality_checks=quality_checks,
        )
