"""
Tests for the data quality checking framework.

Tests cover:
- Null rate detection
- Row count validation
- Schema matching
- Uniqueness checking
- Accepted values validation
- Referential integrity
- End-to-end quality check execution
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.quality import (
    QualityCheckConfig,
    check_accepted_values,
    check_null_rate,
    check_referential_integrity,
    check_row_count_min,
    check_schema_match,
    check_uniqueness,
    run_all_checks,
    run_check,
)


# ---------------------------------------------------------------------------
# Null rate checks
# ---------------------------------------------------------------------------


class TestNullRate:
    """Tests for null rate checking."""

    def test_no_nulls(self) -> None:
        rows = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.0)
        assert passed
        assert rate == 0.0

    def test_all_nulls(self) -> None:
        rows = [{"id": ""}, {"id": ""}, {"id": ""}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.0)
        assert not passed
        assert rate == 1.0

    def test_partial_nulls_below_threshold(self) -> None:
        rows = [{"id": "1"}, {"id": ""}, {"id": "3"}, {"id": "4"}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.3)
        assert passed
        assert rate == 0.25

    def test_partial_nulls_above_threshold(self) -> None:
        rows = [{"id": "1"}, {"id": ""}, {"id": "3"}, {"id": "4"}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.1)
        assert not passed

    def test_empty_rows(self) -> None:
        passed, rate, _ = check_null_rate([], "id", threshold=0.0)
        assert not passed
        assert rate == 1.0

    def test_missing_column_treated_as_null(self) -> None:
        rows = [{"name": "Alice"}, {"name": "Bob"}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.0)
        assert not passed
        assert rate == 1.0

    def test_whitespace_only_treated_as_null(self) -> None:
        rows = [{"id": "  "}, {"id": "1"}]
        passed, rate, _ = check_null_rate(rows, "id", threshold=0.0)
        assert not passed
        assert rate == 0.5


# ---------------------------------------------------------------------------
# Row count checks
# ---------------------------------------------------------------------------


class TestRowCountMin:
    """Tests for minimum row count checking."""

    def test_above_threshold(self) -> None:
        rows = [{"id": str(i)} for i in range(100)]
        passed, count, _ = check_row_count_min(rows, threshold=10)
        assert passed
        assert count == 100.0

    def test_at_threshold(self) -> None:
        rows = [{"id": str(i)} for i in range(10)]
        passed, count, _ = check_row_count_min(rows, threshold=10)
        assert passed

    def test_below_threshold(self) -> None:
        rows = [{"id": "1"}]
        passed, count, _ = check_row_count_min(rows, threshold=10)
        assert not passed
        assert count == 1.0

    def test_empty(self) -> None:
        passed, count, _ = check_row_count_min([], threshold=1)
        assert not passed
        assert count == 0.0


# ---------------------------------------------------------------------------
# Schema match checks
# ---------------------------------------------------------------------------


class TestSchemaMatch:
    """Tests for schema validation."""

    def test_exact_match(self) -> None:
        passed, rate, _ = check_schema_match(
            ["id", "name", "email"],
            ["id", "name", "email"],
        )
        assert passed
        assert rate == 1.0

    def test_extra_columns_ok(self) -> None:
        passed, rate, _ = check_schema_match(
            ["id", "name", "email", "extra"],
            ["id", "name", "email"],
        )
        assert passed
        assert rate == 1.0

    def test_missing_columns_fail(self) -> None:
        passed, rate, msg = check_schema_match(
            ["id", "name"],
            ["id", "name", "email"],
        )
        assert not passed
        assert rate < 1.0
        assert "email" in msg

    def test_empty_expected(self) -> None:
        passed, rate, _ = check_schema_match(["id"], [])
        assert passed
        assert rate == 1.0


# ---------------------------------------------------------------------------
# Uniqueness checks
# ---------------------------------------------------------------------------


class TestUniqueness:
    """Tests for uniqueness checking."""

    def test_all_unique(self) -> None:
        rows = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        passed, rate, _ = check_uniqueness(rows, "id")
        assert passed
        assert rate == 0.0

    def test_duplicates(self) -> None:
        rows = [{"id": "1"}, {"id": "1"}, {"id": "2"}]
        passed, rate, _ = check_uniqueness(rows, "id")
        assert not passed
        assert rate > 0.0

    def test_empty_rows(self) -> None:
        passed, rate, _ = check_uniqueness([], "id")
        assert passed


# ---------------------------------------------------------------------------
# Accepted values checks
# ---------------------------------------------------------------------------


class TestAcceptedValues:
    """Tests for accepted values checking."""

    def test_all_valid(self) -> None:
        rows = [{"status": "active"}, {"status": "inactive"}]
        passed, rate, _ = check_accepted_values(rows, "status", ["active", "inactive"])
        assert passed
        assert rate == 0.0

    def test_invalid_values(self) -> None:
        rows = [{"status": "active"}, {"status": "unknown"}]
        passed, rate, msg = check_accepted_values(rows, "status", ["active", "inactive"])
        assert not passed
        assert "unknown" in msg

    def test_empty_values_skipped(self) -> None:
        rows = [{"status": "active"}, {"status": ""}]
        passed, rate, _ = check_accepted_values(rows, "status", ["active"])
        assert passed

    def test_empty_rows(self) -> None:
        passed, rate, _ = check_accepted_values([], "status", ["active"])
        assert passed


# ---------------------------------------------------------------------------
# Referential integrity checks
# ---------------------------------------------------------------------------


class TestReferentialIntegrity:
    """Tests for referential integrity checking."""

    def test_valid_references(self) -> None:
        rows = [{"order_id": "1"}, {"order_id": "2"}]
        ref_rows = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        passed, rate, _ = check_referential_integrity(rows, "order_id", ref_rows, "id")
        assert passed
        assert rate == 0.0

    def test_orphaned_records(self) -> None:
        rows = [{"order_id": "1"}, {"order_id": "99"}]
        ref_rows = [{"id": "1"}, {"id": "2"}]
        passed, rate, msg = check_referential_integrity(rows, "order_id", ref_rows, "id")
        assert not passed
        assert "99" in msg

    def test_empty_rows(self) -> None:
        passed, rate, _ = check_referential_integrity([], "order_id", [{"id": "1"}], "id")
        assert passed

    def test_all_orphaned(self) -> None:
        rows = [{"order_id": "10"}, {"order_id": "20"}]
        ref_rows = [{"id": "1"}, {"id": "2"}]
        passed, rate, _ = check_referential_integrity(rows, "order_id", ref_rows, "id")
        assert not passed
        assert rate == 1.0


# ---------------------------------------------------------------------------
# Integration: run_check and run_all_checks
# ---------------------------------------------------------------------------


class TestRunCheck:
    """Tests for the run_check dispatcher."""

    def test_null_rate_check(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id,user_id,order_date,status\n1,1,2018-01-01,placed\n")

        config = QualityCheckConfig("orders", "null_rate", column="id", threshold=0.0)
        result = run_check(config, tmp_path)

        assert result.passed
        assert result.check_name == "null_rate:orders.id"

    def test_missing_file(self, tmp_path: Path) -> None:
        config = QualityCheckConfig("missing", "null_rate", column="id", threshold=0.0)
        result = run_check(config, tmp_path)

        assert not result.passed
        assert "not found" in result.message.lower()

    def test_unknown_check_type(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id\n1\n")

        config = QualityCheckConfig("orders", "nonexistent_check")
        result = run_check(config, tmp_path)

        assert not result.passed
        assert "unknown" in result.message.lower()


class TestRunAllChecks:
    """Tests for the full quality check suite."""

    def test_all_pass_with_valid_data(self, tmp_path: Path) -> None:
        # Create valid test data
        (tmp_path / "orders.csv").write_text(
            "id,user_id,order_date,status\n"
            "1,1,2018-01-01,placed\n"
            "2,3,2018-01-02,completed\n"
            "3,2,2018-01-03,shipped\n"
            "4,1,2018-01-04,placed\n"
            "5,3,2018-01-05,completed\n"
            "6,2,2018-01-06,returned\n"
            "7,1,2018-01-07,return_pending\n"
            "8,3,2018-01-08,placed\n"
            "9,2,2018-01-09,completed\n"
            "10,1,2018-01-10,shipped\n"
        )
        (tmp_path / "customers.csv").write_text(
            "id,first_name,last_name\n"
            "1,Michael,P.\n"
            "2,Shaquille,J.\n"
            "3,Nanna,O.\n"
            "4,Lisa,K.\n"
            "5,Anna,M.\n"
            "6,Bob,T.\n"
            "7,Carol,S.\n"
            "8,Dave,R.\n"
            "9,Eve,Q.\n"
            "10,Frank,L.\n"
        )
        (tmp_path / "payments.csv").write_text(
            "id,order_id,payment_method,amount\n"
            "1,1,credit_card,1000\n"
            "2,2,bank_transfer,2000\n"
            "3,3,coupon,500\n"
            "4,4,gift_card,1500\n"
            "5,5,credit_card,3000\n"
            "6,6,bank_transfer,750\n"
            "7,7,coupon,1200\n"
            "8,8,credit_card,800\n"
            "9,9,gift_card,2500\n"
            "10,10,bank_transfer,1100\n"
        )

        results = run_all_checks(data_dir=tmp_path)
        failed = [r for r in results if not r.passed]

        assert len(failed) == 0, f"Checks failed: {[(r.check_name, r.message) for r in failed]}"

    def test_custom_checks(self, tmp_path: Path) -> None:
        (tmp_path / "test.csv").write_text("id,val\n1,a\n2,b\n")

        custom_checks = [
            QualityCheckConfig("test", "row_count_min", threshold=1),
            QualityCheckConfig("test", "null_rate", column="id", threshold=0.0),
        ]

        results = run_all_checks(data_dir=tmp_path, checks=custom_checks)
        assert all(r.passed for r in results)
