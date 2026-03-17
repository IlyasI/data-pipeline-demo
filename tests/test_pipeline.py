"""
Tests for the extraction and loading pipeline.

Tests cover:
- CSV download and validation
- Schema validation logic
- Idempotent file writes
- Row type casting
- Error handling and retries
"""

from __future__ import annotations

import csv
import io
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipeline.extract import (
    ExtractionResult,
    _compute_checksum,
    _validate_csv,
    extract_source,
)
from pipeline.load import DuckDBBackend, _cast_row, _read_csv, load_source


# ---------------------------------------------------------------------------
# Extract tests
# ---------------------------------------------------------------------------


class TestValidateCSV:
    """Tests for CSV schema validation."""

    def test_valid_orders_csv(self) -> None:
        content = textwrap.dedent("""\
            id,user_id,order_date,status
            1,1,2018-01-01,placed
            2,3,2018-01-02,completed
        """)
        rows, columns = _validate_csv(content, "orders")
        assert len(rows) == 2
        assert columns == ["id", "user_id", "order_date", "status"]

    def test_valid_customers_csv(self) -> None:
        content = textwrap.dedent("""\
            id,first_name,last_name
            1,Michael,P.
            2,Shaquille,J.
        """)
        rows, columns = _validate_csv(content, "customers")
        assert len(rows) == 2

    def test_missing_columns_raises(self) -> None:
        content = textwrap.dedent("""\
            id,user_id
            1,1
        """)
        with pytest.raises(ValueError, match="missing columns"):
            _validate_csv(content, "orders")

    def test_empty_csv_raises(self) -> None:
        content = "id,user_id,order_date,status\n"
        with pytest.raises(ValueError, match="No data rows"):
            _validate_csv(content, "orders")

    def test_extra_columns_accepted(self) -> None:
        content = textwrap.dedent("""\
            id,user_id,order_date,status,extra_col
            1,1,2018-01-01,placed,foo
        """)
        rows, columns = _validate_csv(content, "orders")
        assert len(rows) == 1
        assert "extra_col" in columns

    def test_unknown_source_accepts_any_schema(self) -> None:
        content = textwrap.dedent("""\
            a,b,c
            1,2,3
        """)
        rows, columns = _validate_csv(content, "unknown_source")
        assert len(rows) == 1


class TestChecksum:
    """Tests for content checksumming."""

    def test_deterministic(self) -> None:
        assert _compute_checksum("hello") == _compute_checksum("hello")

    def test_different_content(self) -> None:
        assert _compute_checksum("hello") != _compute_checksum("world")

    def test_returns_16_chars(self) -> None:
        assert len(_compute_checksum("test")) == 16


class TestExtractSource:
    """Tests for the extract_source function."""

    @patch("pipeline.extract._download_with_retry")
    def test_successful_extraction(self, mock_download: MagicMock, tmp_path: Path) -> None:
        mock_download.return_value = textwrap.dedent("""\
            id,user_id,order_date,status
            1,1,2018-01-01,placed
        """)

        result = extract_source("orders", "http://example.com/orders.csv", tmp_path)

        assert result.success
        assert result.row_count == 1
        assert result.source_name == "orders"
        assert (tmp_path / "orders.csv").exists()

    @patch("pipeline.extract._download_with_retry")
    def test_idempotent_skip(self, mock_download: MagicMock, tmp_path: Path) -> None:
        content = textwrap.dedent("""\
            id,user_id,order_date,status
            1,1,2018-01-01,placed
        """)
        mock_download.return_value = content

        # First extraction
        extract_source("orders", "http://example.com/orders.csv", tmp_path)
        # Second extraction with same content should skip
        result = extract_source("orders", "http://example.com/orders.csv", tmp_path)

        assert result.success
        assert result.row_count == 1

    @patch("pipeline.extract._download_with_retry")
    def test_download_failure(self, mock_download: MagicMock, tmp_path: Path) -> None:
        mock_download.side_effect = Exception("Network error")

        result = extract_source("orders", "http://example.com/orders.csv", tmp_path)

        assert not result.success
        assert "Network error" in (result.error or "")


# ---------------------------------------------------------------------------
# Load tests
# ---------------------------------------------------------------------------


class TestCastRow:
    """Tests for row type casting."""

    def test_integer_casting(self) -> None:
        schema = [{"name": "id", "type": "INTEGER"}]
        result = _cast_row({"id": "42"}, schema)
        assert result["id"] == 42
        assert isinstance(result["id"], int)

    def test_varchar_passthrough(self) -> None:
        schema = [{"name": "name", "type": "VARCHAR"}]
        result = _cast_row({"name": "Alice"}, schema)
        assert result["name"] == "Alice"

    def test_null_handling(self) -> None:
        schema = [{"name": "id", "type": "INTEGER"}]
        result = _cast_row({"id": ""}, schema)
        assert result["id"] is None

    def test_none_handling(self) -> None:
        schema = [{"name": "id", "type": "INTEGER"}]
        result = _cast_row({"id": None}, schema)  # type: ignore[dict-item]
        assert result["id"] is None

    def test_float_casting(self) -> None:
        schema = [{"name": "amount", "type": "FLOAT"}]
        result = _cast_row({"amount": "3.14"}, schema)
        assert abs(result["amount"] - 3.14) < 0.001


class TestReadCSV:
    """Tests for CSV file reading."""

    def test_read_valid_csv(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")

        rows = _read_csv(csv_file)
        assert len(rows) == 2
        assert rows[0]["id"] == "1"
        assert rows[1]["name"] == "Bob"


class TestDuckDBBackend:
    """Tests for the DuckDB warehouse backend."""

    def test_create_schema_and_table(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            backend.create_schema("test_schema")
            backend.create_table(
                "test_schema",
                "test_table",
                [{"name": "id", "type": "INTEGER"}, {"name": "name", "type": "VARCHAR"}],
            )
            assert backend.row_count("test_schema", "test_table") == 0
        finally:
            backend.close()

    def test_load_and_count_rows(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            backend.create_schema("raw")
            backend.create_table(
                "raw", "test",
                [{"name": "id", "type": "INTEGER"}, {"name": "val", "type": "VARCHAR"}],
            )

            rows = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
            loaded = backend.load_rows("raw", "test", rows)

            assert loaded == 2
            assert backend.row_count("raw", "test") == 2
        finally:
            backend.close()

    def test_idempotent_reload(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            backend.create_schema("raw")
            backend.create_table(
                "raw", "test",
                [{"name": "id", "type": "INTEGER"}],
            )

            # Load twice
            backend.load_rows("raw", "test", [{"id": 1}])
            backend.load_rows("raw", "test", [{"id": 1}, {"id": 2}])

            # Should have 2 rows (not 3), because reload clears first
            assert backend.row_count("raw", "test") == 2
        finally:
            backend.close()

    def test_empty_load(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            backend.create_schema("raw")
            backend.create_table("raw", "test", [{"name": "id", "type": "INTEGER"}])
            loaded = backend.load_rows("raw", "test", [])
            assert loaded == 0
        finally:
            backend.close()


class TestLoadSource:
    """Tests for the load_source function."""

    def test_missing_file(self, tmp_path: Path) -> None:
        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            result = load_source("orders", tmp_path / "nonexistent.csv", backend)
            assert not result.success
            assert "not found" in (result.error or "").lower()
        finally:
            backend.close()

    def test_unknown_source_schema(self, tmp_path: Path) -> None:
        csv_file = tmp_path / "unknown.csv"
        csv_file.write_text("a,b\n1,2\n")

        db_path = str(tmp_path / "test.duckdb")
        backend = DuckDBBackend(db_path=db_path)

        try:
            result = load_source("unknown", csv_file, backend)
            assert not result.success
            assert "no schema" in (result.error or "").lower()
        finally:
            backend.close()
