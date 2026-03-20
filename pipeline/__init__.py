"""
ELT Pipeline Package.

A production-grade data pipeline demonstrating extraction, loading,
transformation, quality checks, and monitoring. Built with clean
architecture, comprehensive type hints, and testable design.

Modules:
    extract: Data extraction from REST APIs, databases, and files
    transform: Data validation, type casting, SCD Type 2, quality checks
    load: Warehouse loading (BigQuery, PostgreSQL, Parquet)
    orchestrator: DAG-based pipeline orchestration with parallel execution
    monitoring: Structured logging, metrics, alerting
"""

__version__ = "2.0.0"
