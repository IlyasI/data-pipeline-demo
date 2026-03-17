# Data Pipeline Demo

[![CI](https://github.com/IlyasI/data-pipeline-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/IlyasI/data-pipeline-demo/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-orange.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A production-grade ELT pipeline demonstrating real-world data engineering patterns. This project
extracts public e-commerce data, loads it into a warehouse, transforms it with dbt, and validates
everything with automated data quality checks and monitoring.

## Architecture

```
                        Data Pipeline Architecture
  ============================================================================

  +------------------+     +------------------+     +---------------------+
  |                  |     |                  |     |                     |
  |  Public Dataset  +---->+  Extract (Python)+---->+  Raw / Landing Zone |
  |  (CSV / API)     |     |  Retry + Logging |     |  (DuckDB / BQ)      |
  |                  |     |                  |     |                     |
  +------------------+     +------------------+     +----------+----------+
                                                               |
                                                               v
  +------------------+     +------------------+     +----------+----------+
  |                  |     |                  |     |                     |
  |  Mart Models     +<----+  dbt Transform   +<----+  Staging Models     |
  |  (dim_, fct_,    |     |  Compile + Run   |     |  (stg_*)            |
  |   metrics_)      |     |                  |     |  Cleaning, typing,  |
  |                  |     |                  |     |  deduplication       |
  +--------+---------+     +------------------+     +---------------------+
           |
           v
  +--------+---------+     +------------------+
  |                  |     |                  |
  |  Data Quality    +---->+  Monitor / Alert |
  |  Checks          |     |  Execution time, |
  |  Nulls, freshness|     |  row counts,     |
  |  schema, counts  |     |  failures, Slack |
  |                  |     |                  |
  +------------------+     +------------------+
```

## Tech Stack

| Layer          | Technology                        |
|----------------|-----------------------------------|
| Extraction     | Python 3.11+, `requests`, `csv`   |
| Loading        | Python, DuckDB (local) / BigQuery  |
| Transformation | dbt-core 1.7+                     |
| Quality        | Custom Python checks + dbt tests  |
| Monitoring     | Python logging, Slack webhooks    |
| Orchestration  | Custom Python orchestrator        |
| CI/CD          | GitHub Actions                    |
| Testing        | pytest, mypy, ruff                |

## What This Demonstrates

- **ELT Architecture**: Extract-Load-Transform pattern with clear separation of concerns
- **dbt Best Practices**: Staging/marts layering, source definitions, documentation, and tests
- **Data Quality Engineering**: Null rate checks, freshness monitoring, schema validation, row count anomaly detection
- **Production Patterns**: Retry logic with exponential backoff, structured logging, graceful error handling
- **Pipeline Monitoring**: Execution timing, failure tracking, alerting hooks
- **Software Engineering**: Type hints throughout, comprehensive docstrings, pytest test suite, CI/CD pipeline
- **Clean Code**: Linted with ruff, type-checked with mypy, formatted consistently

## Quick Start

```bash
# Clone and set up
git clone https://github.com/IlyasI/data-pipeline-demo.git
cd data-pipeline-demo
make setup

# Run the full pipeline (extract -> load -> transform -> quality checks)
make run

# Run tests
make test

# Lint and type-check
make lint
```

## Project Structure

```
data-pipeline-demo/
  pipeline/
    extract.py          # Download and validate source data
    load.py             # Load raw data into warehouse
    orchestrate.py      # Pipeline orchestration with retry + logging
    quality.py          # Data quality checks framework
    monitor.py          # Execution monitoring and alerting
  models/
    staging/
      stg_orders.sql    # Orders staging model
      stg_customers.sql # Customers staging model
      stg_payments.sql  # Payments staging model
      schema.yml        # Source definitions and column tests
    marts/
      dim_customers.sql # Customer dimension table
      fct_orders.sql    # Orders fact table
      metrics_daily.sql # Daily business metrics rollup
      schema.yml        # Mart documentation and tests
  tests/
    test_pipeline.py    # Pipeline unit and integration tests
    test_quality.py     # Data quality framework tests
  .github/workflows/
    ci.yml              # CI: lint, test, dbt compile
  docs/
    architecture.md     # Architecture decisions and rationale
  dbt_project.yml
  Makefile
  requirements.txt
```

## Design Decisions

See [docs/architecture.md](docs/architecture.md) for detailed rationale on:

- Why ELT over ETL for this pipeline
- The staging/marts layering pattern
- Data quality strategy and trade-offs
- Monitoring approach and alerting
- How this architecture scales to terabyte-scale workloads

## Running with BigQuery

This project defaults to DuckDB for local development (zero setup required). To run against
BigQuery:

1. Set `PIPELINE_WAREHOUSE=bigquery` in your environment
2. Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to a service account JSON
3. Set `GCP_PROJECT` and `BQ_DATASET` environment variables
4. Update `profiles.yml` with your BigQuery connection details

```bash
export PIPELINE_WAREHOUSE=bigquery
export GCP_PROJECT=your-project
export BQ_DATASET=pipeline_demo
make run
```

## License

MIT
