# Architecture Decisions

This document explains the key architectural decisions in this pipeline and how they
would translate to a production environment handling terabytes of data.

## Why ELT Over ETL

This pipeline uses ELT (Extract-Load-Transform) rather than ETL (Extract-Transform-Load):

**In this demo:**
- Raw data is loaded into the warehouse as-is, preserving the original schema
- Transformations happen inside the warehouse using dbt (SQL)
- The Python layer handles extraction and loading only, not business logic

**Why this matters:**
- **Separation of concerns**: Business logic lives in version-controlled SQL, not buried in Python scripts. Analysts can read, review, and modify transformations without touching the pipeline code.
- **Warehouse-native performance**: Modern warehouses (BigQuery, Snowflake, Redshift) are optimized for columnar operations. Running transformations inside the warehouse is almost always faster than doing it in Python.
- **Debuggability**: When a metric looks wrong, you can query the raw table directly to see what the source data looked like. With ETL, the raw data is often lost after transformation.
- **Reprocessing**: If a transformation bug is found, you re-run dbt. You don't need to re-extract from source systems.

## The Staging/Marts Pattern

The dbt models follow the staging/marts layering pattern:

```
Raw (loaded by Python) -> Staging (dbt views) -> Marts (dbt tables)
```

### Staging Layer (`models/staging/`)
- One model per source table
- Renaming columns to consistent snake_case
- Explicit type casting
- Deduplication on primary keys
- Filtering invalid records
- Materialized as **views** (zero storage cost, always up-to-date)

### Marts Layer (`models/marts/`)
- Business-oriented tables: dimensions, facts, metrics
- Joins across staging models
- Business logic (customer tiers, daily aggregations)
- Materialized as **tables** (query performance for dashboards)

**Why this matters:**
- Staging models create a clean contract between raw data and business logic. If the source schema changes, only the staging model needs updating.
- Marts are what analysts and dashboards query. They're named for business concepts (dim_customers, fct_orders, metrics_daily), not source system tables.

## Data Quality Strategy

Quality checks run at two levels:

### 1. dbt Tests (schema.yml)
- Column-level: not_null, unique, accepted_values, relationships
- Run after every dbt build
- Catch issues in the transformation layer

### 2. Python Quality Checks (pipeline/quality.py)
- Source-level: null rates, row counts, schema validation, referential integrity
- Run after loading, before transformation
- Catch issues in the extraction/loading layer

**Why both?**
- dbt tests validate the transformed output. They answer "is the data correct after our business logic?"
- Python checks validate the raw input. They answer "did we receive the data we expected?"
- A data quality issue in the source might not surface in dbt tests if a staging model silently filters bad records. The Python checks catch it first.

**Production considerations:**
- Quality results would be stored in a dedicated table for trend analysis
- Thresholds would be tuned based on historical data (not hardcoded)
- Great Expectations or Soda could replace the custom framework for more complex checks

## Monitoring Approach

The monitoring module (`pipeline/monitor.py`) tracks:

1. **Execution timing**: How long each stage takes, with anomaly detection when a stage runs significantly slower or faster than expected
2. **Row counts**: How many rows were processed, with alerts for significant drops
3. **Failure tracking**: Persistent history of successes and failures
4. **Alerting**: Slack webhook integration with cooldown to prevent alert storms

**Design decisions:**
- **Local history file**: The execution history is stored in a JSON file. In production, this would be a time-series database (InfluxDB) or a metrics platform (Datadog, CloudWatch).
- **Statistical anomaly detection**: Uses mean and standard deviation from recent history. Simple but effective for catching "something changed" situations.
- **Alert cooldown**: Prevents flooding the alert channel during cascading failures. If the extract stage fails and causes load to fail, you get one alert, not ten.

## How This Scales

Here's how each component would change for a real company processing terabytes:

### Extraction
- **Demo**: Single-threaded HTTP downloads of small CSV files
- **Production**: Parallel extraction from APIs, databases, and event streams. Tools like Fivetran, Airbyte, or custom connectors with connection pooling and incremental extraction (only pulling new/changed records since the last run).

### Loading
- **Demo**: DuckDB with full table replacement
- **Production**: BigQuery load jobs with WRITE_APPEND, partitioned by `_loaded_at`. For streaming data, Pub/Sub to BigQuery via a Dataflow job. For bulk loads, GCS staging with `bq load`.

### Transformation
- **Demo**: dbt with DuckDB adapter
- **Production**: dbt Cloud or dbt Core running against BigQuery/Snowflake. Incremental models (processing only new data) instead of full rebuilds. Model dependencies managed by dbt's DAG.

### Orchestration
- **Demo**: Custom Python orchestrator
- **Production**: Airflow, Dagster, or Prefect. The custom orchestrator demonstrates the same patterns (retry, logging, fail-fast, monitoring hooks) that these tools provide out of the box.

### Quality
- **Demo**: Custom Python checks on CSV files
- **Production**: dbt tests on warehouse tables, plus Great Expectations or Monte Carlo for automated anomaly detection, data profiling, and lineage tracking.

### Monitoring
- **Demo**: JSON file history with Slack webhooks
- **Production**: Datadog dashboards, PagerDuty escalation, SLA tracking. Metrics emitted to CloudWatch or Prometheus. Cost monitoring to catch runaway queries.

The architecture stays the same. The tools get more robust, but the ELT pattern, staging/marts layers, quality-first approach, and separation of concerns all carry over directly.
