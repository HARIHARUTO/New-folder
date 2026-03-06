# DEVLOG

## Week 1: Foundation

- Finalized API-first direction for AQI + weather.
- Implemented ingestion loaders and BigQuery raw landing.

## Week 2: Modeling and Quality

- Built dbt staging/intermediate/marts.
- Added tests and quarantine-aware data quality flow.

## Week 3: Orchestration

- Added Airflow DAG for parallel ingestion and sequential dbt.
- Tuned retries and scheduler settings for local stability.

## 2026-03-06: Reliability Debug Session

- Observed queued runs and `None` task states.
- Identified `max_active_runs=1` behavior and trigger backlog effect.
- Confirmed one `SIGTERM` came from service restart during run.
- Validated repeated successful end-to-end DAG runs after stabilization.

## First Full Green DAG Run - 2026-03-06

Run ID: `manual__2026-03-06T06:50:49+00:00`

Task breakdown:

- `ingest_weather_data`: about 23 seconds
- `ingest_aqi_data`: about 5 minutes (API pagination)
- `dbt_seed` -> `dbt_run_staging` -> `dbt_run_intermediate` -> `dbt_run_marts`: about 3 minutes total
- `dbt_test`: all assertions passed
- `log_run_summary`: completion confirmed

Notable:

- `ingest_aqi_data` is the main runtime bottleneck because it paginates through a large AQI response set.
- Near-term optimization path is incremental ingestion to fetch only new or changed records.
