# India AQI Weather Pipeline

API-driven data engineering project that correlates Indian city AQI with weather conditions using BigQuery, dbt, and Airflow.

## What It Does

1. Ingests AQI data from data.gov.in (CPCB resource API).
2. Ingests weather data from OpenWeatherMap.
3. Validates records and quarantines schema/type failures.
4. Loads raw data to BigQuery (`raw_aqi`).
5. Builds staging, intermediate, mart, and feature tables with dbt (`dbt_aqi`).
6. Runs quality tests and pipeline-health metrics.
7. Orchestrates the full flow in Airflow.

## Architecture

AQI API + Weather API -> Python ingestion + validation -> BigQuery raw -> dbt models -> Airflow orchestration -> Looker Studio

## Tech Stack

- Python 3.11 (local)
- requests, pandas, python-dotenv
- Google BigQuery
- dbt Core + dbt-bigquery
- Apache Airflow 2.8 (Docker Compose)
- Docker

## Project Structure

```text
airflow/
  dags/aqi_pipeline_dag.py
  Dockerfile
ingestion/
  aqi_ingestion.py
  weather_ingestion.py
  schema_validator.py
  bq_loader.py
  utils.py
dbt_project/
  models/
  seeds/
  tests/
scripts/
  backfill_aqi.py
  backfill_weather.py
data/
  cities.json
docker-compose.yml
```

## Setup

1. Copy `.env.example` to `.env`.
2. Fill required env vars:
   - `GCP_PROJECT_ID`
   - `DATA_GOV_API_KEY`
   - `OPENWEATHER_API_KEY`
   - `GOOGLE_APPLICATION_CREDENTIALS` (container path is mounted automatically)
3. Place `credentials.json` in repo root.
4. Ensure BigQuery datasets exist:
   - `raw_aqi`
   - `dbt_aqi`

## Run Locally (without Airflow)

```bash
python ingestion/aqi_ingestion.py
python ingestion/weather_ingestion.py
cd dbt_project
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Run with Airflow

```bash
docker compose up -d
docker compose exec airflow-webserver airflow dags trigger india_aqi_weather_pipeline
docker compose exec airflow-webserver airflow dags list-runs -d india_aqi_weather_pipeline --no-backfill
```

To inspect a specific run, use the actual run id from `list-runs`:

```bash
docker compose exec airflow-webserver airflow tasks states-for-dag-run india_aqi_weather_pipeline manual__2026-03-06T05:31:48+00:00
```

PowerShell helper to fetch latest running run and task states:

```powershell
$rid=(docker compose exec airflow-webserver airflow dags list-runs -d india_aqi_weather_pipeline --no-backfill | Select-String "running" | Select-Object -First 1).ToString().Split("|")[1].Trim(); Write-Host "RunId=$rid"; docker compose exec airflow-webserver airflow tasks states-for-dag-run india_aqi_weather_pipeline $rid
```

Airflow UI: `http://localhost:8080` (`airflow` / `airflow`)

## DAG Tasks

- `ingest_aqi_data`
- `ingest_weather_data`
- `dbt_seed`
- `dbt_run_staging`
- `dbt_run_intermediate`
- `dbt_run_marts`
- `dbt_test`
- `log_run_summary`

## Data Quality Controls

- Pre-load schema validation for ingestion.
- Quarantine table: `raw_aqi.invalid_records`.
- dbt tests for nulls, category validity, future timestamps, and freshness.
- Mart-level quality metrics via `mart_data_quality_metrics`.

## Warehouse Optimizations

- Marts are materialized as BigQuery tables for predictable query performance.
- Time-series marts are partitioned:
  - `mart_city_aqi_trends` by `reading_date`
  - `mart_pollutant_breakdown` by `reading_date`
  - `mart_rain_impact` by `rain_event_date`
  - `mart_pipeline_health` by `run_timestamp`
  - `mart_data_quality_metrics` by `run_timestamp`
  - `features_city_air_quality` by `reading_hour`
- City-heavy marts are clustered by `canonical_city` for better pruning.

## Backfill

AQI:

```bash
python scripts/backfill_aqi.py --start 2024-01-01 --end 2024-03-01
```

Weather:

```bash
python scripts/backfill_weather.py --start 2024-01-01 --end 2024-03-01
```

## Current Status

- End-to-end Airflow DAG runs are passing.
- Local ingestion + dbt run/test are passing.
- Project has been migrated from scraper-based approach to API-only ingestion.

## Recent Ops Notes (2026-03-06)

- Multiple manual triggers created queued runs due to `max_active_runs=1`.
- `ingest_aqi_data` showed `SIGTERM` once because scheduler/webserver restart interrupted the task; subsequent retries/runs succeeded.
- Seeing all task states as `None` usually means the run is still queued and not yet picked by scheduler.
- Current live behavior is healthy: ingestion -> dbt seed/run/test -> log summary completes successfully.
