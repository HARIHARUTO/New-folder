# Technical Documentation: India AQI Weather Pipeline

## 1) Objective

Build a reliable, API-first data pipeline to answer:
- How AQI behaves across major Indian cities over time.
- Whether rainfall, humidity, and wind are correlated with AQI changes.

The pipeline is designed for production-style data engineering demonstration: ingestion validation, quarantine handling, orchestrated runs, and warehouse-level quality checks.

## 2) Data Sources

- AQI API (data.gov.in / CPCB resource)
  - Pollutant-level readings by city/station/timestamp.
- Weather API (OpenWeatherMap)
  - City-level weather observations (temperature, humidity, wind, rain).

## 3) End-to-End Flow

1. Airflow triggers ingestion tasks in parallel.
2. Ingestion code validates source payloads.
3. Valid rows are loaded to `raw_aqi` BigQuery tables.
4. Invalid rows are written to `raw_aqi.invalid_records`.
5. dbt executes:
   - staging models
   - intermediate join models
   - marts + feature model
6. dbt tests validate model quality.
7. Run summary task marks completion.

## 4) Warehouse Layers

- Raw dataset: `raw_aqi`
  - `aqi_readings`
  - `weather_readings`
  - `invalid_records`
- dbt dataset: `dbt_aqi`
  - `stg_*` cleaned and standardized views
  - `int_*` business-logic views
  - `mart_*` analytics tables
  - `features_city_air_quality` feature table

### Warehouse optimization

- Date/time marts are partitioned on event or run timestamps.
- City-focused marts are clustered by `canonical_city`.
- This reduces BigQuery scan cost for city and date-range filters.

## 5) Validation and Quarantine Strategy

- Validation runs before BigQuery load.
- If required fields are missing or malformed:
  - record is not loaded to main raw table
  - error metadata + payload goes to `invalid_records`
- This prevents bad upstream records from breaking dbt downstream.

## 6) Airflow Orchestration

DAG: `india_aqi_weather_pipeline`

Task sequence:
- `ingest_aqi_data` + `ingest_weather_data` (parallel)
- `dbt_seed`
- `dbt_run_staging`
- `dbt_run_intermediate`
- `dbt_run_marts`
- `dbt_test`
- `log_run_summary`

Operational settings:
- Retries and exponential backoff enabled.
- SLA configured in DAG default args.
- Schedule controlled via `PIPELINE_SCHEDULE_CRON` in `.env`.

## 7) dbt Testing Strategy

Implemented checks include:
- non-null critical keys/timestamps/city columns
- AQI category validity
- no impossible future timestamps (with tolerance window)
- quality metrics freshness check

## 8) Backfill

- `scripts/backfill_aqi.py`
  - date-range backfill with progress file support (`scripts/completed_dates.txt`)
- `scripts/backfill_weather.py`
  - historical weather backfill using Open-Meteo archive endpoint

## 9) Known Constraints

- BigQuery free tier restricts DML-heavy patterns.
  - `mart_data_quality_metrics` is materialized as a table (not incremental DML) for compatibility.
- Local Python and Airflow container Python versions differ.
  - dependencies are pinned in `requirements.txt` and `airflow/Dockerfile` accordingly.

## 10) Runbook

Start services:

```bash
docker compose up -d
```

Trigger DAG:

```bash
docker compose exec airflow-webserver airflow dags trigger india_aqi_weather_pipeline
```

Check run status:

```bash
docker compose exec airflow-webserver airflow dags list-runs -d india_aqi_weather_pipeline --no-backfill
```

Check task-level states (use actual run id):

```bash
docker compose exec airflow-webserver airflow tasks states-for-dag-run india_aqi_weather_pipeline manual__2026-03-06T05:31:48+00:00
```

PowerShell helper to inspect the latest running DAG run:

```powershell
$rid=(docker compose exec airflow-webserver airflow dags list-runs -d india_aqi_weather_pipeline --no-backfill | Select-String "running" | Select-Object -First 1).ToString().Split("|")[1].Trim(); Write-Host "RunId=$rid"; docker compose exec airflow-webserver airflow tasks states-for-dag-run india_aqi_weather_pipeline $rid
```

## 11) Operational Timeline (2026-03-06)

- Airflow stack came up successfully after folder/path cleanup.
- Early manual runs stayed queued because several triggers were submitted while `max_active_runs=1`.
- One `ingest_aqi_data` attempt moved to `up_for_retry` due to `SIGTERM` (task interrupted by service restart), not data/code failure.
- Subsequent runs completed end-to-end with all tasks successful:
  - `ingest_aqi_data`
  - `ingest_weather_data`
  - `dbt_seed`
  - `dbt_run_staging`
  - `dbt_run_intermediate`
  - `dbt_run_marts`
  - `dbt_test`
  - `log_run_summary`

## 12) Troubleshooting Notes

- If all tasks show `None`, first check run status (`queued` vs `running` vs `success`).
- If many runs are queued, do not trigger repeatedly; wait for current run completion.
- If a task shows `SIGTERM`, inspect whether scheduler/webserver/container was restarted during task execution.
- Use:
  - `docker compose exec airflow-webserver airflow dags list-import-errors`
  - `docker compose logs --tail=200 airflow-scheduler`
  - task log path under `/opt/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=...log`

## 13) Interview Positioning

This project demonstrates:
- source contract validation
- warehouse modeling discipline (staging/intermediate/marts)
- orchestration and operational debugging
- measurable data quality controls
- feature-ready outputs for downstream ML
