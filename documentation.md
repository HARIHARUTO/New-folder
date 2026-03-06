# India AQI Weather Pipeline: Ground-Truth Project Story

## What this project is

This is an end-to-end data engineering project built to answer one practical question:

- How does weather impact AQI in Indian cities?

The pipeline collects AQI data from data.gov.in, weather data from OpenWeatherMap, loads both into BigQuery, transforms with dbt, and orchestrates everything in Airflow.

As of March 6, 2026, the DAG runs are passing end-to-end.

## Why I built it this way

I wanted this project to be:

- reliable enough to run repeatedly
- easy to debug when something breaks
- clean enough for a recruiter or senior data engineer to review quickly

So instead of just writing one script, I designed it as a small data platform:

- ingestion layer
- validation + quarantine layer
- warehouse layer
- transformation layer
- orchestration layer

## The journey (real sequence)

### Phase 1: Make ingestion work

I first focused on getting both sources into the same warehouse:

- AQI API -> `raw_aqi.aqi_readings`
- Weather API -> `raw_aqi.weather_readings`

At this stage the priority was getting data moving, not perfect modeling.

### Phase 2: Make data trustworthy

Once ingestion worked, I added schema checks before loading:

- valid records go to main raw tables
- invalid records go to `raw_aqi.invalid_records`

This prevented bad records from silently polluting downstream models.

### Phase 3: Build dbt layers

I split transformations into:

- `stg_*`: clean and standardize
- `int_*`: business logic and joining
- `mart_*`: analytics outputs
- `features_*`: ML-friendly feature table

This made debugging much easier than one large SQL model.

### Phase 4: Orchestrate in Airflow

I created DAG `india_aqi_weather_pipeline`:

1. `ingest_aqi_data` and `ingest_weather_data` in parallel
2. `dbt_seed`
3. `dbt_run_staging`
4. `dbt_run_intermediate`
5. `dbt_run_marts`
6. `dbt_test`
7. `log_run_summary`

Now one trigger runs the full pipeline in order.

### Phase 5: Stabilize and productionize

I fixed operational issues and made the project more realistic:

- custom Airflow image for dependency compatibility
- timezone normalization for AQI timestamps (IST -> UTC)
- realistic dbt test tolerance for future timestamps
- free-tier compatible dbt materialization choice for quality mart
- partition/cluster settings on marts for better query efficiency

## Main problems I faced and how I fixed them

### 1) Airflow containers crashing on package install

- Problem: runtime pip install inside Airflow was fragile.
- Fix: built a custom Airflow image with pinned dependencies.

### 2) Local ingestion import errors

- Problem: `python ingestion/...` failed with `ModuleNotFoundError`.
- Fix: added package path bootstrap in ingestion entry scripts.

### 3) dbt marts failing due to environment constraints

- Problem: `mart_data_quality_metrics` failed under current billing/runtime restrictions.
- Fix: changed this model to `table` materialization for compatibility.

### 4) Future timestamp test failures

- Problem: AQI source timestamps were interpreted without explicit local timezone handling.
- Fix: parse as `Asia/Kolkata`, convert to UTC, then test with practical tolerance.

### 5) Airflow CLI run-id errors during manual checks

- Problem: using placeholder run IDs caused parsing errors.
- Fix: always copy real run ID from `airflow dags list-runs`.

## Current architecture in plain words

- Source APIs provide AQI and weather.
- Python ingestion validates and loads data to BigQuery raw.
- dbt builds cleaned and analytical layers.
- Airflow schedules and controls task dependencies.
- Tests and quality marts monitor data health.

## Warehouse details

- Raw dataset: `raw_aqi`
  - `aqi_readings`
  - `weather_readings`
  - `invalid_records`

- dbt dataset: `dbt_aqi`
  - staging models
  - intermediate models
  - mart models
  - feature model

## Optimizations implemented

- Partitioned marts:
  - `mart_city_aqi_trends` by `reading_date`
  - `mart_pollutant_breakdown` by `reading_date`
  - `mart_rain_impact` by `rain_event_date`
  - `mart_pipeline_health` by `run_timestamp`
  - `mart_data_quality_metrics` by `run_timestamp`
  - `features_city_air_quality` by `reading_hour`

- Clustered city-heavy marts by `canonical_city`.

## What someone should learn from this project

1. Make ingestion robust before making dashboards pretty.
2. Add validation early; quarantine bad records instead of ignoring them.
3. Keep SQL transformations layered and reviewable.
4. Treat orchestration as part of the product, not an afterthought.
5. Debugging real runs is where most engineering learning happens.

## What is still left (next iteration)

- Add automated alerting (email/Slack/PagerDuty style).
- Add CI checks for dbt/tests on PR.
- Move secrets from local mounts to secret manager in a cloud setup.

## Quick runbook

### Start services
```bash
docker compose up -d
```

### Trigger pipeline
```bash
docker compose exec airflow-webserver airflow dags trigger india_aqi_weather_pipeline
```

### View runs
```bash
docker compose exec airflow-webserver airflow dags list-runs -d india_aqi_weather_pipeline --no-backfill
```

### View task states for a run
```bash
docker compose exec airflow-webserver airflow tasks states-for-dag-run india_aqi_weather_pipeline <actual_run_id>
```

### Local full check
```bash
python ingestion/aqi_ingestion.py
python ingestion/weather_ingestion.py
cd dbt_project
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Final note

This document is intentionally written as a transparent engineering journey.
It includes what worked, what broke, and what was changed to make the pipeline stable.
