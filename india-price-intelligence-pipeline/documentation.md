# India Price Intelligence Pipeline

## Why this project?
We want to compare retail vs wholesale prices for common commodities in India and make the results easy to analyze and visualize. This project builds a pipeline that collects raw data, cleans it, transforms it into analytics tables, and publishes it in dashboards.

## What are we doing here?
We are building a data pipeline plus analytics stack that moves from raw API and scraper data to BigQuery analytics tables, then to Looker Studio dashboards, and schedules the full process using Airflow.

## Who is this for?
Anyone who wants to understand price movements and margins (students, analysts, or teams) without needing to manually clean or join multiple datasets.

## Where does the data come from?
- Mandi wholesale prices from the data.gov.in API.
- BigBasket retail prices scraped from the public website.

## When does it run?
Manually during development and on a schedule once Airflow is enabled. The pipeline is designed to run daily, but can also run in smaller test batches.

## How does it work (end-to-end flow)?
1. Ingest raw data
   - Mandi API data is fetched in pages.
   - BigBasket prices are scraped from product listings.
2. Load raw data into BigQuery
3. Transform data with dbt
   - Staging: clean raw tables
   - Intermediate: reconcile retail vs wholesale
   - Marts: final tables for dashboarding
4. Validate data with dbt tests
5. Orchestrate runs with Airflow
6. Visualize in Looker Studio

## What is the end goal?
A repeatable pipeline that reliably produces analytics tables like:
- mart_price_trends
- mart_margin_by_commodity
- mart_state_wholesale_comparison
- mart_pipeline_health
- mart_weekly_summary

These tables power a dashboard showing trends, margins, and comparisons.

---

# Project Structure

- ingestion/
  - mandi_api.py: fetch wholesale data
  - scraper.py: scrape retail data
  - bq_loader.py: load DataFrames into BigQuery
- dbt_project/
  - staging models: clean raw tables
  - intermediate models: reconcile data
  - marts models: final reporting tables
- airflow/
  - Airflow DAG to run ingestion + dbt

---

# Setup Sequence (exact order used)

1. Create a Python virtual environment (Python 3.11).
2. Install dependencies from requirements.txt.
3. Configure .env with API keys and BigQuery project details.
4. Set GOOGLE_APPLICATION_CREDENTIALS to the service account JSON.
5. (Optional) Set PIPELINE_* variables for schedule and alerting.
6. Run Mandi ingestion.
7. Run BigBasket scraper.
8. Run dbt seed, run, and test.
9. Start Airflow with Docker Compose.
10. Open Looker Studio and connect to BigQuery marts.

---

# Errors We Faced and How We Solved Them

## 1. API 429 Rate Limit
Problem: Mandi API returned Too Many Requests.
Fix: Added retries and delays, and allowed smaller test batches.

## 2. Missing BigQuery Credentials
Problem: DefaultCredentialsError.
Fix: Set GOOGLE_APPLICATION_CREDENTIALS and verified file path.

## 3. Dataset Not Found
Problem: 404 errors loading data into BigQuery.
Fix: Created dataset or enabled dataset auto-create (requires permissions).

## 4. dbt Syntax Error: Illegal input character "\357"
Problem: BOM characters in SQL files.
Fix: Removed BOM from SQL/YAML files.

## 5. dbt Staging Failed for BigBasket
Problem: raw_ecommerce.bigbasket_prices did not exist.
Fix: Run scraper first so the raw table exists.

## 6. Airflow DAG Import Errors
Problem: Airflow could not import ingestion modules.
Fix: Added ingestion/__init__.py and updated DAG import path.

## 7. Python 3.8 Typing Errors in Airflow
Problem: list[str] / dict[str, Any] not supported.
Fix: Replaced with typing.List and typing.Dict.

## 8. BigQuery Schema Mismatch
Problem: Added new column (source_url) but table schema didn’t allow it.
Fix: Removed extra column or updated schema explicitly.

---

# Current Status

- Mandi ingestion works
- BigBasket scraper loads data
- dbt models and tests pass
- Airflow UI runs and DAG is visible
- Looker Studio dashboard shows results

---

# How to Run (short checklist)

1. Set env vars and credentials
2. Run ingestion scripts
3. Run dbt transformations
4. Start Airflow
5. Open Looker Studio

---

# Shareable Summary

This project builds an end-to-end price intelligence pipeline for India. It pulls wholesale and retail data, cleans and reconciles it in BigQuery with dbt, validates it, schedules it in Airflow, and makes it visible in dashboards. It’s designed so a non-technical user can view clean insights without touching raw data.

---

# Architecture Diagram (ASCII)

[Data.gov.in API] ----> [Mandi Ingestion] ----\
                                               \
                                                --> [BigQuery: raw_ecommerce] --> [dbt: staging] --> [dbt: intermediate] --> [dbt: marts] --> [Looker Studio]
                                               /
[BigBasket Website] -> [Scraper Ingestion] ----/
                                                \
                                                 --> [Airflow DAG] schedules ingestion + dbt + tests

---

# Data Modeling Strategy

## Grain
- Raw tables: one row per raw record (API row or scraped product listing).
- Staging: one row per normalized record (cleaned and typed).
- Intermediate: one row per commodity per time period after reconciliation.
- Marts: one row per commodity per time period (or per state/time) depending on mart.

## Partitioning
- Time-based partitioning is used or recommended on date fields:
  - `arrival_date` for mandi data
  - `scraped_date` for retail data
  - `week` or `run_date` for marts

## Keys
- Natural keys at staging level (commodity + date + location).
- Surrogate or composite keys at intermediate/mart level (canonical_commodity + week or run_date).

## Naming Conventions
- Raw datasets: `raw_ecommerce`
- dbt datasets: `dbt_ecommerce`
- Model prefixes:
  - `stg_` for staging
  - `int_` for intermediate
  - `mart_` for final analytics

---

# Data Quality & Testing Strategy

## dbt generic tests
- `not_null` on critical fields (dates, commodity names, prices).
- `accepted_values` for constrained fields (source values like `mandi`/`bigbasket`).

## Custom tests
- Price > 0
- Margin within reasonable range
- Week not in the future

## Freshness tests
- Validate that new data arrives within expected time windows (daily).
- Alerts if data is stale beyond threshold.

## Threshold alerts
- Trigger alerts on:
  - High failure rates in dbt tests
  - Sudden drops in row counts
  - Large price jumps beyond expected bounds

---

# Operational Considerations

## Retry logic
- API calls retried on 403/429/5xx with backoff.
- Scraper includes retry + polite sleep between requests.

## Idempotency
- Ingestion designed to be re-runnable without duplicating data.
- dbt models are deterministic and can be rebuilt cleanly.

## Logging
- Structured logs for ingestion and dbt.
- Airflow logs each task run.

## Monitoring
- Airflow UI for DAG run status.
- dbt test results for data quality.

## Alerting
- Airflow task failures should trigger alerts (email/Slack).
- dbt test failures should surface in monitoring.
- Email alerts require SMTP settings (AIRFLOW__SMTP__* in .env).

## Backfills
- Mandi ingestion supports paging and offsets for historical backfills.
- dbt can rebuild marts for any time window.

---

# Future Roadmap

- Add more retailers (Blinkit, Zepto, Swiggy Instamart).
- Add anomaly detection for price spikes.
- Add forecasting models for commodity trends.
- Build a public API layer for downstream apps.
- Move scraper to a managed service (Cloud Run / Cloud Functions).
- Add CI/CD for dbt (automated tests + deploys).
