# India Retail Price Intelligence Pipeline

Tracks farm-to-fork price gap by combining BigBasket retail scraping with Government of India mandi wholesale prices.

## What This Project Does

A weekly automated data pipeline that:
1. Scrapes retail product prices from BigBasket.
2. Pulls mandi wholesale prices from data.gov.in.
3. Loads both sources to BigQuery raw tables.
4. Transforms and reconciles commodity names via dbt.
5. Produces marts for trend, margin, state comparison, and pipeline health.
6. Orchestrates everything with Airflow on a weekly schedule.
7. Powers a Looker Studio dashboard.

## Architecture Flow

BigBasket Scraper + data.gov.in API -> BigQuery raw dataset -> dbt staging/intermediate/marts -> Looker Studio
Orchestration: Airflow DAG (weekly, Sunday 06:00)

## Current Progress (as of 2026-03-03)

Completed:
- Ingestion scripts for Mandi API and retail scraper.
- BigQuery raw tables and dbt models (staging, intermediate, marts).
- dbt tests + custom data quality checks.
- Airflow DAG orchestration with retries and source freshness check.
- Docker Compose setup for Airflow + Postgres.
- Looker Studio dashboard connected to marts.
- Source freshness rules + partitioned marts for performance.

In progress:
- Real-time retail scraping stability (site availability/anti-bot).
- Email alerting (SMTP config via Airflow).
- Consistent manual trigger + log visibility on Airflow UI.

## Tech Stack

- Python 3.11
- requests, BeautifulSoup4, pandas, python-dotenv
- Google BigQuery
- dbt Core (dbt-bigquery)
- Apache Airflow 2.8 (Docker Compose)
- Looker Studio

## Folder Structure

```text
india-price-intelligence-pipeline/
  ingestion/
  dbt_project/
  airflow/
  .env.example
  .gitignore
  requirements.txt
  docker-compose.yml
  README.md
```

## Setup

1. Create GCP project and enable BigQuery API.
2. Create service account with BigQuery permissions and download `credentials.json` in repo root.
3. Register on data.gov.in and get API key.
4. Copy `.env.example` to `.env` and fill values.
5. Install dependencies:

```bash
pip install -r requirements.txt
```

6. Create BigQuery datasets:
- `raw_ecommerce`
- `dbt_ecommerce`

## BigQuery Raw Tables

`raw_ecommerce.bigbasket_prices`
- product_name STRING
- category STRING
- subcategory STRING
- price_raw STRING
- price_per_kg FLOAT64
- unit_raw STRING
- discount_percent FLOAT64
- scraped_at TIMESTAMP
- scraped_date DATE

`raw_ecommerce.mandi_prices`
- state STRING
- district STRING
- market STRING
- commodity STRING
- variety STRING
- arrival_date DATE
- min_price_per_kg FLOAT64
- max_price_per_kg FLOAT64
- modal_price_per_kg FLOAT64
- fetched_at TIMESTAMP

## Run Ingestion Locally

```bash
python ingestion/scraper.py
python ingestion/mandi_api.py
```

## Run dbt

```bash
cd dbt_project
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Run Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Airflow UI: `http://localhost:8080` (airflow / airflow)

## DAG Tasks

- `scrape_bigbasket` and `fetch_mandi_api` run in parallel
- `dbt_seed`
- `dbt_run_staging`
- `dbt_run_intermediate`
- `dbt_run_marts`
- `dbt_test`
- `log_success`

## Data Challenges Solved

- Price strings with currency symbols and commas.
- Mixed units (g, kg, piece) normalized to per-kg.
- API pagination and offset-based iteration.
- Commodity name mismatch handled using dbt seed mapping.
- Filtering invalid/zero price rows.

## Looker Studio Pages

1. Price Tracker
2. Margin Analysis
3. State Wholesale Prices
4. Pipeline Health

## Interview Summary

I built an end-to-end weekly pipeline to answer which commodities in India have the largest farm-to-fork margin gap by joining retail and wholesale price systems with data quality checks and reproducible orchestration.
