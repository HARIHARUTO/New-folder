import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DAG_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from ingestion.scraper import run_bigbasket_scraper  # noqa: E402
from ingestion.mandi_api import run_mandi_fetcher  # noqa: E402


def log_pipeline_completion() -> None:
    print("Pipeline completed successfully")


default_args = {
    "owner": "data-platform",
    "retries": int(os.getenv("PIPELINE_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("PIPELINE_RETRY_DELAY_MIN", "10"))),
    "email_on_failure": bool(os.getenv("PIPELINE_ALERT_EMAIL")),
    "email": [os.getenv("PIPELINE_ALERT_EMAIL")] if os.getenv("PIPELINE_ALERT_EMAIL") else [],
}

with DAG(
    dag_id="india_price_intelligence_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=os.getenv("PIPELINE_SCHEDULE_CRON", "0 6 * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["data-engineering", "india-prices", "weekly"],
) as dag:
    scrape_bigbasket = PythonOperator(
        task_id="scrape_bigbasket",
        python_callable=run_bigbasket_scraper,
    )

    fetch_mandi_api = PythonOperator(
        task_id="fetch_mandi_api",
        python_callable=run_mandi_fetcher,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd /opt/airflow/dbt_project && dbt source freshness --profiles-dir .",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt_project && dbt seed --profiles-dir .",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select staging --profiles-dir .",
    )

    dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select intermediate --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/dbt_project && dbt run --select marts --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
    )

    log_success = PythonOperator(
        task_id="log_success",
        python_callable=log_pipeline_completion,
    )

    [scrape_bigbasket, fetch_mandi_api] >> dbt_source_freshness >> dbt_seed
    dbt_seed >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test >> log_success