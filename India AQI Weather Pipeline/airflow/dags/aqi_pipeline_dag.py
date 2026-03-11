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

from ingestion.aqi_ingestion import run_aqi_ingestion  # noqa: E402
from ingestion.weather_ingestion import run_weather_ingestion  # noqa: E402


def log_run_summary() -> None:
    print("AQI + Weather pipeline run completed")


default_args = {
    "owner": "data-platform",
    "retries": int(os.getenv("PIPELINE_RETRIES", "2")),
    "retry_delay": timedelta(minutes=int(os.getenv("PIPELINE_RETRY_DELAY_MIN", "10"))),
    "email_on_failure": bool(os.getenv("PIPELINE_ALERT_EMAIL")),
    "email": [os.getenv("PIPELINE_ALERT_EMAIL")] if os.getenv("PIPELINE_ALERT_EMAIL") else [],
    "retry_exponential_backoff": True,
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="india_aqi_weather_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=os.getenv("PIPELINE_SCHEDULE_CRON", "0 * * * *"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["data-engineering", "aqi", "weather", "hourly"],
) as dag:
    ingest_aqi_data = PythonOperator(
        task_id="ingest_aqi_data",
        python_callable=run_aqi_ingestion,
    )

    ingest_weather_data = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=run_weather_ingestion,
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

    log_run_summary_task = PythonOperator(
        task_id="log_run_summary",
        python_callable=log_run_summary,
    )

    [ingest_aqi_data, ingest_weather_data] >> dbt_seed
    dbt_seed >> dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts >> dbt_test >> log_run_summary_task
