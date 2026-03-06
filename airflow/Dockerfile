FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-bigquery==1.7.0 \
    google-cloud-bigquery==3.13.0 \
    requests==2.31.0 \
    pandas==1.5.3 \
    python-dotenv==1.0.0
