import logging
import os
from typing import Iterable

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client() -> bigquery.Client:
    project_id = os.getenv("GCP_PROJECT_ID")
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        return bigquery.Client(project=project_id, credentials=credentials)
    return bigquery.Client(project=project_id)


def ensure_dataset(client: bigquery.Client, table_id: str) -> None:
    parts = table_id.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected table_id as project.dataset.table, got: {table_id}")

    project_id, dataset_id, _ = parts
    dataset_fqn = f"{project_id}.{dataset_id}"

    # First check if dataset already exists (does not need create permission).
    try:
        client.get_dataset(dataset_fqn)
        return
    except NotFound:
        pass

    # Create only when missing.
    dataset_ref = bigquery.Dataset(dataset_fqn)
    dataset_ref.location = os.getenv("BQ_LOCATION", "asia-south1")
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
    except Forbidden as exc:
        raise PermissionError(
            f"Dataset {dataset_fqn} does not exist and credentials cannot create datasets. "
            "Create it manually in BigQuery or grant bigquery.datasets.create permission."
        ) from exc


def load_dataframe(df: pd.DataFrame, table_id: str, write_disposition: str = "WRITE_APPEND") -> int:
    client = get_bq_client()
    ensure_dataset(client, table_id)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()
    logging.info("Loaded %s rows into %s", len(df.index), table_id)
    return len(df.index)


def ensure_columns(df: pd.DataFrame, required_cols: Iterable[str]) -> pd.DataFrame:
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    return df
