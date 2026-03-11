import json
import logging
import os
from typing import Any, Dict, Iterable, Optional

import pandas as pd
from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

LOGGER_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOGGER_FORMAT)


def get_bq_client() -> bigquery.Client:
    project_id = os.getenv("GCP_PROJECT_ID")
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        credentials = service_account.Credentials.from_service_account_file(cred_path)
        return bigquery.Client(project=project_id, credentials=credentials)
    return bigquery.Client(project=project_id)


def ensure_dataset(client: bigquery.Client, dataset_id: str) -> None:
    project_id = client.project
    dataset_fqn = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_fqn)
        return
    except NotFound:
        pass

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
    dataset_id = table_id.split(".")[1]
    ensure_dataset(client, dataset_id)
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


def load_quarantine_records(
    records: Iterable[Dict[str, Any]],
    table_id: str,
) -> int:
    if not records:
        return 0
    df = pd.DataFrame(records)
    return load_dataframe(df, table_id)


def to_json_string(value: Dict[str, Any]) -> str:
    try:
        return json.dumps(value, ensure_ascii=True)
    except TypeError:
        return json.dumps({"raw": str(value)}, ensure_ascii=True)