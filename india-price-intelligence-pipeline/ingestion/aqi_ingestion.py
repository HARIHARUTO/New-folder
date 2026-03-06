import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from dotenv import load_dotenv

if __package__ in (None, ""):
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.bq_loader import load_dataframe, load_quarantine_records, setup_logging, to_json_string
from ingestion.schema_validator import validate_aqi_record, summarize_schema_error
from ingestion.utils import paginate_api, parse_datetime_str, safe_float

DEFAULT_AQI_RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
BASE_URL = "https://api.data.gov.in/resource"


def normalize_record(record: Dict[str, Any], fetched_at: datetime) -> Dict[str, Any]:
    reading_ts = parse_datetime_str(record.get("last_update", ""))
    avg_val = record.get("avg_value", record.get("pollutant_avg"))
    min_val = record.get("min_value", record.get("pollutant_min"))
    max_val = record.get("max_value", record.get("pollutant_max"))
    return {
        "station_id": record.get("id"),
        "station_name": record.get("station"),
        "city": record.get("city"),
        "state": record.get("state"),
        "pollutant_id": record.get("pollutant_id"),
        "pollutant_avg": safe_float(avg_val),
        "pollutant_min": safe_float(min_val),
        "pollutant_max": safe_float(max_val),
        "pollutant_unit": record.get("pollutant_unit"),
        "reading_timestamp": reading_ts,
        "fetched_at": fetched_at,
    }


def run_aqi_ingestion() -> int:
    load_dotenv()
    setup_logging()

    api_key = os.getenv("DATA_GOV_API_KEY")
    if not api_key:
        raise ValueError("DATA_GOV_API_KEY is missing")

    resource_id = os.getenv("AQI_RESOURCE_ID", DEFAULT_AQI_RESOURCE_ID)

    limit = int(os.getenv("AQI_LIMIT", "100"))
    offset = int(os.getenv("AQI_START_OFFSET", "0"))
    max_pages_env = os.getenv("AQI_MAX_PAGES")
    max_pages = int(max_pages_env) if max_pages_env else None

    base_url = f"{BASE_URL}/{resource_id}"
    raw_records = paginate_api(base_url, api_key, limit=limit, offset=offset, max_pages=max_pages)

    fetched_at = datetime.now(timezone.utc)
    good_rows: List[Dict[str, Any]] = []
    bad_rows: List[Dict[str, Any]] = []

    for record in raw_records:
        try:
            validate_aqi_record(record)
            good_rows.append(normalize_record(record, fetched_at))
        except Exception as exc:
            field, reason = ("unknown", str(exc))
            try:
                field, reason = summarize_schema_error(exc)  # type: ignore[arg-type]
            except Exception:
                pass
            bad_rows.append(
                {
                    "source": "aqi",
                    "error_reason": reason,
                    "error_field": field,
                    "raw_payload": to_json_string(record),
                    "quarantined_at": fetched_at,
                }
            )

    if not good_rows:
        logging.warning("No valid AQI records fetched")

    project = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW", "raw_aqi")
    table_id = f"{project}.{dataset_raw}.aqi_readings"
    quarantine_id = f"{project}.{dataset_raw}.invalid_records"

    loaded = load_dataframe(pd.DataFrame(good_rows), table_id) if good_rows else 0
    quarantined = load_quarantine_records(bad_rows, quarantine_id)

    logging.info("AQI loaded=%s quarantined=%s", loaded, quarantined)
    return loaded


if __name__ == "__main__":
    run_aqi_ingestion()
