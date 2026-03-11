import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from ingestion.bq_loader import load_dataframe, setup_logging
from ingestion.utils import paginate_api, parse_datetime_str, safe_float

AQI_RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
BASE_URL = "https://api.data.gov.in/resource"


def date_range(start: datetime, end: datetime):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def load_completed(path: str) -> set:
    if not os.path.exists(path):
        return set()
    with open(path, "r", encoding="utf-8") as handle:
        return set(line.strip() for line in handle if line.strip())


def append_completed(path: str, date_str: str) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(date_str + "\n")


def normalize_record(record, fetched_at):
    reading_ts = parse_datetime_str(record.get("last_update", ""))
    return {
        "station_id": record.get("id"),
        "station_name": record.get("station"),
        "city": record.get("city"),
        "state": record.get("state"),
        "pollutant_id": record.get("pollutant_id"),
        "pollutant_avg": safe_float(record.get("pollutant_avg")),
        "pollutant_min": safe_float(record.get("pollutant_min")),
        "pollutant_max": safe_float(record.get("pollutant_max")),
        "pollutant_unit": record.get("pollutant_unit"),
        "reading_timestamp": reading_ts,
        "fetched_at": fetched_at,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--resume-file", default="scripts/completed_dates.txt")
    args = parser.parse_args()

    load_dotenv()
    setup_logging()

    api_key = os.getenv("DATA_GOV_API_KEY")
    if not api_key:
        raise ValueError("DATA_GOV_API_KEY is missing")

    project = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW", "raw_aqi")
    table_id = f"{project}.{dataset_raw}.aqi_readings"

    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    completed = load_completed(args.resume_file)

    base_url = f"{BASE_URL}/{AQI_RESOURCE_ID}"
    limit = int(os.getenv("AQI_LIMIT", "100"))

    for day in date_range(start_date, end_date):
        day_str = day.strftime("%Y-%m-%d")
        if day_str in completed:
            continue

        records = paginate_api(base_url, api_key, limit=limit, offset=0, max_pages=None)
        fetched_at = datetime.utcnow()
        rows = []
        for record in records:
            parsed = parse_datetime_str(record.get("last_update", ""))
            if parsed and parsed.date() == day.date():
                rows.append(normalize_record(record, fetched_at))

        if rows:
            load_dataframe(pd.DataFrame(rows), table_id)

        append_completed(args.resume_file, day_str)
        time.sleep(2)


if __name__ == "__main__":
    main()
