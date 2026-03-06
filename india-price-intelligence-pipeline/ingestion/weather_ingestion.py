import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd
from dotenv import load_dotenv

if __package__ in (None, ""):
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ingestion.bq_loader import load_dataframe, load_quarantine_records, setup_logging, to_json_string
from ingestion.schema_validator import validate_weather_record, summarize_schema_error, weather_rainfall
from ingestion.utils import retry_request


def load_cities() -> List[Dict[str, float]]:
    path = os.getenv("CITIES_FILE")
    if not path:
        path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "cities.json"))
    if not os.path.exists(path):
        raise FileNotFoundError(f"Cities file not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def normalize_weather(record: Dict[str, object], fetched_at: datetime) -> Dict[str, object]:
    recorded_at = datetime.fromtimestamp(int(record["dt"]), tz=timezone.utc)
    main = record.get("main") or {}
    wind = record.get("wind") or {}
    weather = record.get("weather") or [{}]
    return {
        "city": record.get("name"),
        "recorded_at": recorded_at,
        "temp_celsius": main.get("temp"),
        "feels_like_celsius": main.get("feels_like"),
        "humidity_pct": main.get("humidity"),
        "wind_speed_mps": wind.get("speed"),
        "wind_direction_degrees": wind.get("deg"),
        "weather_condition": weather[0].get("main") if weather else None,
        "rainfall_1h_mm": weather_rainfall(record),
        "visibility_meters": record.get("visibility"),
        "pressure_hpa": main.get("pressure"),
        "fetched_at": fetched_at,
    }


def run_weather_ingestion() -> int:
    load_dotenv()
    setup_logging()

    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY is missing")

    fetched_at = datetime.now(timezone.utc)
    good_rows: List[Dict[str, object]] = []
    bad_rows: List[Dict[str, object]] = []

    for city in load_cities():
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": f"{city['city']},IN",
            "appid": api_key,
            "units": "metric",
        }
        try:
            resp = retry_request(url, params=params, retries=3, backoff_base=2.0, timeout=20)
            payload = resp.json()
            validate_weather_record(payload)
            good_rows.append(normalize_weather(payload, fetched_at))
        except Exception as exc:
            field, reason = ("unknown", str(exc))
            try:
                field, reason = summarize_schema_error(exc)  # type: ignore[arg-type]
            except Exception:
                pass
            bad_rows.append(
                {
                    "source": "weather",
                    "error_reason": reason,
                    "error_field": field,
                    "raw_payload": to_json_string({"city": city["city"], "error": str(exc)}),
                    "quarantined_at": fetched_at,
                }
            )
        time.sleep(1.0)

    project = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW", "raw_aqi")
    table_id = f"{project}.{dataset_raw}.weather_readings"
    quarantine_id = f"{project}.{dataset_raw}.invalid_records"

    loaded = load_dataframe(pd.DataFrame(good_rows), table_id) if good_rows else 0
    quarantined = load_quarantine_records(bad_rows, quarantine_id)

    logging.info("Weather loaded=%s quarantined=%s", loaded, quarantined)
    return loaded


if __name__ == "__main__":
    run_weather_ingestion()
