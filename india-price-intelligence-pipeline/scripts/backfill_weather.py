import argparse
import json
import os
import time
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.bq_loader import load_dataframe, setup_logging

OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"


def load_cities(path: str):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--cities", default="data/cities.json")
    args = parser.parse_args()

    load_dotenv()
    setup_logging()

    project = os.getenv("GCP_PROJECT_ID")
    dataset_raw = os.getenv("BQ_DATASET_RAW", "raw_aqi")
    table_id = f"{project}.{dataset_raw}.weather_readings"

    start_date = args.start
    end_date = args.end

    rows = []
    for city in load_cities(args.cities):
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m,precipitation",
        }
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        times = payload.get("hourly", {}).get("time", [])
        temps = payload.get("hourly", {}).get("temperature_2m", [])
        humidity = payload.get("hourly", {}).get("relativehumidity_2m", [])
        wind = payload.get("hourly", {}).get("windspeed_10m", [])
        rain = payload.get("hourly", {}).get("precipitation", [])

        for idx, ts in enumerate(times):
            rows.append(
                {
                    "city": city["city"],
                    "recorded_at": datetime.fromisoformat(ts),
                    "temp_celsius": temps[idx] if idx < len(temps) else None,
                    "feels_like_celsius": None,
                    "humidity_pct": humidity[idx] if idx < len(humidity) else None,
                    "wind_speed_mps": wind[idx] if idx < len(wind) else None,
                    "wind_direction_degrees": None,
                    "weather_condition": None,
                    "rainfall_1h_mm": rain[idx] if idx < len(rain) else 0.0,
                    "visibility_meters": None,
                    "pressure_hpa": None,
                    "fetched_at": datetime.utcnow(),
                }
            )

        time.sleep(1)

    if rows:
        load_dataframe(pd.DataFrame(rows), table_id)


if __name__ == "__main__":
    main()
