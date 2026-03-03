import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, List, Dict, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

from ingestion.bq_loader import ensure_columns, load_dataframe
from ingestion.utils import clean_price_to_float, parse_ddmmyyyy, retry, setup_logging

MANDI_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"


def to_price_per_kg(value: Any) -> Optional[float]:
    price = clean_price_to_float(value)
    if price is None:
        return None
    return price / 100.0


def fetch_page(api_key: str, limit: int, offset: int) -> List[Dict[str, Any]]:
    params = {
        "api-key": api_key,
        "format": "json",
        "limit": limit,
        "offset": offset,
    }

    def _request() -> requests.Response:
        resp = requests.get(MANDI_URL, params=params, timeout=30)
        if resp.status_code != 200:
            logging.error("Mandi API error status=%s body=%s", resp.status_code, resp.text[:500])
            resp.raise_for_status()
        return resp

    response = retry(_request, retries=8, base_delay=2.0)
    payload = response.json()
    return payload.get("records", [])


def normalize_row(raw: Dict[str, Any], fetched_at: datetime) -> Optional[Dict[str, Any]]:
    modal = to_price_per_kg(raw.get("modal_price"))
    if modal is None or modal <= 0:
        return None

    arrival_str = str(raw.get("arrival_date", ""))
    arrival_date = parse_ddmmyyyy(arrival_str)
    if arrival_date is None and arrival_str:
        logging.warning("Bad arrival_date value=%s", arrival_str)

    min_p = to_price_per_kg(raw.get("min_price"))
    max_p = to_price_per_kg(raw.get("max_price"))

    return {
        "state": raw.get("state"),
        "district": raw.get("district"),
        "market": raw.get("market"),
        "commodity": raw.get("commodity"),
        "variety": raw.get("variety"),
        "arrival_date": arrival_date,
        "min_price_per_kg": min_p,
        "max_price_per_kg": max_p,
        "modal_price_per_kg": modal,
        "fetched_at": fetched_at,
    }


def run_mandi_fetcher() -> int:
    load_dotenv()
    setup_logging()

    api_key = os.getenv("DATA_GOV_API_KEY")
    if not api_key:
        raise ValueError("DATA_GOV_API_KEY is missing")

    rows: List[Dict[str, Any]] = []
    limit = int(os.getenv("MANDI_LIMIT", "50"))
    offset = int(os.getenv("MANDI_START_OFFSET", "0"))
    max_pages = os.getenv("MANDI_MAX_PAGES")
    max_pages = int(max_pages) if max_pages else None
    pages_fetched = 0
    fetched_at = datetime.now(timezone.utc)

    while True:
        try:
            page = fetch_page(api_key, limit, offset)
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else None
            if status_code == 429:
                logging.warning("Rate limit hit at offset=%s. Cooling down for 90 seconds.", offset)
                time.sleep(90)
                continue
            raise

        if not page:
            break

        for raw in page:
            normalized = normalize_row(raw, fetched_at)
            if normalized is not None:
                rows.append(normalized)

        logging.info("Fetched page offset=%s rows=%s", offset, len(page))
        if len(page) < limit:
            break

        offset += limit
        pages_fetched += 1
        if max_pages is not None and pages_fetched >= max_pages:
            logging.info("Reached MANDI_MAX_PAGES=%s, stopping early.", max_pages)
            break
        time.sleep(1.5)

    if not rows:
        logging.warning("No valid mandi records fetched")
        return 0

    df = pd.DataFrame(rows)
    df = ensure_columns(
        df,
        [
            "state",
            "district",
            "market",
            "commodity",
            "variety",
            "arrival_date",
            "min_price_per_kg",
            "max_price_per_kg",
            "modal_price_per_kg",
            "fetched_at",
        ],
    )

    dataset = os.getenv("BQ_DATASET_RAW", "raw_ecommerce")
    project = os.getenv("GCP_PROJECT_ID")
    table_id = f"{project}.{dataset}.mandi_prices"
    loaded = load_dataframe(df, table_id)
    logging.info("Total mandi rows loaded=%s", loaded)
    return loaded


if __name__ == "__main__":
    run_mandi_fetcher()
