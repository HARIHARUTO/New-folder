import os
from datetime import datetime, timezone, date

import pandas as pd
from dotenv import load_dotenv

from bq_loader import load_dataframe


def main() -> None:
    load_dotenv()
    project = os.getenv("GCP_PROJECT_ID")
    dataset = os.getenv("BQ_DATASET_RAW", "raw_ecommerce")
    table_id = f"{project}.{dataset}.bigbasket_prices"

    now = datetime.now(timezone.utc)
    today = date.today()

    rows = [
        {
            "product_name": "Tomato",
            "category": "fruits-vegetables",
            "subcategory": "fresh-vegetables",
            "price_raw": "Rs. 40 / 1 kg",
            "price_per_kg": 40.0,
            "unit_raw": "1 kg",
            "discount_percent": 10.0,
            "scraped_at": now,
            "scraped_date": today,
        },
        {
            "product_name": "Potato",
            "category": "fruits-vegetables",
            "subcategory": "fresh-vegetables",
            "price_raw": "Rs. 30 / 1 kg",
            "price_per_kg": 30.0,
            "unit_raw": "1 kg",
            "discount_percent": 0.0,
            "scraped_at": now,
            "scraped_date": today,
        },
        {
            "product_name": "Onion",
            "category": "fruits-vegetables",
            "subcategory": "fresh-vegetables",
            "price_raw": "Rs. 35 / 1 kg",
            "price_per_kg": 35.0,
            "unit_raw": "1 kg",
            "discount_percent": 5.0,
            "scraped_at": now,
            "scraped_date": today,
        },
    ]

    df = pd.DataFrame(rows)
    load_dataframe(df, table_id)


if __name__ == "__main__":
    main()
