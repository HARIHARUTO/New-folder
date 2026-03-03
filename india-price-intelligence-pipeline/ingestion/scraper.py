import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional, List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from ingestion.bq_loader import ensure_columns, load_dataframe
from ingestion.utils import clean_price_to_float, normalize_to_per_kg, random_polite_sleep, retry, setup_logging

BASE_URLS = [
    "https://www.bigbasket.com/pc/fruits-vegetables/fresh-vegetables/",
    "https://www.bigbasket.com/pc/fruits-vegetables/fresh-fruits/",
    "https://www.bigbasket.com/ps/?q=dairy",
    "https://www.bigbasket.com/ps/?q=rice",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )
}


def parse_cards(html: str, url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(
        "div[class*='sku-item'], div[class*='product'], div[class*='item'], div[class*='product-card']"
    )
    rows: List[Dict[str, Any]] = []
    scraped_at = datetime.now(timezone.utc)
    scraped_date = scraped_at.date()

    for card in cards:
        name_el = card.select_one("h3, h2, a[title], div[class*='name'], span[class*='name']")
        price_el = card.select_one(
            "span[class*='price'], div[class*='price'], span[class*='sale'], span[class*='Sale']"
        )
        unit_el = card.select_one("span[class*='Pack'], span[class*='size'], div[class*='variant'], span[class*='pack']")
        discount_el = card.select_one("span[class*='off'], div[class*='off'], span[class*='discount']")

        product_name = (name_el.get_text(" ", strip=True) if name_el else "").strip()
        price_raw = (price_el.get_text(" ", strip=True) if price_el else "").strip()
        unit_raw = (unit_el.get_text(" ", strip=True) if unit_el else "").strip()
        discount_raw = (discount_el.get_text(" ", strip=True) if discount_el else "").strip()

        if not product_name:
            continue

        price = clean_price_to_float(price_raw)
        discount_percent = clean_price_to_float(discount_raw)
        price_per_kg = normalize_to_per_kg(price, unit_raw)
        if price is None:
            logging.warning("Could not parse price for product=%s raw=%s", product_name, price_raw)
        if price_per_kg is None:
            logging.warning("Could not normalize unit for product=%s unit=%s", product_name, unit_raw)

        rows.append(
            {
                "product_name": product_name,
                "category": "fruits-vegetables",
                "subcategory": "unknown",
                "price_raw": price_raw,
                "price_per_kg": price_per_kg,
                "unit_raw": unit_raw,
                "discount_percent": discount_percent,
                "scraped_at": scraped_at,
                "scraped_date": scraped_date,
            }
        )

    return rows


def fetch_url_requests(url: str) -> str:
    def _request() -> requests.Response:
        response = requests.get(url, headers=HEADERS, timeout=25)
        if response.status_code in (403, 429):
            logging.warning("BigBasket returned %s. Backing off before retry", response.status_code)
        response.raise_for_status()
        return response

    response = retry(_request, retries=3)
    return response.text


def fetch_url_selenium(url: str) -> Optional[str]:
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except Exception:
        logging.exception("Selenium not available")
        return None

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(f"user-agent={HEADERS['User-Agent']}")

    driver_path = os.getenv("CHROME_DRIVER_PATH")
    driver = None
    try:
        if driver_path:
            driver = webdriver.Chrome(driver_path, options=options)
        else:
            driver = webdriver.Chrome(options=options)
        driver.get(url)
        return driver.page_source
    except Exception:
        logging.exception("Selenium fetch failed for %s", url)
        return None
    finally:
        if driver:
            driver.quit()


def run_bigbasket_scraper() -> int:
    load_dotenv()
    setup_logging()
    all_rows: List[Dict[str, Any]] = []
    max_pages = int(os.getenv("BIGBASKET_MAX_PAGES", "3"))
    use_selenium = os.getenv("BIGBASKET_USE_SELENIUM", "false").lower() in ("1", "true", "yes")

    try:
        for base_url in BASE_URLS:
            for page in range(1, max_pages + 1):
                page_url = f"{base_url}?page={page}"
                html = fetch_url_requests(page_url)
                rows = parse_cards(html, page_url)
                if not rows and use_selenium:
                    logging.info("No rows from requests for %s, trying selenium", page_url)
                    html = fetch_url_selenium(page_url)
                    if html:
                        rows = parse_cards(html, page_url)
                all_rows.extend(rows)
                random_polite_sleep(1.0, 3.0)

        if not all_rows:
            logging.warning("No BigBasket rows scraped")
            return 0

        df = pd.DataFrame(all_rows)
        df["scraped_date"] = pd.to_datetime(df["scraped_date"]).dt.date
        df = df.drop_duplicates(subset=["product_name", "scraped_date"], keep="last")
        df = ensure_columns(
            df,
            [
                "product_name",
                "category",
                "subcategory",
                "price_raw",
                "price_per_kg",
                "unit_raw",
                "discount_percent",
                "scraped_at",
                "scraped_date",
            ],
        )

        dataset = os.getenv("BQ_DATASET_RAW", "raw_ecommerce")
        project = os.getenv("GCP_PROJECT_ID")
        table_id = f"{project}.{dataset}.bigbasket_prices"
        return load_dataframe(df, table_id)
    except Exception:
        logging.exception("BigBasket scrape failed")
        raise


if __name__ == "__main__":
    run_bigbasket_scraper()
