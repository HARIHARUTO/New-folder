import logging
import random
import re
import time
from datetime import date, datetime
from typing import Any, Callable, Optional, Tuple

LOGGER_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format=LOGGER_FORMAT)


def clean_price_to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def normalize_to_per_kg(price: Optional[float], unit_raw: str) -> Optional[float]:
    if price is None:
        return None
    unit = (unit_raw or "").strip().lower()
    if not unit:
        return None

    kg_match = re.search(r"([0-9]*\.?[0-9]+)\s*kg", unit)
    if kg_match:
        qty_kg = float(kg_match.group(1))
        return price / qty_kg if qty_kg > 0 else None

    g_match = re.search(r"([0-9]*\.?[0-9]+)\s*g", unit)
    if g_match:
        qty_kg = float(g_match.group(1)) / 1000.0
        return price / qty_kg if qty_kg > 0 else None

    ml_match = re.search(r"([0-9]*\.?[0-9]+)\s*ml", unit)
    if ml_match:
        # Approximate density for water-like products where 1000 ml ~= 1 kg.
        qty_kg = float(ml_match.group(1)) / 1000.0
        return price / qty_kg if qty_kg > 0 else None

    l_match = re.search(r"([0-9]*\.?[0-9]+)\s*l", unit)
    if l_match:
        qty_kg = float(l_match.group(1))
        return price / qty_kg if qty_kg > 0 else None

    if "piece" in unit or "pc" in unit:
        assumed_piece_weight_kg = 0.2
        return price / assumed_piece_weight_kg

    return None


def parse_ddmmyyyy(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def retry(
    fn: Callable[[], Any],
    retries: int = 3,
    base_delay: float = 1.5,
    jitter: float = 0.7,
    retry_on: Tuple[int, ...] = (403, 429, 500, 502, 503, 504),
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and status not in retry_on:
                raise
            if attempt == retries:
                break
            delay = (base_delay ** attempt) + random.uniform(0, jitter)
            time.sleep(delay)
    if last_error:
        raise last_error
    raise RuntimeError("Retry failed without captured exception")


def random_polite_sleep(min_seconds: float = 1.0, max_seconds: float = 3.0) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))
