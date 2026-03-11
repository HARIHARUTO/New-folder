import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests


class SchemaMismatchError(Exception):
    def __init__(self, field: str, reason: str) -> None:
        super().__init__(f"{field}: {reason}")
        self.field = field
        self.reason = reason


def parse_datetime_str(value: str, fmt: str = "%d/%m/%Y %H:%M:%S") -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    formats = [fmt, "%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"]
    ist = timezone(timedelta(hours=5, minutes=30))
    for item in formats:
        try:
            parsed = datetime.strptime(raw, item)
            # data.gov AQI timestamps are local India time; convert to UTC
            return parsed.replace(tzinfo=ist).astimezone(timezone.utc)
        except Exception:
            continue
    return None


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def retry_request(
    url: str,
    params: Dict[str, Any],
    retries: int = 3,
    backoff_base: float = 2.0,
    timeout: int = 30,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt == retries:
                break
            time.sleep((backoff_base ** attempt) + random.uniform(0, 1.0))
    if last_exc:
        raise last_exc
    raise RuntimeError("retry_request failed without exception")


def paginate_api(
    base_url: str,
    api_key: str,
    limit: int = 100,
    offset: int = 0,
    max_pages: Optional[int] = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    pages = 0
    while True:
        params = {
            "api-key": api_key,
            "format": "json",
            "limit": limit,
            "offset": offset,
        }
        resp = retry_request(base_url, params=params)
        payload = resp.json()
        records = payload.get("records", [])
        if not records:
            break
        rows.extend(records)
        if len(records) < limit:
            break
        offset += limit
        pages += 1
        if max_pages is not None and pages >= max_pages:
            break
        time.sleep(1.0)
    return rows

