from typing import Any, Dict, Tuple

from ingestion.utils import SchemaMismatchError, parse_datetime_str, safe_float

EXPECTED_AQI_FIELDS = {
    "city": "str",
    "state": "str",
    "station": "str",
    "pollutant_id": "str",
    "avg_value": "float",
    "min_value": "float",
    "max_value": "float",
    "last_update": "str",
}

EXPECTED_WEATHER_FIELDS = {
    "name": "str",
    "dt": "int",
    "main.temp": "float",
    "main.humidity": "float",
    "wind.speed": "float",
    "weather[0].main": "str",
}


def _get_nested(record: Dict[str, Any], path: str) -> Any:
    if path == "weather[0].main":
        weather = record.get("weather") or []
        return weather[0].get("main") if weather else None
    if "." not in path:
        return record.get(path)
    parts = path.split(".")
    current: Any = record
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def validate_aqi_record(record: Dict[str, Any]) -> Dict[str, Any]:
    for field in EXPECTED_AQI_FIELDS:
        if _get_nested(record, field) is None:
            raise SchemaMismatchError(field, "missing")

    avg = safe_float(record.get("avg_value"))
    if avg is None:
        raise SchemaMismatchError("avg_value", "not numeric")
    if avg < 0 or avg > 1000:
        raise SchemaMismatchError("avg_value", "out of range")

    parsed = parse_datetime_str(record.get("last_update", ""))
    if parsed is None:
        raise SchemaMismatchError("last_update", "bad datetime format")

    if not str(record.get("city", "")).strip():
        raise SchemaMismatchError("city", "empty")
    if not str(record.get("station", "")).strip():
        raise SchemaMismatchError("station", "empty")

    return record


def validate_weather_record(record: Dict[str, Any]) -> Dict[str, Any]:
    for field in EXPECTED_WEATHER_FIELDS:
        if _get_nested(record, field) is None:
            raise SchemaMismatchError(field, "missing")

    temp = safe_float(_get_nested(record, "main.temp"))
    if temp is None or temp < -10 or temp > 60:
        raise SchemaMismatchError("main.temp", "out of range")

    humidity = safe_float(_get_nested(record, "main.humidity"))
    if humidity is None or humidity < 0 or humidity > 100:
        raise SchemaMismatchError("main.humidity", "out of range")

    return record


def weather_rainfall(record: Dict[str, Any]) -> float:
    rain = record.get("rain") or {}
    return float(rain.get("1h", 0.0) or 0.0)


def summarize_schema_error(exc: SchemaMismatchError) -> Tuple[str, str]:
    return exc.field, exc.reason
