{{ config(
    materialized='table',
    partition_by={"field": "run_timestamp", "data_type": "timestamp"}
) }}

with
latest_aqi as (
    select max(latest_fetched_at) as last_aqi_fetch
    from {{ ref('stg_aqi_readings') }}
),
aqi_recent as (
    select count(*) as aqi_rows_last_hour
    from {{ ref('stg_aqi_readings') }}
    where reading_hour >= timestamp_sub(current_timestamp(), interval 1 hour)
),
weather_recent as (
    select count(*) as weather_rows_last_hour
    from {{ ref('stg_weather_readings') }}
    where weather_hour >= timestamp_sub(current_timestamp(), interval 1 hour)
),
quarantine_recent as (
    select count(*) as quarantine_rows_last_hour
    from {{ source('raw_aqi', 'invalid_records') }}
    where quarantined_at >= timestamp_sub(current_timestamp(), interval 1 hour)
),
null_rates as (
    select
        safe_divide(countif(pm25 is null), count(*)) as null_pm25_rate,
        safe_divide(countif(pm10 is null), count(*)) as null_pm10_rate
    from {{ ref('stg_aqi_readings') }}
    where reading_hour >= timestamp_sub(current_timestamp(), interval 1 hour)
)
select
    current_timestamp() as run_timestamp,
    aqi_recent.aqi_rows_last_hour,
    weather_recent.weather_rows_last_hour,
    quarantine_recent.quarantine_rows_last_hour,
    null_rates.null_pm25_rate,
    null_rates.null_pm10_rate,
    coalesce(timestamp_diff(current_timestamp(), latest_aqi.last_aqi_fetch, minute), 0) as freshness_minutes,
    0 as dbt_test_failures,
    aqi_recent.aqi_rows_last_hour < 50 as is_aqi_low_volume,
    quarantine_recent.quarantine_rows_last_hour > 10 as is_high_quarantine_rate,
    null_rates.null_pm25_rate > 0.3 as is_pm25_data_missing
from latest_aqi, aqi_recent, weather_recent, quarantine_recent, null_rates
