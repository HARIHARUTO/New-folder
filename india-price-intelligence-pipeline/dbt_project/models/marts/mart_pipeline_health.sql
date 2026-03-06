{{ config(
    materialized='table',
    partition_by={"field": "run_timestamp", "data_type": "timestamp"}
) }}

select
    current_timestamp() as run_timestamp,
    (select count(*) from {{ ref('stg_aqi_readings') }} where reading_hour >= timestamp_sub(current_timestamp(), interval 24 hour)) as aqi_rows_last_24h,
    (select count(*) from {{ ref('stg_weather_readings') }} where weather_hour >= timestamp_sub(current_timestamp(), interval 24 hour)) as weather_rows_last_24h,
    (select max(latest_fetched_at) from {{ ref('stg_aqi_readings') }}) as last_aqi_fetch,
    (select max(fetched_at) from {{ ref('stg_weather_readings') }}) as last_weather_fetch
