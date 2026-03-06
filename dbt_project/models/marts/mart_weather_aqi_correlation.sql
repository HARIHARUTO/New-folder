{{ config(
    materialized='table',
    cluster_by=["canonical_city"]
) }}

select
    canonical_city,
    corr(composite_aqi, wind_speed_mps) as wind_aqi_correlation,
    corr(composite_aqi, humidity_pct) as humidity_aqi_correlation,
    corr(composite_aqi, temp_celsius) as temp_aqi_correlation,
    corr(composite_aqi, rainfall_1h_mm) as rain_aqi_correlation,
    count(*) as data_points_used
from {{ ref('int_aqi_weather_joined') }}
where has_weather_data = true
group by canonical_city
