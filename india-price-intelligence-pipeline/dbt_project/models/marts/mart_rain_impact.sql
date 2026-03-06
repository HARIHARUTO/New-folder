{{ config(
    materialized='table',
    partition_by={"field": "rain_event_date", "data_type": "date"},
    cluster_by=["canonical_city"]
) }}

with base as (
    select
        canonical_city,
        reading_hour,
        date(reading_hour) as rain_event_date,
        composite_aqi,
        rainfall_1h_mm,
        is_raining
    from {{ ref('int_aqi_weather_joined') }}
    where has_weather_data = true
),
rain_events as (
    select
        canonical_city,
        rain_event_date,
        composite_aqi,
        rainfall_1h_mm,
        avg(composite_aqi) over (
            partition by canonical_city
            order by reading_hour
            rows between 3 preceding and 1 preceding
        ) as pre_rain_avg_aqi,
        avg(composite_aqi) over (
            partition by canonical_city
            order by reading_hour
            rows between current row and current row
        ) as during_rain_avg_aqi,
        avg(composite_aqi) over (
            partition by canonical_city
            order by reading_hour
            rows between 1 following and 1 following
        ) as post_rain_1h_avg_aqi,
        avg(composite_aqi) over (
            partition by canonical_city
            order by reading_hour
            rows between 1 following and 6 following
        ) as post_rain_6h_avg_aqi
    from base
    where is_raining = true
)
select
    canonical_city,
    rain_event_date,
    avg(pre_rain_avg_aqi) as pre_rain_avg_aqi,
    avg(during_rain_avg_aqi) as during_rain_avg_aqi,
    avg(post_rain_1h_avg_aqi) as post_rain_1h_avg_aqi,
    avg(post_rain_6h_avg_aqi) as post_rain_6h_avg_aqi,
    avg(pre_rain_avg_aqi) - avg(post_rain_6h_avg_aqi) as aqi_improvement,
    sum(rainfall_1h_mm) as rainfall_total_mm
from rain_events
group by canonical_city, rain_event_date
