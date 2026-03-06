{{ config(
    materialized='table',
    partition_by={"field": "reading_date", "data_type": "date"},
    cluster_by=["canonical_city"]
) }}

with base as (
    select
        canonical_city,
        reading_date,
        avg(pm25) as avg_pm25,
        avg(pm10) as avg_pm10,
        avg(no2) as avg_no2,
        avg(so2) as avg_so2,
        avg(co) as avg_co,
        avg(ozone) as avg_ozone,
        avg(nh3) as avg_nh3,
        avg(composite_aqi) as avg_composite_aqi
    from {{ ref('int_aqi_with_category') }}
    group by canonical_city, reading_date
)
select
    *,
    case
        when avg_pm25 = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'PM2.5'
        when avg_pm10 = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'PM10'
        when avg_no2 = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'NO2'
        when avg_so2 = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'SO2'
        when avg_co = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'CO'
        when avg_ozone = greatest(avg_pm25, avg_pm10, avg_no2, avg_so2, avg_co, avg_ozone, avg_nh3) then 'OZONE'
        else 'NH3'
    end as dominant_pollutant
from base
