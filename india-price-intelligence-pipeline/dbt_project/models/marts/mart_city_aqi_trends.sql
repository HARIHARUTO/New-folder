{{ config(
    materialized='table',
    partition_by={"field": "reading_date", "data_type": "date"},
    cluster_by=["canonical_city"]
) }}

with base as (
    select *
    from {{ ref('int_aqi_weather_joined') }}
),
agg as (
    select
        canonical_city,
        reading_date,
        avg(composite_aqi) as avg_aqi,
        max(composite_aqi) as max_aqi,
        min(composite_aqi) as min_aqi,
        avg(pm25) as avg_pm25,
        avg(pm10) as avg_pm10
    from base
    group by canonical_city, reading_date
),
cat_rank as (
    select
        canonical_city,
        reading_date,
        aqi_category,
        row_number() over (
            partition by canonical_city, reading_date
            order by count(*) desc, aqi_category
        ) as rn
    from base
    group by canonical_city, reading_date, aqi_category
)
select
    agg.*,
    cat_rank.aqi_category as dominant_category
from agg
left join cat_rank
  on agg.canonical_city = cat_rank.canonical_city
 and agg.reading_date = cat_rank.reading_date
 and cat_rank.rn = 1
