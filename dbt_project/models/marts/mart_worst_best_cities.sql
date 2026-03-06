{{ config(
    materialized='table',
    cluster_by=["canonical_city"]
) }}

with base as (
    select
        canonical_city,
        avg_aqi,
        dominant_category
    from {{ ref('mart_city_aqi_trends') }}
)
select
    canonical_city,
    avg(avg_aqi) as overall_avg_aqi,
    max(avg_aqi) as worst_day_aqi,
    min(avg_aqi) as best_day_aqi,
    sum(case when dominant_category = 'Good' then 1 else 0 end) as days_good,
    sum(case when dominant_category = 'Severe' then 1 else 0 end) as days_severe,
    rank() over (order by avg(avg_aqi)) as aqi_rank
from base
group by canonical_city
