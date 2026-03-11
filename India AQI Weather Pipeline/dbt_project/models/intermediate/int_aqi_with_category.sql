with base as (
    select
        station_name,
        canonical_city,
        state,
        reading_hour,
        reading_date,
        pm25,
        pm10,
        no2,
        so2,
        co,
        ozone,
        nh3
    from {{ ref('stg_aqi_readings') }}
),
calc as (
    select
        *,
        greatest(
            coalesce(pm25, 0),
            coalesce(pm10, 0),
            coalesce(no2, 0),
            coalesce(so2, 0),
            coalesce(co, 0),
            coalesce(ozone, 0),
            coalesce(nh3, 0)
        ) as composite_aqi
    from base
)
select
    calc.*,
    breakpoints.category as aqi_category,
    breakpoints.health_message
from calc
left join {{ ref('aqi_breakpoints') }} as breakpoints
  on calc.composite_aqi between breakpoints.min_aqi and breakpoints.max_aqi
