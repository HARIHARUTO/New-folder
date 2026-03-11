{{ config(
    materialized='table',
    partition_by={"field": "reading_hour", "data_type": "timestamp"},
    cluster_by=["canonical_city"]
) }}

select
    canonical_city,
    reading_hour,
    composite_aqi,
    avg(pm25) over (partition by canonical_city order by reading_hour rows between 2 preceding and current row) as avg_pm25_last_3h,
    avg(pm25) over (partition by canonical_city order by reading_hour rows between 23 preceding and current row) as avg_pm25_last_24h,
    sum(rainfall_1h_mm) over (partition by canonical_city order by reading_hour rows between 5 preceding and current row) as rainfall_last_6h,
    avg(wind_speed_mps) over (partition by canonical_city order by reading_hour rows between 2 preceding and current row) as wind_speed_last_3h,
    case
        when composite_aqi > lag(composite_aqi, 3) over (partition by canonical_city order by reading_hour) then 'rising'
        when composite_aqi < lag(composite_aqi, 3) over (partition by canonical_city order by reading_hour) then 'falling'
        else 'stable'
    end as pollution_trend,
    timestamp_diff(
        reading_hour,
        max(case when rainfall_1h_mm > 0 then reading_hour end) over (partition by canonical_city order by reading_hour rows between unbounded preceding and current row),
        hour
    ) as hours_since_last_rain,
    case
        when max(composite_aqi) over (partition by canonical_city order by reading_hour rows between 1 following and 3 following) > 300 then 1
        else 0
    end as aqi_spike_flag
from {{ ref('int_aqi_weather_joined') }}
where has_weather_data = true
