with base as (
    select
        station_name,
        city,
        state,
        pollutant_id,
        pollutant_avg,
        reading_timestamp as reading_timestamp_raw,
        fetched_at as fetched_at_raw
    from {{ source('raw_aqi', 'aqi_readings') }}
),
typed as (
    select
        station_name,
        city,
        state,
        pollutant_id,
        pollutant_avg,
        case
            when reading_timestamp_raw is null then null
            when reading_timestamp_raw >= 100000000000000000 then timestamp_micros(cast(reading_timestamp_raw / 1000 as int64))
            when reading_timestamp_raw >= 100000000000000 then timestamp_micros(reading_timestamp_raw)
            when reading_timestamp_raw >= 100000000000 then timestamp_millis(reading_timestamp_raw)
            else timestamp_seconds(reading_timestamp_raw)
        end as reading_timestamp,
        case
            when fetched_at_raw is null then null
            when fetched_at_raw >= 100000000000000000 then timestamp_micros(cast(fetched_at_raw / 1000 as int64))
            when fetched_at_raw >= 100000000000000 then timestamp_micros(fetched_at_raw)
            when fetched_at_raw >= 100000000000 then timestamp_millis(fetched_at_raw)
            else timestamp_seconds(fetched_at_raw)
        end as fetched_at
    from base
    where pollutant_avg is not null
      and pollutant_avg > 0
      and reading_timestamp_raw is not null
),
normalized as (
    select
        typed.station_name,
        coalesce(city_map.canonical_city, typed.city) as canonical_city,
        typed.state,
        timestamp_trunc(typed.reading_timestamp, hour) as reading_hour,
        date(timestamp_trunc(typed.reading_timestamp, hour)) as reading_date,
        typed.pollutant_id,
        typed.pollutant_avg,
        typed.fetched_at
    from typed
    left join {{ ref('city_map') }} as city_map
      on trim(typed.city) = city_map.raw_city_name
)
select
    station_name,
    canonical_city,
    state,
    reading_hour,
    reading_date,
    max(case when upper(pollutant_id) in ('PM2.5', 'PM2_5', 'PM2.5 ') then pollutant_avg end) as pm25,
    max(case when upper(pollutant_id) in ('PM10', 'PM_10') then pollutant_avg end) as pm10,
    max(case when upper(pollutant_id) = 'NO2' then pollutant_avg end) as no2,
    max(case when upper(pollutant_id) = 'SO2' then pollutant_avg end) as so2,
    max(case when upper(pollutant_id) = 'CO' then pollutant_avg end) as co,
    max(case when upper(pollutant_id) in ('OZONE', 'O3') then pollutant_avg end) as ozone,
    max(case when upper(pollutant_id) = 'NH3' then pollutant_avg end) as nh3,
    max(fetched_at) as latest_fetched_at
from normalized
group by
    station_name,
    canonical_city,
    state,
    reading_hour,
    reading_date
