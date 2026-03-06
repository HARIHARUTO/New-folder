with base as (
    select
        city,
        recorded_at,
        temp_celsius,
        feels_like_celsius,
        humidity_pct,
        wind_speed_mps,
        wind_direction_degrees,
        weather_condition,
        rainfall_1h_mm,
        visibility_meters,
        pressure_hpa,
        fetched_at
    from {{ source('raw_aqi', 'weather_readings') }}
    where recorded_at is not null
),
typed as (
    select
        city,
        recorded_at,
        temp_celsius,
        feels_like_celsius,
        humidity_pct,
        wind_speed_mps,
        wind_direction_degrees,
        weather_condition,
        rainfall_1h_mm,
        visibility_meters,
        pressure_hpa,
        fetched_at
    from base
),
normalized as (
    select
        coalesce(city_map.canonical_city, typed.city) as canonical_city,
        timestamp_trunc(typed.recorded_at, hour) as weather_hour,
        typed.temp_celsius,
        typed.feels_like_celsius,
        typed.humidity_pct,
        typed.wind_speed_mps,
        typed.wind_direction_degrees,
        typed.weather_condition,
        coalesce(typed.rainfall_1h_mm, 0.0) as rainfall_1h_mm,
        typed.visibility_meters,
        typed.pressure_hpa,
        typed.fetched_at
    from typed
    left join {{ ref('city_map') }} as city_map
      on trim(typed.city) = city_map.raw_city_name
),
deduped as (
    select
        *,
        row_number() over (
            partition by canonical_city, weather_hour
            order by fetched_at desc
        ) as rn
    from normalized
)
select
    canonical_city,
    weather_hour,
    temp_celsius,
    feels_like_celsius,
    humidity_pct,
    wind_speed_mps,
    wind_direction_degrees,
    weather_condition,
    rainfall_1h_mm,
    visibility_meters,
    pressure_hpa,
    rainfall_1h_mm > 0 as is_raining,
    case
        when wind_speed_mps < 2 then 'Calm'
        when wind_speed_mps < 5 then 'Light'
        when wind_speed_mps < 10 then 'Moderate'
        else 'Strong'
    end as wind_category,
    fetched_at
from deduped
where rn = 1
