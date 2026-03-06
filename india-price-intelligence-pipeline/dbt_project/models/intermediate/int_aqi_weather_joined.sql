select
    aqi.station_name,
    aqi.canonical_city,
    aqi.state,
    aqi.reading_hour,
    aqi.reading_date,
    aqi.composite_aqi,
    aqi.aqi_category,
    aqi.pm25,
    aqi.pm10,
    aqi.no2,
    aqi.so2,
    aqi.co,
    aqi.ozone,
    aqi.nh3,
    weather.temp_celsius,
    weather.feels_like_celsius,
    weather.humidity_pct,
    weather.wind_speed_mps,
    weather.wind_direction_degrees,
    weather.weather_condition,
    weather.rainfall_1h_mm,
    weather.is_raining,
    weather.wind_category,
    weather.visibility_meters,
    weather.pressure_hpa,
    weather.fetched_at as weather_fetched_at,
    weather.canonical_city is not null as has_weather_data
from {{ ref('int_aqi_with_category') }} as aqi
left join {{ ref('stg_weather_readings') }} as weather
  on aqi.canonical_city = weather.canonical_city
 and aqi.reading_hour = weather.weather_hour
