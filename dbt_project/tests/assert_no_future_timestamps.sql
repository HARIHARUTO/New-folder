select *
from {{ ref('int_aqi_with_category') }}
where reading_hour > timestamp_add(current_timestamp(), interval 6 hour)
