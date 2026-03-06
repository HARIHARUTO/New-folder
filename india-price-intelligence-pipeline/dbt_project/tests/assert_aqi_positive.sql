select *
from {{ ref('int_aqi_with_category') }}
where composite_aqi < 0
