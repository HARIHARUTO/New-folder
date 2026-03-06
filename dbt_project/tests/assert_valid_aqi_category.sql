select *
from {{ ref('int_aqi_with_category') }}
where aqi_category not in ('Good', 'Satisfactory', 'Moderate', 'Poor', 'Very Poor', 'Severe')
