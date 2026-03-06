select *
from {{ ref('mart_data_quality_metrics') }}
where run_timestamp < timestamp_sub(current_timestamp(), interval 2 hour)
