select *
from {{ ref('mart_margin_by_commodity') }}
where avg_margin_percentage > 1000
