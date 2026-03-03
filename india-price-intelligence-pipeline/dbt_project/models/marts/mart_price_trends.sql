select
    week,
    canonical_commodity_name as canonical_commodity,
    avg_retail_price,
    avg_wholesale_price,
    margin_spread,
    margin_percentage,
    is_high_markup
from {{ ref('int_retail_vs_wholesale') }}
order by canonical_commodity, week
