with base as (
    select *
    from {{ ref('int_retail_vs_wholesale') }}
    where week >= date_sub(current_date(), interval 90 day)
)
select
    canonical_commodity_name as canonical_commodity,
    avg(margin_percentage) as avg_margin_percentage,
    avg(margin_spread) as avg_margin_spread,
    count(distinct week) as weeks_tracked,
    rank() over (order by avg(margin_percentage) desc) as commodity_rank
from base
group by 1
