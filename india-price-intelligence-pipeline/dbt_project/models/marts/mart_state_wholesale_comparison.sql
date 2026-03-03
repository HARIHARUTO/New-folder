{{ config(partition_by={"field": "price_week", "data_type": "date"}, cluster_by=["state", "canonical_commodity"]) }}

with reconciled as (
    select * from {{ ref('int_commodity_reconciled') }} where source = 'mandi'
)
select
    state,
    canonical_commodity_name as canonical_commodity,
    avg(wholesale_price_per_kg) as avg_wholesale_price,
    count(*) as data_points,
    week as price_week
from reconciled
group by 1, 2, 5
