with reconciled as (
    select * from {{ ref('int_commodity_reconciled') }}
),
retail as (
    select
        week,
        canonical_commodity_name,
        avg(retail_price_per_kg) as avg_retail_price
    from reconciled
    where source = 'bigbasket'
    group by 1, 2
),
wholesale as (
    select
        week,
        canonical_commodity_name,
        avg(wholesale_price_per_kg) as avg_wholesale_price
    from reconciled
    where source = 'mandi'
    group by 1, 2
)
select
    r.week,
    r.canonical_commodity_name,
    r.avg_retail_price,
    w.avg_wholesale_price,
    (r.avg_retail_price - w.avg_wholesale_price) as margin_spread,
    safe_multiply(safe_divide((r.avg_retail_price - w.avg_wholesale_price), w.avg_wholesale_price), 100) as margin_percentage,
    if(safe_multiply(safe_divide((r.avg_retail_price - w.avg_wholesale_price), w.avg_wholesale_price), 100) > 200, true, false) as is_high_markup
from retail r
join wholesale w
    on r.week = w.week
    and r.canonical_commodity_name = w.canonical_commodity_name
