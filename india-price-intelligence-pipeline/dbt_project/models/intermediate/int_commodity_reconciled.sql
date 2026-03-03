with commodity_map as (
    select raw_name, canonical_name, lower(source) as source
    from {{ ref('commodity_map') }}
),
bigbasket as (
    select
        'bigbasket' as source,
        b.scraped_week as week,
        coalesce(cm.canonical_name, b.product_name) as canonical_commodity_name,
        b.product_name as raw_commodity_name,
        b.price_per_kg as retail_price_per_kg,
        cast(null as string) as state,
        cast(null as float64) as wholesale_price_per_kg
    from {{ ref('stg_bigbasket_prices') }} b
    left join commodity_map cm
        on b.product_name = cm.raw_name
        and cm.source = 'bigbasket'
),
mandi as (
    select
        'mandi' as source,
        m.price_week as week,
        coalesce(cm.canonical_name, m.commodity) as canonical_commodity_name,
        m.commodity as raw_commodity_name,
        cast(null as float64) as retail_price_per_kg,
        m.state,
        m.modal_price_per_kg as wholesale_price_per_kg
    from {{ ref('stg_mandi_prices') }} m
    left join commodity_map cm
        on m.commodity = cm.raw_name
        and cm.source = 'mandi'
)
select * from bigbasket
union all
select * from mandi
