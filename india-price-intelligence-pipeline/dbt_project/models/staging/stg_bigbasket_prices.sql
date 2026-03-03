with raw as (
    select
        product_name,
        lower(trim(category)) as category,
        lower(trim(subcategory)) as subcategory,
        price_per_kg,
        scraped_at,
        scraped_date,
        date_trunc(scraped_date, week(monday)) as scraped_week
    from {{ source('raw_ecommerce', 'bigbasket_prices') }}
    where price_per_kg is not null and price_per_kg > 0
),
ranked as (
    select
        *,
        row_number() over (
            partition by product_name, scraped_date
            order by scraped_at desc
        ) as rn
    from raw
)
select
    product_name,
    category,
    subcategory,
    price_per_kg,
    cast(scraped_at as timestamp) as scraped_at,
    scraped_date,
    scraped_week
from ranked
where rn = 1
