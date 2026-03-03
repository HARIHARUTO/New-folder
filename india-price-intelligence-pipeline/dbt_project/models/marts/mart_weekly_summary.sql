with base as (
    select * from {{ ref('int_retail_vs_wholesale') }}
),
weekly as (
    select
        week,
        count(distinct canonical_commodity_name) as total_commodities_tracked,
        avg(avg_retail_price) as avg_retail_price,
        avg(avg_wholesale_price) as avg_wholesale_price,
        countif(is_high_markup) as commodities_with_high_markup
    from base
    group by 1
),
changes as (
    select
        week,
        total_commodities_tracked,
        safe_multiply(safe_divide(avg_retail_price - lag(avg_retail_price) over (order by week), lag(avg_retail_price) over (order by week)), 100) as avg_retail_price_change_pct,
        safe_multiply(safe_divide(avg_wholesale_price - lag(avg_wholesale_price) over (order by week), lag(avg_wholesale_price) over (order by week)), 100) as avg_wholesale_price_change_pct,
        commodities_with_high_markup
    from weekly
),
raw_counts as (
    select
        date_trunc(scraped_date, week(monday)) as week,
        count(*) as total_retail_rows
    from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.bigbasket_prices`
    group by 1
),
mandi_counts as (
    select
        date_trunc(arrival_date, week(monday)) as week,
        count(*) as total_mandi_rows
    from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.mandi_prices`
    group by 1
)
select
    c.week,
    c.total_commodities_tracked,
    c.avg_retail_price_change_pct,
    c.avg_wholesale_price_change_pct,
    c.commodities_with_high_markup,
    coalesce(r.total_retail_rows, 0) as total_retail_rows,
    coalesce(m.total_mandi_rows, 0) as total_mandi_rows
from changes c
left join raw_counts r using (week)
left join mandi_counts m using (week)
