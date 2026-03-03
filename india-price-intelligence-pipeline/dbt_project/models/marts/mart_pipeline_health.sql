with latest as (
    select
        current_date() as run_date,
        (select count(*) from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.bigbasket_prices`
          where scraped_date >= date_trunc(current_date(), week(monday))) as bigbasket_rows_loaded,
        (select count(*) from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.mandi_prices`
          where arrival_date >= date_trunc(current_date(), week(monday))) as mandi_rows_loaded,
        (select max(scraped_at) from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.bigbasket_prices`) as last_successful_run
),
commodities as (
    select distinct canonical_commodity_name
    from {{ ref('int_commodity_reconciled') }}
),
retail_missing as (
    select count(*) as commodities_missing_retail
    from commodities c
    left join (
        select distinct canonical_commodity_name
        from {{ ref('int_commodity_reconciled') }}
        where source = 'bigbasket'
          and week >= date_trunc(current_date(), week(monday))
    ) r using (canonical_commodity_name)
    where r.canonical_commodity_name is null
),
wholesale_missing as (
    select count(*) as commodities_missing_wholesale
    from commodities c
    left join (
        select distinct canonical_commodity_name
        from {{ ref('int_commodity_reconciled') }}
        where source = 'mandi'
          and week >= date_trunc(current_date(), week(monday))
    ) w using (canonical_commodity_name)
    where w.canonical_commodity_name is null
)
select
    l.run_date,
    l.bigbasket_rows_loaded,
    l.mandi_rows_loaded,
    r.commodities_missing_retail,
    w.commodities_missing_wholesale,
    l.last_successful_run
from latest l
cross join retail_missing r
cross join wholesale_missing w
