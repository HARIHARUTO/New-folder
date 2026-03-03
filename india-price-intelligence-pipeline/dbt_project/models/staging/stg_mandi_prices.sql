with raw as (
    select
        initcap(trim(state)) as state,
        district,
        market,
        initcap(trim(commodity)) as commodity,
        variety,
        arrival_date,
        modal_price_per_kg,
        fetched_at,
        date_trunc(arrival_date, week(monday)) as price_week
    from `{{ env_var("GCP_PROJECT_ID") }}.{{ env_var("BQ_DATASET_RAW") }}.mandi_prices`
    where modal_price_per_kg is not null and modal_price_per_kg > 0
),
ranked as (
    select
        *,
        row_number() over (
            partition by state, market, commodity, arrival_date
            order by fetched_at desc
        ) as rn
    from raw
)
select
    state,
    district,
    market,
    commodity,
    variety,
    arrival_date,
    modal_price_per_kg,
    fetched_at,
    price_week
from ranked
where rn = 1
