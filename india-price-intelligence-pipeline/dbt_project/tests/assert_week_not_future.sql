select *
from {{ ref('mart_price_trends') }}
where week > current_date()
