select *
from {{ ref('mart_price_trends') }}
where avg_retail_price <= 0
   or avg_wholesale_price <= 0
