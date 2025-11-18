with source as (
    select *
    from {{ source('bronze_sales', 'sales_events') }}
)

select distinct
    customer_id,
    loyalty_level,
    age,
    country
from source
