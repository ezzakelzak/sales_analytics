with source as (
    select *
    from {{ source('bronze_sales', 'sales_events') }}
)

select distinct
    product_id,
    product_name,
    category,
    unit_price
from source