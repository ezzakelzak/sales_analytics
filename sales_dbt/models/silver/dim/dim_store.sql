with source as (
    select *
    from {{ source('bronze_sales', 'sales_events') }}
)

select distinct
    store_id,
    channel
from source