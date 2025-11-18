{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

with source as (
    select *
    from {{ source('bronze_sales', 'sales_events') }}
),


sales_eur as (
    select
        s.*,
        case currency
            when 'EUR' then total_amount
            when 'USD' then total_amount / 1.10
            when 'GBP' then total_amount / 0.85
            when 'JPY' then total_amount / 160.0
            else total_amount
        end as total_amount_eur
    from source s
)

select
    event_id,
    event_timestamp,
    sale_id,
    sale_timestamp,
    store_id,
    customer_id,
    product_id,
    payment_method,
    currency,
    total_amount,
    quantity,
    unit_price,
    total_amount_eur
from sales_eur

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
