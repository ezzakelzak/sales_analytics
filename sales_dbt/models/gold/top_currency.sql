with sales as (
    select
        event_id,
        total_amount_eur,
        currency
    from {{ ref('fact_sales') }}
)

select
    currency,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from sales
group by currency
order by total_sales desc

