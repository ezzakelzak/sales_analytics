with sales as (
    select
        event_id,
        total_amount_eur,
        payment_method
    from {{ ref('fact_sales') }}
)

select
    payment_method,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from sales
group by payment_method
order by total_sales desc

