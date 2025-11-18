with sales as (
    select
        f.event_id,
        f.total_amount_eur,
        c.country
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
)

select
    country,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from sales
group by country
