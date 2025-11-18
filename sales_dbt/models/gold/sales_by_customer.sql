with sales as (
    select
        f.event_id,
        f.customer_id,
        c.loyalty_level,
        c.country,
        f.total_amount_eur
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
)

select
    customer_id,
    loyalty_level,
    country,
    sum(total_amount_eur) as total_spent,
    count(distinct event_id) as num_purchases,
    avg(total_amount_eur) as avg_purchase
from sales
group by customer_id, loyalty_level, country
