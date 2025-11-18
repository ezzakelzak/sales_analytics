select
    customer_id,
    sum(total_amount_eur) as total_spent,
    count(distinct event_id) as num_purchases
from {{ ref('fact_sales') }}
group by customer_id
order by total_spent desc
limit 20
