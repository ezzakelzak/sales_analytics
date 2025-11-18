select
    date(event_timestamp) as day,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from {{ ref('fact_sales') }}
group by day
order by day
