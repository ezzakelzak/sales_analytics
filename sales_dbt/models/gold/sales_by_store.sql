with sales as (
    select
        f.event_id,
        f.store_id,
        s.channel,
        f.total_amount_eur
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_store') }} s on f.store_id = s.store_id
)

select
    store_id,
    channel,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from sales
group by store_id, channel
