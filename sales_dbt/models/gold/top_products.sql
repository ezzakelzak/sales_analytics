with sales as (
    select
        f.event_id,
        f.total_amount_eur,
        p.product_name
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_product') }} p
        on f.product_id = p.product_id
)

select
    product_name,
    sum(total_amount_eur) as total_sales,
    count(distinct event_id) as num_sales
from sales
group by product_name
order by total_sales desc
limit 20
