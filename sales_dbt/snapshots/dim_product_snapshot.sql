{% snapshot dim_product_snapshot %}

{{ config(
    target_schema='sales_snapshots',
    unique_key='product_name',
    strategy='check',
    check_cols=['category','unit_price']
) }}

select
    product_name,
    category,
    unit_price
from {{ ref('dim_product') }}

{% endsnapshot %}
