{% snapshot dim_customer_snapshot %}

{{ config(
    target_schema='sales_snapshots',
    unique_key='customer_id',
    strategy='check',
    check_cols=['loyalty_level','age','country']
) }}

select
    customer_id,
    loyalty_level,
    age,
    country
from {{ ref('dim_customer') }}

{% endsnapshot %}
