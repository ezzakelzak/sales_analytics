{% snapshot dim_store_snapshot %}

{{ config(
    target_schema='sales_snapshots',
    unique_key='store_id',
    strategy='check',
    check_cols=['channel']
) }}

select
    store_id,
    channel
from {{ ref('dim_store') }}

{% endsnapshot %}
