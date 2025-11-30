with source as (
    select *
    from {{ source('rifqy_ecommerce_finpro', 'raw_orders') }}
)
select
    order_id,
    user_id,
    product_id,
    quantity,
    amount,
    amount_numeric,
    country,
    status,
    created_date,
    event_ts,
    source,
    ingestion_ts
from source