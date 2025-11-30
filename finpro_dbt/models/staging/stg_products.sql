with source as (
    select *
    from {{ source('rifqy_ecommerce_finpro', 'raw_products') }}
)
select
    product_id,
    product_name,
    brand,
    category,
    sub_category,
    currency,
    price,
    cost,
    created_date,
    ingestion_ts,
    source
from source