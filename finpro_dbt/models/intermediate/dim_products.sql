{{
    config(
        materialized='table',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
        },
        cluster_by=['brand', 'category', 'sub_category']
    )
}}

with products as (
    select *
    from {{ ref('stg_products') }}
)
select
    product_id,
    trim(product_name) as product_name,
    trim(brand) as brand,
    initcap(trim(category)) as category,
    trim(sub_category) as sub_category,
    currency,
    price,
    cost,
    created_date,
    ingestion_ts
from products