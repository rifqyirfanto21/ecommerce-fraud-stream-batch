{{
    config(
        materialized='table',
        partition_by={
            "field": "event_ts",
            "data_type": "timestamp",
        },
        cluster_by=['user_id', 'product_id', 'country', 'status']
    )
}}

with orders as (
    select *
    from {{ ref('stg_orders') }}
),
users as (
    select *
    from {{ ref('dim_users') }}
),
products as (
    select *
    from {{ ref('dim_products') }}
)
select
    o.order_id,
    cast(REGEXP_REPLACE(o.user_id, '^U', '') as INT64) as user_id,
    cast(REGEXP_REPLACE(o.product_id, '^P', '') as INT64) as product_id,
    quantity,
    amount_numeric as amount,
    country,
    status as fraud_status,
    created_date,
    event_ts,
    ingestion_ts
from orders o
join users u on cast(REGEXP_REPLACE(o.user_id, '^U', '') as INT64) = u.user_id
join products p on cast(REGEXP_REPLACE(o.product_id, '^P', '') as INT64) = p.product_id