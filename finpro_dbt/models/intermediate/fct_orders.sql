{{
    config(
        materialized='table',
        partition_by={
            "field": "event_ts",
            "data_type": "timestamp",
        },
        cluster_by=['user_id', 'product_id', 'country', 'fraud_status']
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
    o.quantity,
    o.amount_numeric as amount,
    o.country,
    o.status as fraud_status,
    o.created_date,
    o.event_ts,
    o.ingestion_ts
from orders o
join users u on cast(REGEXP_REPLACE(o.user_id, '^U', '') as INT64) = u.user_id
join products p on cast(REGEXP_REPLACE(o.product_id, '^P', '') as INT64) = p.product_id