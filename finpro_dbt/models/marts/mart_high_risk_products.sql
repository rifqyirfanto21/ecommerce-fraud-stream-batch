{{
    config(
        materialized='table',
        partition_by={
            "field": "created_date", 
            "data_type": "timestamp"
        },
        cluster_by=['product_id', 'category', 'sub_category']
    )
}}

with fraud_orders as (
    select *
    from {{ ref('fct_orders') }}
    where fraud_status = 'Fraud'
),
products as (
    select *
    from {{ ref('dim_products') }}
)
select
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    count(f.order_id) as fraud_attempts
from products p
left join fraud_orders f on p.product_id = f.product_id
group by p.product_id, p.product_name, p.category, p.sub_category
order by fraud_attempts desc