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

with genuine_orders as (
    select *
    from {{ ref('fct_orders') }}
    where fraud_status = 'Genuine'
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
    sum(g.amount) as revenue,
    sum(g.amount - (p.cost * g.quantity)) as profit
from products p
left join genuine_orders g on p.product_id = g.product_id
group by p.product_id, p.product_name, p.category, p.sub_category
order by revenue desc
