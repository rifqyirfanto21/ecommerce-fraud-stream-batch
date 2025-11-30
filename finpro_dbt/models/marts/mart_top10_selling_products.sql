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
    count(g.order_id) as total_orders
from products p
left join genuine_orders g on p.product_id = g.product_id
group by p.product_id, p.product_name, p.category, p.sub_category
order by total_orders desc
limit 10