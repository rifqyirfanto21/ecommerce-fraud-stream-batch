with genuine_orders as (
    select *
    from {{ ref('fct_orders') }}
    where fraud_status = 'Genuine'
),
users as (
    select *
    from {{ ref('dim_users') }}
)
select
    u.user_id,
    u.user_name,
    count(g.order_id) as orders_count
from users u
left join genuine_orders g on u.user_id = g.user_id
group by u.user_id, u.user_name
order by orders_count desc
limit 10