with fraud_orders as (
    select *
    from {{ ref('fct_orders') }}
    where fraud_status = 'Fraud'
),
users as (
    select *
    from {{ ref('dim_users') }}
)
select
    u.user_id,
    u.user_name,
    count(f.order_id) as fraud_attempts,
    max(f.event_ts) as last_fraud_ts
from users u
left join fraud_orders f on u.user_id = f.user_id
group by u.user_id, u.user_name
order by fraud_attempts desc
limit 10