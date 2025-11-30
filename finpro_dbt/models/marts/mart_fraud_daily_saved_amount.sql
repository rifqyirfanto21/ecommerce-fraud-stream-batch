{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date",
            "data_type": "date",
        }
    )
}}

with fraud_orders as (
    select *,
        date(event_ts) as event_date
    from {{ ref('fct_orders') }}
    where fraud_status = 'Fraud'
)
select
    event_date,
    user_id,
    sum(amount) as saved_amount,
    count(order_id) as fraud_attempts
from fraud_orders
group by event_date, user_id