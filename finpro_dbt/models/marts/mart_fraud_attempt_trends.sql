{{
    config(
        materialized='table',
        partition_by={
            "field": "event_date", 
            "data_type": "date"
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
    count(order_id) as total_fraud_attempts
from fraud_orders
group by event_date
order by event_date
