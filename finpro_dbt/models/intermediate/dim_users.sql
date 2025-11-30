{{
    config(
        materialized='table',
        partition_by={
            "field": "created_date",
            "data_type": "timestamp",
        },
        cluster_by=['user_id']
    )
}}

with users as (
    select *
    from {{ ref('stg_users')}}
)
select
    user_id,
    trim(name) as user_name,
    email,
    phone_number,
    REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g') as phone_number_numeric,
    created_date,
    ingestion_ts
from users