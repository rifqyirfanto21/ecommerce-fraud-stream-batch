with source as (
    select *
    from {{ source('rifqy_ecommerce_finpro', 'raw_users') }}
)
select
    user_id,
    name,
    email,
    phone_number,
    created_date,
    ingestion_ts,
    source
from source