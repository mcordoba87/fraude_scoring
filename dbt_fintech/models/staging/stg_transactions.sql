{{
    config(
        materialized='view'
    )
}}

with source as (
    select
        id                     as transaction_id,
        user_id,
        amount,
        merchant_category,
        location,
        is_flagged_fraud,
        created_at
    from {{ source('raw', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        user_id,
        cast(amount as numeric(15,2))   as amount_usd,
        nullif(trim(merchant_category), '') as merchant_category,
        nullif(trim(location), '')      as location,
        coalesce(is_flagged_fraud, false) as is_flagged_fraud,
        created_at
    from source
),

deduped as (
    select
        *,
        row_number() over (partition by transaction_id order by created_at) as row_num
    from renamed
)

select
    transaction_id,
    user_id,
    amount_usd,
    merchant_category,
    location,
    is_flagged_fraud,
    created_at
from deduped
where row_num = 1