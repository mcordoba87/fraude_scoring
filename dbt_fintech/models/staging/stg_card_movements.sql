{{
    config(
        materialized='view'
    )
}}

with source as (
    select
        id,
        card_id,
        date,
        amount,
        merchant_category,
        location,
        source_date
    from {{ source('raw', 'card_movements') }}
),

renamed as (
    select
        id as source_id,
        card_id,
        cast(date as timestamp) as movement_date,
        cast(amount as numeric(15, 2)) as amount_usd,
        source_date,
        nullif(trim(merchant_category), '') as merchant_category,
        nullif(trim(location), '') as location
    from source
)

select
    source_id,
    card_id,
    movement_date,
    amount_usd,
    merchant_category,
    location,
    source_date
from renamed
