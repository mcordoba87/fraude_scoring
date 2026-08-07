{{
    config(
        materialized='view'
    )
}}

-- Tarjetas anonimizadas en el ingest (sin campos sensibles pan/cvv).
-- El worker acumula eventos de cada corrida del ingestor, por lo que se
-- deduplica por card_id conservando la version mas reciente.
with source as (
    select
        card_id,
        user_id,
        pan_last_masked,
        card_type,
        cardholder_masked,
        exp_date,
        ingested_at
    from {{ source('raw', 'cards') }}
),

renamed as (
    select
        card_id,
        user_id,
        pan_last_masked,
        nullif(trim(card_type), '')        as card_type,
        cardholder_masked,
        nullif(trim(exp_date), '')         as exp_date,
        ingested_at,
        row_number() over (partition by card_id order by ingested_at desc) as row_num
    from source
)

select
    card_id,
    user_id,
    pan_last_masked,
    card_type,
    cardholder_masked,
    exp_date,
    ingested_at
from renamed
where row_num = 1