{{
    config(
        materialized='view'
    )
}}

with source as (
    select
        id                  as user_id,
        nombre,
        scoring_crediticio,
        limite_credito,
        status_riesgo,
        location,
        updated_at
    from {{ source('raw', 'users') }}
),

renamed as (
    select
        user_id,
        nullif(trim(nombre), '')              as nombre,
        scoring_crediticio,
        cast(limite_credito as numeric(15,2)) as limite_credito,
        lower(nullif(trim(status_riesgo), '')) as status_riesgo,
        nullif(trim(location), '')            as location,
        updated_at
    from source
),

deduped as (
    select
        *,
        row_number() over (partition by user_id order by updated_at desc) as row_num
    from renamed
)

select
    user_id,
    nombre,
    scoring_crediticio,
    limite_credito,
    status_riesgo,
    location,
    updated_at
from deduped
where row_num = 1