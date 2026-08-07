-- Dimension de usuarios: estado actual + historial de scoring (SCD2).
-- Se construye sobre el snapshot. La version vigente es la que tiene
-- dbt_valid_to = null.

with base as (
    select
        user_id,
        nombre,
        scoring_crediticio,
        limite_credito,
        status_riesgo,
        location,
        dbt_valid_from,
        dbt_valid_to,
        case
            when dbt_valid_to is null then true
            else false
        end as is_current
    from {{ ref('snap_users_scoring') }}
)

select
    user_id,
    user_id as user_sk,
    nombre,
    scoring_crediticio,
    limite_credito,
    status_riesgo,
    location,
    dbt_valid_from as valid_from,
    dbt_valid_to,
    is_current
from base