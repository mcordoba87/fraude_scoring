{{
    config(
        materialized='table'
    )
}}

-- Metricas de velocidad por usuario en ventanas de 5, 15 y 60 minutos.
-- Para cada transaccion se cuenta/suma el monto de las transacciones del
-- mismo usuario dentro de la ventana (cerrada a la derecha, incluye la propia).

with txs as (
    select
        user_id,
        transaction_id,
        created_at,
        amount_usd
    from {{ ref('stg_transactions') }}
)

select
    t.user_id,
    t.transaction_id,
    t.created_at,
    {{ count_tx('t.user_id', 't.created_at', "interval '5 minute'") }}   as cnt_5min,
    {{ sum_tx('t.user_id', 't.created_at', "interval '5 minute'") }}     as sum_5min,
    {{ count_tx('t.user_id', 't.created_at', "interval '15 minute'") }}  as cnt_15min,
    {{ sum_tx('t.user_id', 't.created_at', "interval '15 minute'") }}    as sum_15min,
    {{ count_tx('t.user_id', 't.created_at', "interval '60 minute'") }}  as cnt_60min,
    {{ sum_tx('t.user_id', 't.created_at', "interval '60 minute'") }}    as sum_60min
from txs t