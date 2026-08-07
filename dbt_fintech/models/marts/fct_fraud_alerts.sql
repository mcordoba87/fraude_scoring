{{
    config(
        materialized="table"
    )
}}

-- Alertas de fraude por transaccion, evaluadas sobre reglas de negocio.
--
-- REGLAS DE ALERTA (documentadas para control):
--   1. high_amount   : monto de la transaccion >= 5000
--   2. limit_exceeded: monto > limite de credito vigente del usuario
--   3. velocity      : >= 5 transacciones del mismo usuario en 5 minutos
--   4. location_abroad: la ubicacion de la transaccion difiere de la del usuario
--   5. source_fraud   : el flujo OLTP ya la marco como sospechosa
--
-- Una transaccion tiene alerta ('f'raud_alert=true) si cumple cualquiera.

with tx as (
    select
        ft.transaction_id,
        ft.user_id,
        ft.created_at,
        ft.amount_usd,
        ft.merchant_category,
        ft.location,
        ft.is_flagged_fraud,
        v.cnt_5min,
        du.limite_credito,
        du.location                           as user_location
    from {{ ref('fct_transactions') }} ft
    left join {{ ref('int_user_transaction_velocity') }} v
        on v.transaction_id = ft.transaction_id
    left join {{ ref('dim_users') }} du
        on du.user_id = ft.user_id
       and du.is_current
),

alerts as (
    select
        transaction_id,
        user_id,
        created_at,
        amount_usd,
        merchant_category,
        case
            when amount_usd >= 5000 then true else false
        end                                                    as alert_high_amount,
        case
            when limite_credito is not null and amount_usd > limite_credito then true
            else false
        end                                                    as alert_limit_exceeded,
        case
            when cnt_5min >= 5 then true
            else false
        end                                                    as alert_velocity,
        case
            when user_location is not null
             and location is not null
             and location <> user_location then true
            else false
        end                                                    as alert_location_shift,
        coalesce(is_flagged_fraud, false)                       as alert_source_flag
    from tx
)

select
    transaction_id,
    user_id,
    created_at,
    amount_usd,
    merchant_category,
    alert_high_amount,
    alert_limit_exceeded,
    alert_velocity,
    alert_location_shift,
    alert_source_flag,
    (
        alert_high_amount
        or alert_limit_exceeded
        or alert_velocity
        or alert_location_shift
        or alert_source_flag
    ) as is_fraud_alert
from alerts