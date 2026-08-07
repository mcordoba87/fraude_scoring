{{
    config(
        materialized="table"
    )
}}

-- Hechos transactional unificado: une transacciones con su dimension de
-- usuario (la version vigente del scoring en el momento de la transaccion).

with fact as (
    select
        st.transaction_id,
        st.user_id,
        st.created_at,
        st.amount_usd,
        st.merchant_category,
        st.location,
        st.is_flagged_fraud,
        du.user_sk,
        du.scoring_crediticio,
        du.status_riesgo
    from {{ ref('stg_transactions') }} st
    left join {{ ref('dim_users') }} du
        on du.user_id = st.user_id
       and du.is_current
)

select *
from fact