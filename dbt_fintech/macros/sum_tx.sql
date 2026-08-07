{# Suma monto de transacciones del usuario en ventana (cerrada a la derecha). #}
{% macro sum_tx(user_id_col, ts_col, window_interval) %}
    (select coalesce(sum(x.amount_usd), 0)
     from {{ ref('stg_transactions') }} x
     where x.user_id = {{ user_id_col }}
       and x.created_at > {{ ts_col }} - {{ window_interval }}
       and x.created_at <= {{ ts_col }})
{% endmacro %}