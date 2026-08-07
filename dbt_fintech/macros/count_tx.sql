{# Cuenta transacciones del usuario en ventana (cerrada a la derecha). #}
{% macro count_tx(user_id_col, ts_col, window_interval) %}
    (select count(*)
     from {{ ref('stg_transactions') }} x
     where x.user_id = {{ user_id_col }}
       and x.created_at > {{ ts_col }} - {{ window_interval }}
       and x.created_at <= {{ ts_col }})
{% endmacro %}