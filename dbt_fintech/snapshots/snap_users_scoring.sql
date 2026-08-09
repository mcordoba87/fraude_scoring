{% snapshot snap_users_scoring %}

{{
    config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='user_id',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    user_id,
    nombre,
    scoring_crediticio,
    limite_credito,
    status_riesgo,
    location,
    updated_at
from {{ ref('stg_users') }}
{% endsnapshot %}