# Dashboards Operativos en Metabase (Fase 4.1)

Guía para construir el panel operativo de fraude y scoring sobre la capa
**Gold** de dbt (`public_marts.*`).

La fuente **Data Warehouse (OLAP)** ya está registrada en Metabase
(`fintech_olap`, esquemas `public_marts` + `public_intermediate` habilitados),
así que alcanza con crear los paneles con SQL nativo.

---

## 0. Acceso

1. Abrir `http://localhost:3000`.
2. Iniciar sesión con el usuario superadmin del proyecto.
3. En la barra izquierda, la base **Data Warehouse (OLAP)** debe listar las
   tablas:
   - `public_marts.dim_users`
   - `public_marts.fct_transactions`
   - `public_marts.fct_fraud_alerts`
   - `public_intermediate.int_user_transaction_velocity`

Para crear cada panel: **+ Nuevo -> Pregunta -> Consulta nativa (SQL)**,
elegir la fuente *Data Warehouse (OLAP)*, pegar el query y
**Guardar** en una colección *Fraude & Scoring*.

---

## Panel 1 - % de fraude por hora / día

Ratio de transacciones con alerta (`is_fraud_alert`) sobre el total, agregado
por hora (o por día si se usa `date_trunc('day', ...)`).

```sql
select
    date_trunc('hour', created_at)                                                     as hora,
    count(*)                                                                           as tx_total,
    count(*) filter (where is_fraud_alert)                                             as tx_con_alertas,
    round(100.0
          * count(*) filter (where is_fraud_alert)
          / nullif(count(*), 0), 2)                                                    as pct_fraude
from public_marts.fct_fraud_alerts
group by 1
order by 1 desc
limit 168;
```

**Visualización:** *Barra* o *Línea*, eje X → `hora`, eje Y → `pct_fraude`
(y `tx_con_alertas` si se quiere el volumen).

---

## Panel 2 - Distribución por scoring

Usuarios vigentes (`is_current = true`) agrupados por banda de scoring y por
nivel de riesgo. Da el histograma del estado crediticio del portafolio.

```sql
select
    status_riesgo,
    width_bucket(scoring_crediticio, 300, 850, 11) as bucket,
    min(scoring_crediticio)                        as min_score,
    max(scoring_crediticio)                        as max_score,
    count(*)                                       as usuarios
from public_marts.dim_users
where is_current
group by 1, 2
order by 1, 2;
```

**Visualización:** tabular o barras apiladas por `status_riesgo`.

---

## Panel 3 - Alertas en vivo (feed)

Últimas transacciones con alertas de fraude y el detalle de qué regla la
disparó.

```sql
select
    created_at,
    user_id,
    amount_usd,
    merchant_category,
    alert_high_amount,
    alert_limit_exceeded,
    alert_velocity,
    alert_location_shift,
    alert_source_flag
from public_marts.fct_fraud_alerts
where is_fraud_alert
order by created_at desc
limit 50;
```

**Visualización:** tabla. Añadir un filtro de la base (Campo de fecha en
`created_at`) en el dashboard para los últimos 5 minutos si se quiere un
"radar" en vivo.

---

## Panel de resumen reusable - Ticket de alerta por regla

Para medir qué regla de alerta es responsable de la mayoría de los casos.

Una alerta puede dispararse por más de una regla a la vez, así que se cuenta
cada bandera en forma independiente (una transacción puede sumar en dos filas).

```sql
select 'monto-alto'         as regla, count(*) as alertas
from public_marts.fct_fraud_alerts
where is_fraud_alert and alert_high_amount
union all
select 'limite-superado', count(*)
from public_marts.fct_fraud_alerts
where is_fraud_alert and alert_limit_exceeded
union all
select 'velocidad', count(*)
from public_marts.fct_fraud_alerts
where is_fraud_alert and alert_velocity
union all
select 'ubicacion-distinta', count(*)
from public_marts.fct_fraud_alerts
where is_fraud_alert and alert_location_shift
union all
select 'flag-oltp', count(*)
from public_marts.fct_fraud_alerts
where is_fraud_alert and alert_source_flag
order by 2 desc;
```

---

## Crear el dashboard

1. Mientras esté guardado un panel: **Añadir al dashboard → Nuevo dashboard**
   (nombre: *Fraude & Scoring*).
2. Repetir para cada panel.
3. Lay-out por defecto y listo. Los dashboards se pueden compartir con un
   `link` que funciona sin login (Metabase `public sharing`) si se quiere
   embederlo en la app móvil / front.