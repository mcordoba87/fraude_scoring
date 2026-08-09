"""API Backend de scoring y fraud alerts (Fase 4.2).

Sirve los resultados del Data Warehouse (capa Gold de dbt) para el
frontend / app móvil:

    GET /api/v1/users/{user_id}/credit-score          historial SCD2 del scoring
    GET /api/v1/users/{user_id}/credit-score/current  version vigente
    GET /api/v1/fraud/alerts                          alertas de fraude activas
    GET /api/v1/fraud/alerts/summary                  agregado de fraude por hora

Se corre con (desde la raiz del repo):
    ./venv/bin/uvicorn api.fintech_api:app --host 0.0.0.0 --port 8002

Lee de postgres_olap (fintech_olap). La config se toma de .env
(PG_OLAP_HOST / POSTGRES_OLAP_*).
"""

import os
from datetime import datetime

from dotenv import load_dotenv
import psycopg2
from fastapi import FastAPI, HTTPException, Query

load_dotenv()

app = FastAPI(
    title="Fintech Fraude & Scoring API",
    description="Acceso a la capa Gold (dbt) del Data Warehouse.",
    version="0.1.0",
)

DB_CONFIG = {
    "host": os.getenv("PG_OLAP_HOST", "localhost"),
    "port": os.getenv("PG_OLAP_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_OLAP_DB", "fintech_olap"),
    "user": os.getenv("POSTGRES_OLAP_USER", "olap_user"),
    "password": os.getenv("POSTGRES_OLAP_PASSWORD", "olap_password"),
}


def _conn():
    return psycopg2.connect(**DB_CONFIG)


def _fetch(sql, params=()):
    conn = _conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def _fmt(v):
    if isinstance(v, datetime):
        return v.isoformat()
    return v


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/api/v1/users/{user_id}/credit-score")
def credit_score_history(user_id: int):
    """Historial completo de scoring crediticio del usuario (SCD2), de mas
    antigua a mas reciente."""
    sql = """
        select
            user_id,
            nombre,
            scoring_crediticio,
            status_riesgo,
            limite_credito,
            location,
            valid_from,
            dbt_valid_to,
            is_current
        from public_marts.dim_users
        where user_id = %s
        order by valid_from
    """
    rows = _fetch(sql, (user_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"usuario {user_id} no existe")
    return {"user_id": user_id, "scoring_history": [{**r, "valid_from": _fmt(r["valid_from"]), "dbt_valid_to": _fmt(r["dbt_valid_to"])} for r in rows]}


@app.get("/api/v1/users/{user_id}/credit-score/current")
def credit_score_current(user_id: int):
    """Version vigente del scoring del usuario (historial con is_current = true)."""
    sql = """
        select
            user_id,
            nombre,
            scoring_crediticio,
            status_riesgo,
            limite_credito,
            location,
            valid_from,
            dbt_valid_to,
            is_current
        from public_marts.dim_users
        where user_id = %s
          and is_current
        limit 1
    """
    rows = _fetch(sql, (user_id,))
    if not rows:
        raise HTTPException(status_code=404, detail=f"usuario {user_id} no encontrado o sin version vigente")
    return rows[0]


@app.get("/api/v1/fraud/alerts")
def fraud_alerts(
    limit: int = Query(50, ge=1, le=500),
    user_id: int | None = None,
):
    """Alertas de fraude activas, ordenadas por la mas reciente.

    Una alerta activa es cualquier transaccion con is_fraud_alert = true.
    Cada regla viene desglosada en campos alert_*.
    """
    sql = """
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
            is_fraud_alert
        from public_marts.fct_fraud_alerts
        where is_fraud_alert
          and (%s is null or user_id = %s)
        order by created_at desc
        limit %s
    """
    rows = _fetch(sql, (user_id, user_id, limit))
    return {
        "count": len(rows),
        "alerts": [{k: _fmt(v) for k, v in r.items()} for r in rows],
    }


@app.get("/api/v1/fraud/alerts/summary")
def fraud_alerts_summary(
    hours: int = Query(24, ge=1, le=168),
):
    """Resumen operativo: cantidad de transacciones y de alertas activas por
    hora, con el porcentaje de fraude (para el dashboard en vivo)."""
    sql = """
        select
            date_trunc('hour', created_at) as hora,
            count(*)                          as tx_total,
            count(*) filter (where is_fraud_alert) as tx_con_alertas,
            round(100.0
                  * count(*) filter (where is_fraud_alert)
                  / nullif(count(*), 0), 2)   as pct_fraude
        from public_marts.fct_fraud_alerts
        where created_at >= now() - interval '1 hour' * %s
        group by 1
        order by 1 desc
    """
    rows = _fetch(sql, (hours,))
    return {"window_hours": hours, "by_hour": [{**r, "hora": _fmt(r["hora"])} for r in rows]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)