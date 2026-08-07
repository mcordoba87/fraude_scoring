"""Mock API de tarjetas (fuente externa simulada).

Simula la fuente de una entidad emisora de tarjetas que entrega DATOS CRUDOS
SENSIBLES (PII / PCI-DSS) sobre un endpoint paginado.

Se corre con (desde la raíz del repo):
    ./venv/bin/uvicorn api.mock_cards_api:app --host 0.0.0.0 --port 8001

Advertencia: este servicio solo existe para simular la entrada de datos.
El PAN / CVV completos NO deben persistirse en ningún destino downstream.
"""

import os
import random
from datetime import date, timedelta
from dotenv import load_dotenv
import psycopg2
from fastapi import FastAPI, Query

load_dotenv()

app = FastAPI(title="Mock Cards API", version="0.1.0")

CARD_TYPES = ["visa", "mastercard", "amex", "maestro"]

DB_CONFIG = {
    "host": os.getenv("PG_OLTP_HOST", "localhost"),
    "port": os.getenv("PG_OLTP_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_OLTP_DB", "fintech_oltp"),
    "user": os.getenv("POSTGRES_OLTP_USER", "oltp_user"),
    "password": os.getenv("POSTGRES_OLTP_PASSWORD", "oltp_password"),
}

# Cache simple de user_ids disponibles para asociar tarjetas.
_USER_IDS = None


def get_user_ids():
    global _USER_IDS
    if _USER_IDS is not None:
        return _USER_IDS
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT id FROM users")
        _USER_IDS = [r[0] for r in cur.fetchall()]
        cur.close()
        conn.close()
    except psycopg2.Error:
        _USER_IDS = list(range(1, 51))
    return _USER_IDS or [1]


def gen_pan():
    group = "".join(str(random.randint(0, 9)) for _ in range(4))
    return f"4{group} 1234 5678 {random.randint(1000, 9999)}"


def gen_cvv():
    return f"{random.randint(100, 999)}"


def gen_exp_date():
    month = random.randint(1, 12)
    year = date.today().year + random.randint(0, 5)
    return f"{month:02d}/{year % 100:02d}"


def gen_cardholder():
    first = random.choice([
        "JUAN", "MARIA", "PEDRO", "ANA", "LUIS", "SOFIA", "CARLOS", "VALERIA",
        "DIEGO", "LAURA", "NICOLAS", "VERONICA", "GASTON", "PAULA", "SANTIAGO",
    ])
    last = random.choice([
        "P.", "G.", "M.", "R.", "T.", "S.", "B.", "L.", "D.", "C.", "V.", "F.",
    ])
    return f"{first} {last}"


def build_cards(n):
    user_ids = get_user_ids()
    cards = []
    for i in range(n):
        cards.append({
            "card_id": i + 1,
            "user_id": random.choice(user_ids),
            "pan": gen_pan(),
            "cvv": gen_cvv(),
            "exp_date": gen_exp_date(),
            "card_type": random.choice(CARD_TYPES),
            "cardholder": gen_cardholder(),
        })
    return cards


_CARDS_POOL = build_cards(200)


@app.get("/cards")
def get_cards(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    start = (page - 1) * page_size
    items = _CARDS_POOL[start:start + page_size]
    return {
        "page": page,
        "page_size": page_size,
        "total": len(_CARDS_POOL),
        "items": items,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)