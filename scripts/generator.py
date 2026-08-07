"""Simulador de transacciones para la base OLTP (fintech).

Inserta usuarios de seed y luego genera transacciones continuas,
incluyendo patrones de fraude simulados.

Uso:
    ./venv/bin/python scripts/generator.py            # corrida indefinida
    ./venv/bin/python scripts/generator.py --limit 50 # transacciones fijas
"""

import argparse
import os
import random
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg2

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("PG_OLTP_HOST", "localhost"),
    "port": os.getenv("PG_OLTP_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_OLTP_DB", "fintech_oltp"),
    "user": os.getenv("POSTGRES_OLTP_USER", "oltp_user"),
    "password": os.getenv("POSTGRES_OLTP_PASSWORD", "oltp_password"),
}

MERCHANT_CATEGORIES = [
    "restaurantes", "supermercados", "transporte", "retail",
    "viajes", "entretenimiento", "salud", "telecomunicaciones", "ecommerce",
]

CITIES = {
    "Buenos Aires": (100, 100),
    "Córdoba": (95, 105),
    "Rosario": (90, 110),
    "Mendoza": (85, 115),
    "Salta": (80, 120),
    "Neuquén": (75, 125),
    "Ushuaia": (70, 130),
}

RISK_LEVELS = ["bajo", "medio", "alto"]

SCORING_UPDATE_EVERY = 20
SCORING_UPDATE_PROB = 0.3


def risk_for_scoring(scoring):
    if scoring >= 700:
        return "bajo"
    if scoring >= 550:
        return "medio"
    return "alto"


def refresh_user_scoring(conn, user_id):
    """Actualiza scoring/limite/riesgo de un usuario para alimentar el SCD2."""
    cur = conn.cursor()
    cur.execute(
        "SELECT scoring_crediticio, limite_credito FROM users WHERE id = %s",
        (user_id,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        return
    current, limit = row
    delta = random.randint(-60, 60)
    new_scoring = max(300, min(850, current + delta))
    if new_scoring >= 700:
        new_limit = random.randint(200000, 500000)
    elif new_scoring >= 550:
        new_limit = random.randint(50000, 199999)
    else:
        new_limit = random.randint(10000, 49999)
    cur.execute(
        """UPDATE users
           SET scoring_crediticio = %s, limite_credito = %s,
               status_riesgo = %s, updated_at = now()
           WHERE id = %s""",
        (new_scoring, new_limit, risk_for_scoring(new_scoring), user_id),
    )
    conn.commit()
    cur.close()


def seed_users(conn, n=50):
    cur = conn.cursor()
    for i in range(n):
        scoring = random.randint(300, 850)
        if scoring >= 700:
            risk = "bajo"
            limit = random.randint(200000, 500000)
        elif scoring >= 550:
            risk = "medio"
            limit = random.randint(50000, 199999)
        else:
            risk = "alto"
            limit = random.randint(10000, 49999)
        cur.execute(
            """INSERT INTO users
               (nombre, scoring_crediticio, limite_credito, status_riesgo, location, updated_at)
               VALUES (%s, %s, %s, %s, %s, now())""",
            (f"Usuario {i:03d}", scoring, limit, risk, random.choice(list(CITIES)), ),
        )
    conn.commit()
    cur.close()
    print(f"[generator] Insertados {n} usuarios de seed")


def random_location(user_city):
    if random.random() < 0.9:
        return user_city
    return random.choice([c for c in CITIES if c != user_city])


def build_transaction(user):
    amount = round(random.uniform(10, 3000), 2)
    category = random.choice(MERCHANT_CATEGORIES)
    location = random_location(user["location"])

    fraud = False
    if random.random() < 0.10:
        fraud = True
        if random.random() < 0.5:
            amount = round(random.uniform(5000, 50000), 2)
            category = random.choice(["viajes", "ecommerce", "telecomunicaciones"])
        if random.random() < 0.5:
            location = random.choice([c for c in CITIES if c != user["location"]])

    return {
        "user_id": user["id"],
        "amount": amount,
        "merchant_category": category,
        "location": location,
        "is_flagged_fraud": fraud,
    }


def load_users(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, location FROM users")
    users = [{"id": r[0], "location": r[1]} for r in cur.fetchall()]
    cur.close()
    return users


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0,
                        help="Transacciones a generar (0 = indefinido)")
    parser.add_argument("--delay", type=float, default=0.8,
                        help="Segundos entre transacciones")
    args = parser.parse_args()

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        users = load_users(conn)
        if not users:
            seed_users(conn)
            users = load_users(conn)

        cur = conn.cursor()
        count = 0
        t0 = time.time()
        print("[generator] Generando transacciones (Ctrl+C para detener)...")
        while True:
            tx = build_transaction(random.choice(users))
            cur.execute(
                """INSERT INTO transactions
                   (user_id, amount, merchant_category, location, is_flagged_fraud, created_at)
                   VALUES (%s, %s, %s, %s, %s, now())""",
                (tx["user_id"], tx["amount"], tx["merchant_category"],
                 tx["location"], tx["is_flagged_fraud"]),
            )
            conn.commit()
            count += 1
            if count % SCORING_UPDATE_EVERY == 0:
                target = random.choice(users)
                if random.random() < SCORING_UPDATE_PROB:
                    refresh_user_scoring(conn, target["id"])
            if args.limit and count >= args.limit:
                break
            time.sleep(args.delay)

        elapsed = time.time() - t0
        print(f"[generator] Done: {count} transacciones en {elapsed:.1f}s")
    finally:
        cur.close() if 'cur' in locals() else None
        conn.close()


if __name__ == "__main__":
    main()