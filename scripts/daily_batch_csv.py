"""Batch diario de movimientos de tarjetas (feed legacy -> landing en MinIO).

Simula el sistema LEGACY que genera un CSV diario y lo deja en una ruta de
landing zone dentro del lakehouse. Solo escribre el archivo (sin loader):
la leida la hace dbt directo en la Fase 3.

Archivo destino:
    s3://fintech-lakehouse/landing/cards_daily/YYYY-MM-DD/movimientos.csv

Uso:
    ./venv/bin/python scripts/daily_batch_csv.py
    ./venv/bin/python scripts/daily_batch_csv.py --rows 50
"""

import argparse
import csv
import io
import os
import random
from datetime import date, datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET = os.getenv(
    "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BUCKET = os.getenv("MINIO_BUCKET", "fintech-lakehouse")
LANDING_PATH = os.getenv("LANDING_PATH", "landing/cards_daily")

CATEGORIES = [
    "restaurantes", "supermercados", "transporte", "retail", "viajes",
    "entretenimiento", "salud", "telecomunicaciones", "ecommerce",
]
CITIES = ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "Salta", "Neuquén"]


def build_rows(n, target_date):
    rows = []
    for _ in range(n):
        amount = round(random.uniform(5, 6000), 2)
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        ts = datetime.combine(target_date, datetime.min.time()).replace(
            hour=hour, minute=minute)
        rows.append({
            "card_id": random.randint(1, 200),
            "date": ts.isoformat(),
            "amount": amount,
            "merchant_category": random.choice(CATEGORIES),
            "location": random.choice(CITIES),
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200,
                        help="Filas a generar (default 200)")
    parser.add_argument("--date", default=None,
                        help="Fecha del archivo (YYYY-MM-DD), default hoy")
    args = parser.parse_args()

    import s3fs

    target = date.fromisoformat(args.date) if args.date else date.today()
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS,
        secret=MINIO_SECRET,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
        use_ssl=False,
    )

    rows = build_rows(args.rows, target)
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=["card_id", "date", "amount", "merchant_category", "location"])
    writer.writeheader()
    writer.writerows(rows)

    prefix = f"{BUCKET}/{LANDING_PATH}/{target.isoformat()}"
    path = f"s3://{prefix}/movimientos.csv"
    with fs.open(path, "wb") as f:
        f.write(buf.getvalue().encode("utf-8"))
    print(f"[daily_batch_csv] {len(rows)} filas -> {path}")


if __name__ == "__main__":
    main()