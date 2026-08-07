"""Carga los archivos del lakehouse (MinIO) hacia PostgreSQL OLAP (raw).

Es EL puente que el roadmap anexo dejaba pendiente: convierte los Parquet
particionados y el CSV de landing en tablas `raw.*` del data warehouse,
listas para que dbt las transforme (staging -> silver -> gold).

Origenes (MinIO `fintech-lakehouse`):
  transactions/  -> raw.transactions       (Parquet CDC)
  users/         -> raw.users             (Parquet CDC)
  cards/         -> raw.cards             (Parquet anonimizado, sin PAN/CVV)
  landing/cards_daily/*/movimientos.csv  -> raw.card_movements

Estrategia: TRUNCATE + bulk COPY por corrida (idempotente).

Uso:
    ./venv/bin/python scripts/load_lakehouse.py
    ./venv/bin/python scripts/load_lakehouse.py --only transactions,cards
"""

import argparse
import csv
import io
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BUCKET = os.getenv("MINIO_BUCKET", "fintech-lakehouse")
LANDING_PATH = os.getenv("LANDING_PATH", "landing/cards_daily")

DB_CONFIG = {
    "host": os.getenv("PG_OLAP_HOST", "localhost"),
    "port": os.getenv("PG_OLAP_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_OLAP_DB", "fintech_olap"),
    "user": os.getenv("POSTGRES_OLAP_USER", "olap_user"),
    "password": os.getenv("POSTGRES_OLAP_PASSWORD", "olap_password"),
}

# --- Esquemas RAW (DDL de destino en Postgres) --------------------------------
RAW_DDL = {
    "transactions": """
        CREATE TABLE IF NOT EXISTS raw.transactions (
            id BIGINT,
            user_id BIGINT,
            amount NUMERIC(15,2),
            merchant_category TEXT,
            location TEXT,
            is_flagged_fraud BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ
        )""",
    "users": """
        CREATE TABLE IF NOT EXISTS raw.users (
            id BIGINT,
            nombre TEXT,
            scoring_crediticio INT,
            limite_credito NUMERIC(15,2),
            status_riesgo TEXT,
            location TEXT,
            updated_at TIMESTAMPTZ
        )""",
    "cards": """
        CREATE TABLE IF NOT EXISTS raw.cards (
            card_id BIGINT,
            user_id BIGINT,
            pan_last_masked TEXT,
            card_type TEXT,
            cardholder_masked TEXT,
            exp_date TEXT,
            ingested_at TIMESTAMPTZ
        )""",
    "card_movements": """
        CREATE TABLE IF NOT EXISTS raw.card_movements (
            id BIGSERIAL PRIMARY KEY,
            card_id BIGINT,
            date TIMESTAMPTZ,
            amount NUMERIC(15,2),
            merchant_category TEXT,
            location TEXT,
            source_date TEXT,
            sourced_from TEXT
        )""",
}

# Orden de insercion y su origen (prefijo en MinIO)
LOADERS = {
    "transactions": ("transactions/", "parquet"),
    "users": ("users/", "parquet"),
    "cards": ("cards/", "parquet"),
    "card_movements": (LANDING_PATH, "csv"),
}


def _fs():
    import s3fs
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS,
        secret=MINIO_SECRET,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
        use_ssl=False,
    )


def read_parquet(fs, prefix):
    """Lee Parquet particionados bajo un prefijo y devuelve la tabla pyarrow."""
    import pyarrow.parquet as pq
    files = fs.glob(f"{BUCKET}/{prefix.rstrip('/')}/**/*.parquet")
    if not files:
        print(f"  [warn] sin Parquet en s3://{BUCKET}/{prefix}/")
        return None
    dataset = pq.ParquetDataset(
        [f"s3://{f}" for f in files], filesystem=fs)
    return dataset.read()


def read_csv_landing(fs, prefix):
    """Lee todos los movimientos.csv de la landing zone."""
    import pyarrow as pa
    import pyarrow.csv as pcsv
    files = fs.glob(f"{BUCKET}/{prefix}/**/movimientos.csv")
    if not files:
        print(f"  [warn] sin CSVs en s3://{BUCKET}/{prefix}/")
        return None
    tables = []
    for f in files:
        raw = fs.cat(f)
        tbl = pcsv.read_csv(io.BytesIO(raw)).rename_columns(
            ["card_id", "date", "amount", "merchant_category", "location"])
        day = f.split("/")[-2]
        tbl = tbl.append_column("source_date", pa.array([day] * tbl.num_rows))
        tables.append(tbl)
    return pa.concat_tables(tables)


def copy_table(conn, table, arrow, cols):
    """TRUNCATE + COPY desde un buffer CSV (maneja tipos nativos de Postgres)."""
    cur = conn.cursor()
    cur.execute(f"TRUNCATE raw.{table}")
    conn.commit()
    buf = io.StringIO()
    writer = csv.writer(buf)
    selected = arrow.select(cols)
    data = selected.to_pylist()
    for r in data:
        writer.writerow([r.get(c) if r.get(c) is not None else "None" for c in cols])
    buf.seek(0)
    cur.copy_expert(
        f"COPY raw.{table} ({','.join(cols)}) FROM STDIN WITH (FORMAT csv, NULL 'None')",
        buf,
    )
    conn.commit()
    cur.close()
    n = len(data)
    print(f"  [load] raw.{table}: {n} filas")
    return n


def main(raw):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--single", default=None,
        help="Solo cargar estos (comma-separated): transactions,users,cards,card_movements")
    args = parser.parse_args()

    fs = _fs()

    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.commit()

    targets = args.single.split(",") if args.single else list(LOADERS.keys())
    for t in targets:
        if t not in RAW_DDL:
            print(f"[load] ignorado: {t} (desconocido)")
            continue
        cur.execute(f"DROP TABLE IF EXISTS raw.{t} CASCADE")
        conn.commit()
        cur.execute(RAW_DDL[t])
        conn.commit()
        prefix, kind = LOADERS[t]
        if kind == "parquet":
            arrow = read_parquet(fs, prefix)
            if arrow is None:
                continue
        else:
            arrow = read_csv_landing(fs, prefix)
            if arrow is None:
                continue
        cols = [f.name for f in arrow.schema]
        cols = [c for c in cols if c not in ("year", "month", "day")]
        copy_table(conn, t, arrow, cols)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main(sys.argv)