"""Consumer de Redpanda que escribe eventos CDC/API a Parquet en MinIO.

Agrupa por micro-batches y escribe archivos Parquet particionados por
fecha (year/=/month=/day=) bajo el prefijo de cada topico.

Soportados (TOPIC -> PREFIX):
    postgres_oltp.public.transactions -> fintech-lakehouse/transactions/
    postgres_oltp.public.users       -> fintech-lakehouse/users/
    cards.api                        -> fintech-lakehouse/cards/

Uso:
    ./venv/bin/python scripts/worker.py                     # todos los topicos
    ./venv/bin/python scripts/worker.py --topic cards.api  # solo uno
"""

import argparse
import io
import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
GROUP_ID = os.getenv("KAFKA_WORKER_GROUP", "sink-parquet-worker")
BATCH_MAX = int(os.getenv("BATCH_MAX", "200"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5"))
FLUSH_INTERVAL_S = float(os.getenv("FLUSH_INTERVAL_S", "10"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET = os.getenv(
    "MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
BUCKET = os.getenv("MINIO_BUCKET", "fintech-lakehouse")

TRANSACTIONS_TOPIC = "postgres_oltp.public.transactions"
USERS_TOPIC = "postgres_oltp.public.users"
CARDS_TOPIC = os.getenv("CARDS_TOPIC", "cards.api")

SCHEMA_TRANSACTIONS = pa.schema([
    ("id", pa.int64()),
    ("user_id", pa.int64()),
    ("amount", pa.float64()),
    ("merchant_category", pa.string()),
    ("location", pa.string()),
    ("is_flagged_fraud", pa.bool_()),
    ("created_at", pa.timestamp("us")),
])

SCHEMA_CARDS = pa.schema([
    ("card_id", pa.int64()),
    ("user_id", pa.int64()),
    ("pan_last_masked", pa.string()),
    ("card_type", pa.string()),
    ("cardholder_masked", pa.string()),
    ("exp_date", pa.string()),
    ("ingested_at", pa.timestamp("us")),
])

SCHEMA_USERS = pa.schema([
    ("id", pa.int64()),
    ("nombre", pa.string()),
    ("scoring_crediticio", pa.int64()),
    ("limite_credito", pa.float64()),
    ("status_riesgo", pa.string()),
    ("location", pa.string()),
    ("updated_at", pa.timestamp("us")),
])


def parse_ts(value):
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        ts = datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def parse_number(d):
    try:
        return float(d) if d is not None else None
    except (ValueError, TypeError):
        return None


def parse_transaction(value_bytes):
    if not value_bytes:
        return None
    try:
        d = json.loads(value_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    ts = parse_ts(d.get("created_at"))
    if not ts:
        return None
    return {
        "id": d.get("id"),
        "user_id": d.get("user_id"),
        "amount": parse_number(d.get("amount")),
        "merchant_category": d.get("merchant_category"),
        "location": d.get("location"),
        "is_flagged_fraud": bool(d.get("is_flagged_fraud")),
        "created_at": ts,
    }


def parse_card(value_bytes):
    if not value_bytes:
        return None
    try:
        d = json.loads(value_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    ts = parse_ts(d.get("ingested_at"))
    if not ts:
        return None
    return {
        "card_id": d.get("card_id"),
        "user_id": d.get("user_id"),
        "pan_last_masked": d.get("pan_last_masked"),
        "card_type": d.get("card_type"),
        "cardholder_masked": d.get("cardholder_masked"),
        "exp_date": d.get("exp_date"),
        "ingested_at": ts,
    }


def parse_user(value_bytes):
    if not value_bytes:
        return None
    try:
        d = json.loads(value_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    ts = parse_ts(d.get("updated_at"))
    if not ts:
        return None
    return {
        "id": d.get("id"),
        "nombre": d.get("nombre"),
        "scoring_crediticio": d.get("scoring_crediticio"),
        "limite_credito": parse_number(d.get("limite_credito")),
        "status_riesgo": d.get("status_riesgo"),
        "location": d.get("location"),
        "updated_at": ts,
    }


# TOPIC -> (PREFIX, parser, schema, time_field)
HANDLERS = {
    TRANSACTIONS_TOPIC: ("transactions", parse_transaction, SCHEMA_TRANSACTIONS, "created_at"),
    USERS_TOPIC: ("users", parse_user, SCHEMA_USERS, "updated_at"),
    CARDS_TOPIC: ("cards", parse_card, SCHEMA_CARDS, "ingested_at"),
}


def write_batch(fs, prefix, schema, rows, time_field):
    if not rows:
        return
    by_date = {}
    for r in rows:
        ts = r.get(time_field)
        if ts is None:
            continue
        key = ts.date()
        by_date.setdefault(key, []).append(r)
    for date, group in by_date.items():
        year, month, day = date.isoformat().split("-")
        col_map = {}
        for name in schema.names:
            col_map[name] = [r[name] for r in group]
        table = pa.table(col_map, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        path = (f"s3://{BUCKET}/{prefix}/year={year}/month={month}/day={day}/"
                f"batch_{int(time.time()*1000)}.snappy.parquet")
        with fs.open(path, "wb") as f:
            f.write(buf.getvalue())
        print(f"[worker] Escribio {len(group)} filas -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=None,
                        help="Solo procesar este topico (transactions, users o cards)")
    args = parser.parse_args()

    import s3fs
    from confluent_kafka import Consumer

    topics = [t for t in HANDLERS
              if args.topic is None or args.topic in t]

    if args.topic and not topics:
        print(f"[worker] No se encontro handler para topic='{args.topic}'")
        return

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS,
        secret=MINIO_SECRET,
        client_kwargs={"endpoint_url": f"http://{MINIO_ENDPOINT}"},
        use_ssl=False,
    )

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)

    buffers = {t: [] for t in topics}
    batch_start = {t: time.time() for t in topics}
    last_commit = time.time()
    print(f"[worker] Consumiendo {topics} -> {BUCKET} (Parquet particionado)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[worker] error: {msg.error()}")
            else:
                pair = HANDLERS.get(msg.topic())
                if pair:
                    prefix, parser, schema, time_field = pair
                    row = parser(msg.value())
                    if row:
                        buffers[msg.topic()].append(row)

            now = time.time()
            for t in topics:
                prefix, schema, time_field = HANDLERS[t][0], HANDLERS[t][2], HANDLERS[t][3]
                if buffers[t] and (
                        len(buffers[t]) >= BATCH_MAX
                        or now - batch_start[t] >= BATCH_TIMEOUT_S):
                    write_batch(fs, prefix, schema, buffers[t], time_field)
                    buffers[t] = []
                    batch_start[t] = now

            if now - last_commit >= FLUSH_INTERVAL_S:
                try:
                    consumer.commit(asynchronous=False)
                except Exception as e:
                    print(f"[worker] commit ignorado: {e}")
                last_commit = now
    except KeyboardInterrupt:
        for t in buffers:
            if buffers[t]:
                prefix, schema, time_field = HANDLERS[t][0], HANDLERS[t][2], HANDLERS[t][3]
                write_batch(fs, prefix, schema, buffers[t], time_field)
        print("[worker] detenido")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()