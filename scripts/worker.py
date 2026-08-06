"""Consumer de Redpanda que escribe eventos CDC a Parquet en MinIO.

Agrupa por micro-batches y escribe archivos Parquet particionados por
fecha (year/=/month=/day=) bajo s3://fintech-lakehouse/transactions/.

Uso:
    ./venv/bin/python scripts/worker.py
"""

import io
import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = "postgres_oltp.public.transactions"
GROUP_ID = "sink-parquet-worker"
BATCH_MAX = int(os.getenv("BATCH_MAX", "100"))
BATCH_TIMEOUT_S = float(os.getenv("BATCH_TIMEOUT_S", "5"))
FLUSH_INTERVAL_S = float(os.getenv("FLUSH_INTERVAL_S", "10"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "fintech-lakehouse")
PREFIX = "transactions"

SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("user_id", pa.int64()),
    ("amount", pa.float64()),
    ("merchant_category", pa.string()),
    ("location", pa.string()),
    ("is_flagged_fraud", pa.bool_()),
    ("created_at", pa.timestamp("us")),
])


def parse_event(value_bytes):
    if not value_bytes:
        return None
    try:
        d = json.loads(value_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    created = d.get("created_at")
    if not created:
        return None
    try:
        ts = datetime.fromisoformat(created.replace("Z", "+00:00"))
        amount = float(d.get("amount")) if d.get("amount") is not None else None
    except (ValueError, TypeError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return {
        "id": d.get("id"),
        "user_id": d.get("user_id"),
        "amount": amount,
        "merchant_category": d.get("merchant_category"),
        "location": d.get("location"),
        "is_flagged_fraud": bool(d.get("is_flagged_fraud")),
        "created_at": ts,
    }


def write_batch(fs, rows):
    if not rows:
        return

    # Agrupar por fecha de created_at para particionar
    by_date = {}
    for r in rows:
        key = r["created_at"].date()
        by_date.setdefault(key, []).append(r)

    for date, group in by_date.items():
        year, month, day = date.isoformat().split("-")
        table = pa.table({
            "id": [r["id"] for r in group],
            "user_id": [r["user_id"] for r in group],
            "amount": [r["amount"] for r in group],
            "merchant_category": [r["merchant_category"] for r in group],
            "location": [r["location"] for r in group],
            "is_flagged_fraud": [r["is_flagged_fraud"] for r in group],
            "created_at": [r["created_at"] for r in group],
        }, schema=SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        path = (f"s3://{BUCKET}/{PREFIX}/year={year}/month={month}/day={day}/"
                f"batch_{int(time.time()*1000)}.snappy.parquet")
        with fs.open(path, "wb") as f:
            f.write(buf.getvalue())
        print(f"[worker] Escribió {len(group)} filas -> {path}")


def main():
    import s3fs
    from confluent_kafka import Consumer

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
    consumer.subscribe([TOPIC])

    buffer = []
    batch_start = time.time()
    last_commit = time.time()
    print(f"[worker] Consumiendo {TOPIC} -> {BUCKET}/{PREFIX} (Parquet particionado)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[worker] error: {msg.error()}")
            else:
                row = parse_event(msg.value())
                if row:
                    buffer.append(row)

            now = time.time()
            batch_due = len(buffer) >= BATCH_MAX
            if buffer and (batch_due or now - batch_start >= BATCH_TIMEOUT_S):
                write_batch(fs, buffer)
                buffer = []
                batch_start = now

            if now - last_commit >= FLUSH_INTERVAL_S:
                try:
                    consumer.commit(asynchronous=False)
                except Exception as e:
                    print(f"[worker] commit ignorado: {e}")
                last_commit = now
    except KeyboardInterrupt:
        if buffer:
            write_batch(fs, buffer)
        print("[worker] detenido")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()