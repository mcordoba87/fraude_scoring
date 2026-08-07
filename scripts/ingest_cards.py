"""Ingestor de tarjetas desde la Mock Cards API hacia Redpanda.

Consume el endpoint paginado GET /cards, aplica MASKING DE PII (practica
obligatoria en fintech / PCI-DSS) y publica cada registro anonimizado al
topico Redpanda `cards.api`.

Masqueado de PII:
    * pan        -> pan_last_masked (solo ultimos 4 digitos)
    * cvv        -> DESCARTADO (nunca se persiste)
    * cardholder -> enmascarado (ej. JUAN P. -> JU** P.)

Uso:
    ./venv/bin/python scripts/ingest_cards.py            # recorre todas las paginas
    ./venv/bin/python scripts/ingest_cards.py --pages 2  # solo 2 paginas
"""

import argparse
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
from confluent_kafka import Producer

load_dotenv()

CARDS_API_URL = os.getenv("CARDS_API_URL", "http://localhost:8001")
CARDS_TOPIC = os.getenv("CARDS_TOPIC", "cards.api")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
PAGE_SIZE = int(os.getenv("CARDS_PAGE_SIZE", "50"))


def mask_pan(pan):
    digits = "".join(ch for ch in pan if ch.isdigit())
    return f"**** **** **** {digits[-4:]}"


def mask_cardholder(name):
    parts = name.split()
    if not parts:
        return ""
    return f"{parts[0][:2]}** {parts[-1][0]}."


def anonymize(card):
    """Aplica masking PII. El CVV se descarta y nunca se persiste."""
    return {
        "card_id": card.get("card_id"),
        "user_id": card.get("user_id"),
        "pan_last_masked": mask_pan(card.get("pan", "")),
        "card_type": card.get("card_type"),
        "cardholder_masked": mask_cardholder(card.get("cardholder", "")),
        "exp_date": card.get("exp_date"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_page(session, page):
    resp = session.get(
        f"{CARDS_API_URL}/cards",
        params={"page": page, "page_size": PAGE_SIZE},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pages", type=int, default=0,
                        help="Paginas a procesar (0 = todas)")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    session = requests.Session()

    page = 1
    total_published = 0
    print(f"[ingest_cards] Paginando {CARDS_API_URL}, publicando a {CARDS_TOPIC}")
    while True:
        data = fetch_page(session, page)
        items = data.get("items", [])
        if not items:
            break
        for card in items:
            record = anonymize(card)
            producer.produce(
                CARDS_TOPIC,
                key=str(record["card_id"]).encode(),
                value=json.dumps(record).encode("utf-8"),
            )
            producer.poll(0)
        producer.flush()
        total_published += len(items)
        print(f"[ingest_cards] pagina {page}: {len(items)} tarjetas anonimizadas")
        page += 1
        if args.pages and page > args.pages:
            break
        if page * PAGE_SIZE >= data.get("total", 0):
            break

    print(f"[ingest_cards] Done: {total_published} tarjetas a {CARDS_TOPIC}")


if __name__ == "__main__":
    main()